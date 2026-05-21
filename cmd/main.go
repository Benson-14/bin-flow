package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/Benson-14/bin-flow/internal/checkpoint"
	"github.com/Benson-14/bin-flow/internal/metadata"
	"github.com/Benson-14/bin-flow/internal/models"
	"github.com/Benson-14/bin-flow/internal/parser"
	"github.com/Benson-14/bin-flow/internal/processor"
	"github.com/Benson-14/bin-flow/internal/sink"
)

const checkpointFile = "checkpoint.json"
const batchSize = 10

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)

	signal.Notify(
		signalChan,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	defer signal.Stop(signalChan)

	go func() {
		sig := <-signalChan
		log.Printf("received signal: %v", sig)
		cancel()
	}()

	// Load Checkpoint
	cp, err := checkpoint.LoadCheckpoint(checkpointFile)
	if err != nil {
		log.Fatalf("failed to load checkpoint: %v", err)
	}

	// Runtime replication state
	var currentBinlogFile string
	currentBinlogFile = cp.BinlogFile

	fmt.Printf("Resuming from %s @ position %d\n", cp.BinlogFile, cp.BinlogPos)

	// Replication Configuration
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "localhost",
		Port:     3307,
		User:     "replicator",
		Password: "replica_password",
	}

	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	steamer, err := syncer.StartSync(mysql.Position{
		Name: cp.BinlogFile,
		Pos:  cp.BinlogPos,
	})

	if err != nil {
		log.Fatalf("failed to start sync: %v", err)
	}

	s3Sink, err := sink.NewS3Sink(ctx, "cdc-lake")
	if err != nil {
		log.Fatalf("failed to create S3 sink: %v", err)
	}

	// CDC Event Channel
	eventsChan := make(chan models.CDCMessage, 100)

	var wg sync.WaitGroup

	fmt.Println("Listening for binlog events...")

	// Producer Goroutine
	// Reads replication stream and sends events to eventsChan
	wg.Go(func() {

		defer close(eventsChan)

		schemaCache := make(map[string]string)

		for {
			event, err := steamer.GetEvent(ctx)
			if err != nil {
				// Graceful shutdown
				if ctx.Err() != nil {
					log.Println("Shutdown signal received. Exiting event loop...")
					break
				}
				log.Printf("Error getting event: %v", err)
				continue
			}

			switch e := event.Event.(type) {
			case *replication.RotateEvent:
				newFile := string(e.NextLogName)
				if newFile != currentBinlogFile {
					fmt.Printf("Binlog rotated → %s\n", newFile)
					currentBinlogFile = newFile
				}

			case *replication.QueryEvent:
				query := strings.ToUpper(strings.TrimSpace(string(e.Query)))
				if strings.HasPrefix(query, "ALTER TABLE") {

					parts := strings.Fields(query)

					if len(parts) >= 3 {
						tableName := strings.Trim(parts[2], "`")
						tableKey := fmt.Sprintf("%s/%s", string(e.Schema), tableName)
						delete(schemaCache, tableKey)

						log.Printf("schema cache invalidated for %s", tableKey)
					}
				}

			case *replication.RowsEvent:

				tableKey := fmt.Sprintf("%s/%s", string(e.Table.Schema), string(e.Table.Table))

				columns := make([]string, 0, len(e.Table.ColumnName))
				for _, col := range e.Table.ColumnName {
					columns = append(columns, string(col))
				}

				schemaSignature := strings.Join(columns, ",")

				cachedSignature, exists := schemaCache[tableKey]

				if !exists || cachedSignature != schemaSignature {
					if err := metadata.WriteSchemaMetadata(ctx, s3Sink, string(e.Table.Schema), string(e.Table.Table), columns); err != nil {
						log.Printf("failed to write schema metadata: %v", err)
					} else {
						schemaCache[tableKey] = schemaSignature
						log.Printf("Schema updated for %s", tableKey)
					}
				}

				cdcEvents := parser.ParseCDCEvent(e, event.Header.EventType, event.Header.Timestamp)
				for _, cdcEvent := range cdcEvents {

					log.Println("sending event to channel...")
					cdcMessage := models.CDCMessage{
						Event:      cdcEvent,
						BinlogFile: currentBinlogFile,
						BinlogPos:  event.Header.LogPos,
					}

					select {
					case eventsChan <- cdcMessage:
						log.Println("event queued")
					case <-ctx.Done():
						log.Println("Context cancelled, breaking from event loop...")
						return
					}
				}
			}
		}
	})

	if err := os.MkdirAll("data", 0755); err != nil {
		log.Fatalf("failed to create data directory: %v", err)
	}

	// Consumer Goroutine
	wg.Go(func() {

		batches := make(map[string][]models.CDCMessage)
		batchCounters := make(map[string]int)

		for msg := range eventsChan {
			tableKey := fmt.Sprintf("%s/%s", msg.Event.Database, msg.Event.Table)
			batches[tableKey] = append(batches[tableKey], msg)

			currentBatch := batches[tableKey]
			if len(currentBatch) >= batchSize {
				batchCounters[tableKey]++
				if err := processor.FlushBatch(ctx, s3Sink, currentBatch, tableKey, batchCounters[tableKey]); err != nil {
					log.Printf("failed to flush batch: %v", err)
					return
				}

				lastMessage := batches[tableKey][len(batches[tableKey])-1]

				if err := cp.Update(lastMessage.BinlogFile, lastMessage.BinlogPos); err != nil {
					log.Printf("failed to save checkpoint: %v", err)
				}

				log.Printf("checkpoint updated → %s:%d", lastMessage.BinlogFile, lastMessage.BinlogPos)

				// clear only this table's batch to save memory
				batches[tableKey] = batches[tableKey][:0]
			}
		}

		for tableKey, batch := range batches {
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer flushCancel()

			if len(batch) == 0 {
				continue
			}

			log.Printf("Flushing remaining %d events...", len(batch))

			batchCounters[tableKey]++

			if err := processor.FlushBatch(flushCtx, s3Sink, batch, tableKey, batchCounters[tableKey]); err != nil {
				log.Printf("failed to flush batch: %v", err)
				continue
			}

			lastMessage := batch[len(batch)-1]

			if err := cp.Update(lastMessage.BinlogFile, lastMessage.BinlogPos); err != nil {
				log.Printf("failed to save checkpoint: %v", err)
			} else {
				log.Printf("checkpoint updated → %s:%d", lastMessage.BinlogFile, lastMessage.BinlogPos)
			}
		}

		log.Println("Consumer stopped!")
	})

	wg.Wait()

	// Final checkpoint save before shutdown
	if err := cp.Save(); err != nil {
		log.Printf("failed to save final checkpoint: %v", err)
	}

	log.Println("CDC engine stopped gracefully!")
}
