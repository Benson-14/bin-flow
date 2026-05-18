package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/Benson-14/bin-flow/internal/checkpoint"
	"github.com/Benson-14/bin-flow/internal/models"
	"github.com/Benson-14/bin-flow/internal/parser"
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

	// CDC Event Channel
	eventsChan := make(chan models.CDCMessage, 100)

	var wg sync.WaitGroup

	fmt.Println("Listening for binlog events...")

	// Producer Goroutine
	// Reads replication stream and sends events to eventsChan
	wg.Go(func() {

		defer close(eventsChan)

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
				if newFile != cp.BinlogFile {
					fmt.Printf("Binlog rotated → %s\n", newFile)
					if err := cp.Update(newFile, uint32(e.Position)); err != nil {
						log.Printf("failed to save checkpoint after rotation: %v", err)
					}
				}

			case *replication.RowsEvent:
				cdcEvents := parser.ParseCDCEvent(e, event.Header.EventType, event.Header.Timestamp)
				for _, cdcEvent := range cdcEvents {

					log.Println("sending event to channel...")
					cdcMessage := models.CDCMessage{
						Event:      cdcEvent,
						BinlogFile: cp.BinlogFile,
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

		batch := make([]models.CDCMessage, 0, batchSize)
		var batchCounter int

		for msg := range eventsChan {
			batch = append(batch, msg)

			if len(batch) >= batchSize {
				batchCounter++
				if err := flushBatch(batch, batchCounter); err != nil {
					log.Printf("failed to flush batch: %v", err)
					continue
				}

				lastMessage := batch[len(batch)-1]

				if err := cp.Update(lastMessage.BinlogFile, lastMessage.BinlogPos); err != nil {
					log.Printf("failed to save checkpoint: %v", err)
				}

				log.Printf("checkpoint updated → %s:%d", lastMessage.BinlogFile, lastMessage.BinlogPos)

				// Reset batch to clear memory
				batch = batch[:0]
			}
		}

		if len(batch) > 0 {
			log.Printf("Flushing remaining %d events...", len(batch))
			batchCounter++

			if err := flushBatch(batch, batchCounter); err != nil {
				log.Printf("failed to flush batch: %v", err)
			} else {

				lastMessage := batch[len(batch)-1]

				if err := cp.Update(lastMessage.BinlogFile, lastMessage.BinlogPos); err != nil {
					log.Printf("failed to save checkpoint: %v", err)
				} else {
					log.Printf("checkpoint updated → %s:%d", lastMessage.BinlogFile, lastMessage.BinlogPos)
				}
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

func flushBatch(batch []models.CDCMessage, batchNumber int) error {
	log.Printf("flushing parquet batch %d with %d events", batchNumber, len(batch))

	records := make([]models.CDCParquetRecord, 0, len(batch))

	for _, message := range batch {
		record, err := sink.BuildParquetRecord(message.Event)
		if err != nil {
			log.Printf("failed to build parquet record: %v", err)
			continue
		}
		records = append(records, record)
	}

	fileName := fmt.Sprintf("data/batch-%03d.parquet", batchNumber)

	if err := sink.WriteParquetBatch(fileName, records); err != nil {
		return fmt.Errorf("WriteParquetBatch: %w", err)
	}

	log.Printf("parquet batch %d flushed to %s", batchNumber, fileName)

	return nil
}
