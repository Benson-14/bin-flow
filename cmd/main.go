package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/Benson-14/bin-flow/internal/checkpoint"
	"github.com/Benson-14/bin-flow/internal/parser"
)

const checkpointFile = "checkpoint.json"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalChan := make(chan os.Signal, 1)

	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signalChan)

	go func() {
		sig := <-signalChan
		log.Printf("received signal: %v", sig)
		cancel()
	}()

	cp, err := checkpoint.LoadCheckpoint(checkpointFile)
	if err != nil {
		log.Fatalf("failed to load checkpoint: %v", err)
	}

	fmt.Printf("Resuming from %s @ position %d\n", cp.BinlogFile, cp.BinlogPos)

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

	fmt.Println("Listening for binlog events...")

	for {
		event, err := steamer.GetEvent(ctx)
		if err != nil {
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
				b, _ := json.MarshalIndent(cdcEvent, "", "  ")
				fmt.Println(string(b))
			}
			// update checkpoint after processing each rows event, will change later to happen after XIDEvent
			if err := cp.Update(cp.BinlogFile, event.Header.LogPos); err != nil {
				log.Printf("failed to save checkpoint: %v", err)
			}
		}
	}

	if err := cp.Save(); err != nil {
		log.Printf("failed to save final checkpoint: %v", err)
	}

	log.Println("CDC engine stopped gracefully")
}
