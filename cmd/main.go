package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/Benson-14/bin-flow/internal/parser"
)

func main() {

	cfg := replication.BinlogSyncerConfig{
		ServerID: 100,
		Flavor:   "mysql",
		Host:     "localhost",
		Port:     3307,
		User:     "replicator",
		Password: "replica_password",
	}

	syncer := replication.NewBinlogSyncer(cfg)

	steamer, err := syncer.StartSync(mysql.Position{
		Name: "mysql-bin.000001",
		Pos:  4,
	})

	if err != nil {
		panic(err)
	}

	fmt.Println("Listening for binlog events...")

	for {
		event, err := steamer.GetEvent(context.Background())
		if err != nil {
			panic(err)
		}

		switch e := event.Event.(type) {
		case *replication.RowsEvent:
			cdcEvents := parser.ParseCDCEvent(e, event.Header.EventType, event.Header.Timestamp)
			for _, cdcEvent := range cdcEvents {
				b, _ := json.MarshalIndent(cdcEvent, "", "  ")
				fmt.Println(string(b))
			}
		}

	}
}
