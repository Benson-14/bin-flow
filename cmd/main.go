package main

import (
	"context"
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
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

		switch event.Header.EventType {
		case replication.WRITE_ROWS_EVENTv2:
			fmt.Println("INSERT detected")
		case replication.UPDATE_ROWS_EVENTv2:
			fmt.Println("UPDATE detected")
		case replication.DELETE_ROWS_EVENTv2:
			fmt.Println("DELETE detected")
		case replication.XID_EVENT:
			fmt.Println("TRANSACTION COMMITTED")
		}

		switch e := event.Event.(type) {
		case *replication.RowsEvent:
			fmt.Println(e.Rows)
		}
	}
}
