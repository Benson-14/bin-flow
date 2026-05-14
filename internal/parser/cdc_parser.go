package parser

import (
	"github.com/Benson-14/bin-flow/internal/models"
	"github.com/go-mysql-org/go-mysql/replication"
)

func operationString(eventType replication.EventType) string {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return "INSERT"
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return "UPDATE"
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return "DELETE"
	default:
		return "UNKNOWN"
	}
}

func ParseCDCEvent(e *replication.RowsEvent, eventType replication.EventType, timestamp uint32) []models.CDCEvent {
	columns := e.Table.ColumnName
	operation := operationString(eventType)

	var events []models.CDCEvent

	if operation == "UPDATE" {
		for i := 0; i+1 < len(e.Rows); i += 2 {
			events = append(events, models.CDCEvent{
				Database:  string(e.Table.Schema),
				Table:     string(e.Table.Table),
				Operation: operation,
				Timestamp: timestamp,
				OldData:   rowToMap(columns, e.Rows[i]),
				Data:      rowToMap(columns, e.Rows[i+1]),
			})
		}
	} else {
		for _, row := range e.Rows {
			events = append(events, models.CDCEvent{
				Database:  string(e.Table.Schema),
				Table:     string(e.Table.Table),
				Operation: operation,
				Timestamp: timestamp,
				Data:      rowToMap(columns, row),
			})
		}
	}

	return events
}
