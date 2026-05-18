package sink

import (
	"encoding/json"

	"github.com/Benson-14/bin-flow/internal/models"
)

func BuildParquetRecord(event models.CDCEvent) (models.CDCParquetRecord, error) {
	dataJSON, err := marshalMap(event.Data)
	if err != nil {
		return models.CDCParquetRecord{}, err
	}

	oldDataJSON, err := marshalMap(event.OldData)
	if err != nil {
		return models.CDCParquetRecord{}, err
	}

	return models.CDCParquetRecord{
		Database:    event.Database,
		Table:       event.Table,
		Operation:   event.Operation,
		Timestamp:   int64(event.Timestamp),
		DataJSON:    dataJSON,
		OldDataJSON: oldDataJSON,
	}, nil
}

func marshalMap(m map[string]interface{}) (string, error) {
	if m == nil {
		return "{}", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
