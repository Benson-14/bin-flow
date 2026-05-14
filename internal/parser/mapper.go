package parser

import "github.com/go-mysql-org/go-mysql/replication"

func rowToMap(columns [][]byte, row []interface{}) map[string]interface{} {
	record := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i < len(row) {
			record[string(col)] = row[i]
		}
	}
	return record
}

func RowMap(e *replication.RowsEvent) []map[string]interface{} {
	var result []map[string]interface{}
	for _, row := range e.Rows {
		result = append(result, rowToMap(e.Table.ColumnName, row))
	}
	return result
}
