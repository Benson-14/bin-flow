package sink

import (
	"fmt"
	"time"
)

func BuildPartitionPath(tableKey string, t time.Time) string {
	return fmt.Sprintf(
		"%s/year=%04d/month=%02d/day=%02d",
		tableKey,
		t.Year(),
		t.Month(),
		t.Day(),
	)
}

func BuildParquetFilePath(tableKey string, batchNumber int) string {
	partitionPath := BuildPartitionPath(tableKey, time.Now().UTC())

	return fmt.Sprintf(
		"data/%s/batch-%03d.parquet",
		partitionPath,
		batchNumber,
	)
}
