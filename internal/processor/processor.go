package processor

import (
	"context"
	"fmt"
	"log"

	"github.com/Benson-14/bin-flow/internal/models"
	"github.com/Benson-14/bin-flow/internal/sink"
)

func FlushBatch(ctx context.Context, s3Sink *sink.S3Sink, batch []models.CDCMessage, batchNumber int) error {
	log.Printf("flushing parquet batch %d with %d events", batchNumber, len(batch))

	records := make([]models.CDCParquetRecord, 0, len(batch))

	for _, message := range batch {
		record, err := sink.BuildParquetRecord(message.Event)
		if err != nil {
			log.Printf("failed to build parquet record: %v", err)
		}
		records = append(records, record)
	}

	fileName := fmt.Sprintf("data/batch-%03d.parquet", batchNumber)

	if err := sink.WriteParquetBatch(fileName, records); err != nil {
		return fmt.Errorf("WriteParquetBatch: %w", err)
	}

	if err := s3Sink.UploadFile(ctx, fileName, sink.BuildObjectKey(fileName)); err != nil {
		return fmt.Errorf("UploadFile: %w", err)
	}

	log.Printf("parquet batch %d flushed to %s", batchNumber, fileName)

	return nil
}
