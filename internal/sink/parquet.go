package sink

import (
	"fmt"
	"os"
	"path/filepath"

	parquet "github.com/parquet-go/parquet-go"

	"github.com/Benson-14/bin-flow/internal/models"
)

func WriteParquetBatch(filePath string, batch []models.CDCParquetRecord) error {
	if len(batch) == 0 {
		return fmt.Errorf("WriteParquetBatch: batch is empty, nothing to write")
	}

	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("WriteParquetBatch: create directories: %w", err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("WriteParquetBatch: create file %q: %w", filePath, err)
	}
	defer f.Close()

	// NewGenericWriter infers the Parquet schema from the Go type via reflection.
	writer := parquet.NewGenericWriter[models.CDCParquetRecord](f)

	if _, err := writer.Write(batch); err != nil {
		return fmt.Errorf("WriteParquetBatch: write rows: %w", err)
	}

	// Close flushes all buffered row-groups and writes the Parquet footer.
	if err := writer.Close(); err != nil {
		return fmt.Errorf("WriteParquetBatch: close writer: %w", err)
	}

	return nil
}
