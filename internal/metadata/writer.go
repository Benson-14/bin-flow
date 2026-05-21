package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Benson-14/bin-flow/internal/models"
	"github.com/Benson-14/bin-flow/internal/sink"
)

const schemaRoot = "schemas"

func WriteSchemaMetadata(ctx context.Context, s3Sink *sink.S3Sink, database string, table string, columns []string) error {

	metadata := models.SchemaMetadata{
		Database:  database,
		Table:     table,
		Columns:   columns,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// schemas/cdc_demo/orders/
	dirPath := filepath.Join(schemaRoot, database, table)

	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("create schema directory: %w", err)
	}
	filePath := filepath.Join(dirPath, "schema.json")

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal schema metadata: %w", err)
	}

	tmpFile := filePath + ".tmp"

	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("write temp schema file: %w", err)
	}

	if err := os.Rename(tmpFile, filePath); err != nil {
		return fmt.Errorf("rename schema file: %w", err)
	}

	objectKey := filePath
	if err := s3Sink.UploadFile(ctx, filePath, objectKey); err != nil {
		return fmt.Errorf("upload schema file: %w", err)
	}

	return nil

}
