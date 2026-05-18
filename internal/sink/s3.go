package sink

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Sink struct {
	client *s3.Client
	bucket string
}

func NewS3Sink(ctx context.Context, bucket string) (*S3Sink, error) {

	cfg, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion("us-east-1"),

		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				"test",
				"test",
				"",
			),
		),

		// Override endpoint for LocalStack
		awsconfig.WithBaseEndpoint(
			"http://localhost:4566",
		),
	)

	if err != nil {
		return nil, fmt.Errorf(
			"load aws config: %w",
			err,
		)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {

		// LocalStack requires path-style URLs
		o.UsePathStyle = true
	})

	return &S3Sink{
		client: client,
		bucket: bucket,
	}, nil
}

func (s *S3Sink) UploadFile(
	ctx context.Context,
	filePath string,
	objectKey string,
) error {

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf(
			"open file %q: %w",
			filePath,
			err,
		)
	}

	defer file.Close()

	_, err = s.client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket: aws.String(s.bucket),
			Key:    aws.String(objectKey),
			Body:   file,
		},
	)

	if err != nil {
		return fmt.Errorf("upload object %q: %w", objectKey, err)
	}

	return nil
}

func BuildObjectKey(filePath string) string {

	// Example:
	// data/batch-001.parquet
	// → batch-001.parquet

	return filepath.Base(filePath)
}
