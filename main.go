package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/common/log"

	"github.com/skpr/cloudfront-cloudwatchlogs/internal/handler"
)

const (
	// defaultBatchSize is the default batch size
	defaultBatchSize = 1024
)

func main() {
	lambda.Start(HandleEvents)
}

// HandleEvents sent from AWS S3.
func HandleEvents(ctx context.Context, event events.SNSEvent) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed to setup client: %d", err)
	}

	s3Client := s3.NewFromConfig(cfg)

	cwLogsClient := cloudwatchlogs.NewFromConfig(cfg, func(options *cloudwatchlogs.Options) {
		// Setting max attempts to zero will allow the SDK to retry all retryable errors until the
		// request succeeds, or a non-retryable error is returned.
		// https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/retries-timeouts
		options.Retryer = retry.AddWithMaxAttempts(options.Retryer, 0)
	})
	logger := log.NewLogger(os.Stderr)

	batchSize, err := getBatchSize(err)
	if err != nil {
		return err
	}

	eventHandler := handler.NewEventHandler(logger, s3Client, cwLogsClient, batchSize)

	for _, r := range event.Records {
		var event events.S3Event

		if err := json.Unmarshal([]byte(r.SNS.Message), &event); err != nil {
			return err
		}

		for _, record := range event.Records {
			fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, record.S3.Bucket.Name, record.S3.Object.Key)

			err := eventHandler.HandleEvent(ctx, record)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// getBatchSize gets the batch size.
func getBatchSize(err error) (int, error) {
	batchSize := defaultBatchSize

	batchSizeEnv := os.Getenv("BATCH_SIZE")

	if batchSizeEnv != "" {
		batchSize, err = strconv.Atoi(batchSizeEnv)
		if err != nil {
			return 0, fmt.Errorf("invalid batch size %s: %w", batchSizeEnv, err)
		}
	}

	return batchSize, nil
}
