package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/common/log"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/handler"
)

func main() {
	lambda.Start(HandleEvents)
}

// HandleEvents sent from AWS S3.
func HandleEvents(ctx context.Context, event events.S3Event) error {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return fmt.Errorf("failed to setup client: %d", err)
	}
	s3Client := s3.NewFromConfig(cfg)
	cwLogsClient := cloudwatchlogs.NewFromConfig(cfg)
	logger := log.NewLogger(os.Stderr)

	eventHandler := handler.NewEventHandler(logger, s3Client, cwLogsClient)

	for _, record := range event.Records {
		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, record.S3.Bucket.Name, record.S3.Object.Key)
		err := eventHandler.HandleEvent(ctx, record)
		if err != nil {
			return err
		}
	}
	return nil
}
