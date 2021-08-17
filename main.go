package main

import (
	"context"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/common/log"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/parser"
	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/processor"
	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/pusher"
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
	cwclient := cloudwatchlogs.NewFromConfig(cfg)

	for _, record := range event.Records {
		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, record.S3.Bucket.Name, record.S3.Object.Key)
		err := handleEvent(ctx, s3Client, cwclient, record)
		if err != nil {
			return err
		}
	}
	return nil
}

// Helper function to handle a single S3 event.
func handleEvent(ctx context.Context, s3client manager.DownloadAPIClient, cwclient *cloudwatchlogs.Client, record events.S3EventRecord) error {
	l := log.NewLogger(os.Stderr)

	key := record.S3.Object.Key
	bucket := record.S3.Bucket.Name
	l.Infof("Downloading logs %s from s3 bucket %s", key, bucket)
	downloader := manager.NewDownloader(s3client)
	gzipBuff := manager.NewWriteAtBuffer([]byte{})
	n, err := downloader.Download(ctx, gzipBuff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download %s from %s: %w", key, bucket, err)
	}
	l.Infof("Fetched %s from %s from %s", byteCountDecimal(n), key, bucket)

	l.Infof("Creating log pusher")
	logGroup, logStream := parser.ParseLogGroupAndStream(key)
	logPusher, err := pusher.NewBatchLogPusher(ctx, cwclient, logGroup, logStream, 256)
	if err != nil {
		return fmt.Errorf("error creating logger: %w", err)
	}

	l.Infof("Processing logs")
	err = processor.ProcessLines(gzipBuff.Bytes(), func(event types.InputLogEvent) error {
		err = logPusher.Add(ctx, event)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	err = logPusher.Flush(ctx)
	if err != nil {
		return err
	}
	l.Infof("Processing complete")

	return nil
}

func byteCountDecimal(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}
