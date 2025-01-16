package handler

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/skpr/cloudfront-cloudwatchlogs/internal/parser"
	"github.com/skpr/cloudfront-cloudwatchlogs/internal/processor"
	"github.com/skpr/cloudfront-cloudwatchlogs/internal/pusher"
	"github.com/skpr/cloudfront-cloudwatchlogs/internal/utils"
)

const (
	// LogStreamName is the name of the log stream where all events will be pushed to.
	LogStreamName = "cloudfront"
)

// EventHandler defines the event handler.
type EventHandler struct {
	log            *slog.Logger
	downloadClient manager.DownloadAPIClient
	cwLogsClient   *cloudwatchlogs.Client
	batchSize      int
}

// NewEventHandler creates a new event handler.
func NewEventHandler(log *slog.Logger, downloadClient manager.DownloadAPIClient, cwLogsClient *cloudwatchlogs.Client, batchSize int) *EventHandler {
	return &EventHandler{
		log:            log,
		downloadClient: downloadClient,
		cwLogsClient:   cwLogsClient,
		batchSize:      batchSize,
	}
}

// HandleEvent handles the event.
func (h *EventHandler) HandleEvent(ctx context.Context, record events.S3EventRecord) error {
	key := record.S3.Object.Key
	bucket := record.S3.Bucket.Name
	h.log.Info(fmt.Sprintf("Downloading logs %s from s3 bucket %s", key, bucket))
	downloader := manager.NewDownloader(h.downloadClient)
	gzipBuff := manager.NewWriteAtBuffer([]byte{})
	n, err := downloader.Download(ctx, gzipBuff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download %s from %s: %w", key, bucket, err)
	}
	h.log.Info(fmt.Sprintf("Fetched %s from %s from %s", utils.ByteCountBinary(n), key, bucket))

	h.log.Info("Creating log pusher")
	logGroup := parser.GetLogGroupName(key)
	logPusher := pusher.NewBatchLogPusher(ctx, h.log, h.cwLogsClient, logGroup, LogStreamName, h.batchSize)

	h.log.Info("Creating log group")
	if err := logPusher.CreateLogGroup(ctx, logGroup); err != nil {
		return err
	}

	h.log.Info("Creating log stream")
	if err := logPusher.CreateLogStream(ctx, logGroup, LogStreamName); err != nil {
		return err
	}

	h.log.Info("Processing logs")
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

	h.log.Info("Processing complete")

	return nil
}
