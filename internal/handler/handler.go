package handler

import (
	"context"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/common/log"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/parser"
	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/processor"
	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/pusher"
	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/utils"
)

// EventHandler defines the event handler.
type EventHandler struct {
	log            log.Logger
	downloadClient manager.DownloadAPIClient
	cwLogsClient   *cloudwatchlogs.Client
	batchSize      int
}

// NewEventHandler creates a new event handler.
func NewEventHandler(log log.Logger, downloadClient manager.DownloadAPIClient, cwLogsClient *cloudwatchlogs.Client, batchSize int) *EventHandler {
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

	h.log.Infof("Downloading logs %s from s3 bucket %s", key, bucket)

	downloader := manager.NewDownloader(h.downloadClient)
	gzipBuff := manager.NewWriteAtBuffer([]byte{})
	n, err := downloader.Download(ctx, gzipBuff, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download %s from %s: %w", key, bucket, err)
	}

	h.log.Infof("Fetched %s from %s from %s", utils.ByteCountBinary(n), key, bucket)

	h.log.Infof("Creating log pusher")

	logGroup, logStream := parser.ParseLogGroupAndStream(key)

	if err := pusher.CreateLogGroup(ctx, h.cwLogsClient, logGroup); err != nil {
		return err
	}

	if err := pusher.CreateLogStream(ctx, h.cwLogsClient, logGroup, logStream); err != nil {
		return err
	}

	h.log.Infof("Processing logs")

	input := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(logGroup),
		LogStreamName: aws.String(logStream),
	}

	err = processor.ProcessLines(gzipBuff.Bytes(), func(event types.InputLogEvent) error {
		if len(input.LogEvents) >= h.batchSize {
			return pusher.PutLogEvents(ctx, h.cwLogsClient, input)
		}

		input.LogEvents = append(input.LogEvents, event)

		return nil
	})
	if err != nil {
		return err
	}

	err = pusher.PutLogEvents(ctx, h.cwLogsClient, input)
	if err != nil {
		return err
	}

	h.log.Infof("Processing complete")

	return nil
}
