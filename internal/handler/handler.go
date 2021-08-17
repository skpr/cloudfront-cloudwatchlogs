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
}

// NewEventHandler creates a new event handler.
func NewEventHandler(log log.Logger, downloadClient manager.DownloadAPIClient, cwLogsClient *cloudwatchlogs.Client) *EventHandler {
	return &EventHandler{
		log:            log,
		downloadClient: downloadClient,
		cwLogsClient:   cwLogsClient,
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
	logPusher, err := pusher.NewBatchLogPusher(ctx, h.cwLogsClient, logGroup, logStream, 256)
	if err != nil {
		return fmt.Errorf("error creating logger: %w", err)
	}

	h.log.Infof("Processing logs")
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

	// Clean up our log pusher?
	logPusher = nil

	h.log.Infof("Processing complete")

	return nil
}
