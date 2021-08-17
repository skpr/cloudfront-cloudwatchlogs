package pusher

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/types"
)


// BatchLogPusher cwLogsClient for handling log events.
// @TODO convert into lib reused by fluentbit-cloudwatchlogs
type BatchLogPusher struct {
	// BatchLogPusher for interacting with CloudWatch Logs.
	cwLogsClient types.CloudwatchLogsInterface
	// Group which events will be pushed to.
	Group string
	// Stream which events will be pushed to.
	Stream string
	// Amount of events to keep before flushing.
	batchSize int
	// Events stored in memory before being pushed.
	events []awstypes.InputLogEvent
	// Lock to ensure logs are handled by only 1 process.
	lock sync.Mutex
}

// NewBatchLogPusher creates a new batch log pusher.
func NewBatchLogPusher(ctx context.Context, cwLogsClient types.CloudwatchLogsInterface, group, stream string, batchSize int) (*BatchLogPusher, error) {
	pusher := &BatchLogPusher{
		Group:        group,
		Stream:       stream,
		cwLogsClient: cwLogsClient,
		batchSize:    batchSize,
	}
	err := pusher.initialize(ctx)
	return pusher, err
}

// initialize the log pusher by creating log groups and streams.
func (c *BatchLogPusher) initialize(ctx context.Context) error {
	err := c.createLogGroup(ctx)
	if err != nil {
		return fmt.Errorf("failed to create log group %s: %w", c.Group, err)
	}
	err = c.createLogStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create log stream %s for group %s: %w", c.Stream, c.Group, err)
	}
	return nil
}

// Add event to the cwLogsClient.
func (c *BatchLogPusher) Add(ctx context.Context, event awstypes.InputLogEvent) error {
	c.events = append(c.events, event)

	if len(c.events) >= c.batchSize {
		return c.Flush(ctx)
	}

	return nil
}

// Flush events stored in the cwLogsClient.
func (c *BatchLogPusher) Flush(ctx context.Context) error {
	// Return early if there are no events to push.
	if len(c.events) == 0 {
		return nil
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	input := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(c.Group),
		LogStreamName: aws.String(c.Stream),
		LogEvents:     c.events,
	}

	// Reset the logs back to empty.
	c.events = []awstypes.InputLogEvent{}

	return c.putLogEvents(ctx, input)
}

// PutLogEvents will attempt to execute and handle invalid tokens.
func (c *BatchLogPusher) putLogEvents(ctx context.Context, input *cloudwatchlogs.PutLogEventsInput) error {
	_, err := c.cwLogsClient.PutLogEvents(ctx, input)
	if err != nil {
		if exception, ok := err.(*awstypes.InvalidSequenceTokenException); ok {
			log.Println("Refreshing token:", &input.LogGroupName, &input.LogStreamName)
			input.SequenceToken = exception.ExpectedSequenceToken
			return c.putLogEvents(ctx, input)
		}
		return err
	}

	return nil
}

// createLogGroup will attempt to create a log group and not return an error if it already exists.
func (c *BatchLogPusher) createLogGroup(ctx context.Context) error {
	_, err := c.cwLogsClient.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(c.Group),
	})
	if err != nil {
		var awsErr *awstypes.ResourceAlreadyExistsException
		if errors.As(err, &awsErr) {
			return nil
		}
		return err
	}

	return nil
}

// createLogStream will attempt to create a log stream and not return an error if it already exists.
func (c *BatchLogPusher) createLogStream(ctx context.Context) error {
	_, err := c.cwLogsClient.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(c.Group),
		LogStreamName: aws.String(c.Stream),
	})
	if err != nil {
		var awsErr *awstypes.ResourceAlreadyExistsException
		if errors.As(err, &awsErr) {
			return nil
		}
		return err
	}

	return nil
}
