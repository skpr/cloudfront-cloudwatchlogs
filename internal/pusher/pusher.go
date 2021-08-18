package pusher

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/prometheus/common/log"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/types"
	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/utils"
)

// BatchLogPusher cwLogsClient for handling log events.
// @TODO convert into lib reused by fluentbit-cloudwatchlogs
type BatchLogPusher struct {
	// log for logging.
	log log.Logger
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
	// eventsSize of the current batch in bytes.
	eventsSize int64
	// Lock to ensure logs are handled by only 1 process.
	lock sync.Mutex
}

// NewBatchLogPusher creates a new batch log pusher.
func NewBatchLogPusher(ctx context.Context, logger log.Logger, cwLogsClient types.CloudwatchLogsInterface, group, stream string, batchSize int) (*BatchLogPusher, error) {
	pusher := &BatchLogPusher{
		log:          logger,
		Group:        group,
		Stream:       stream,
		cwLogsClient: cwLogsClient,
		batchSize:    batchSize,
	}
	err := pusher.initialize(ctx)
	return pusher, err
}

// initialize the log pusher by creating log groups and streams.
func (p *BatchLogPusher) initialize(ctx context.Context) error {
	err := p.createLogGroup(ctx)
	if err != nil {
		return fmt.Errorf("failed to create log group %s: %w", p.Group, err)
	}
	err = p.createLogStream(ctx)
	if err != nil {
		return fmt.Errorf("failed to create log stream %s for group %s: %w", p.Stream, p.Group, err)
	}
	return nil
}

// Add event to the cwLogsClient.
func (p *BatchLogPusher) Add(ctx context.Context, event awstypes.InputLogEvent) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.events = append(p.events, event)
	p.updateEventsSize(event)

	if len(p.events) >= p.batchSize {
		return p.Flush(ctx)
	}

	return nil
}

// Flush events stored in the cwLogsClient.
func (p *BatchLogPusher) Flush(ctx context.Context) error {
	// Return early if there are no events to push.
	if len(p.events) == 0 {
		return nil
	}

	payloadSize := p.calculatePayloadSize()
	p.log.Infof("Pushing %v log events with payload of %s", len(p.events), utils.ByteCountBinary(payloadSize))

	// Sort events chronologically.
	p.sortEvents()

	input := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(p.Group),
		LogStreamName: aws.String(p.Stream),
		LogEvents:     p.events,
	}

	err := p.putLogEvents(ctx, input)
	if err != nil {
		return err
	}

	// Reset the events buffer.
	p.clearEvents()

	return nil
}

// PutLogEvents will attempt to execute and handle invalid tokens.
func (p *BatchLogPusher) putLogEvents(ctx context.Context, input *cloudwatchlogs.PutLogEventsInput) error {
	_, err := p.cwLogsClient.PutLogEvents(ctx, input)
	if err != nil {
		if exception, ok := err.(*awstypes.InvalidSequenceTokenException); ok {
			p.log.Infof("Refreshing token:", &input.LogGroupName, &input.LogStreamName)
			input.SequenceToken = exception.ExpectedSequenceToken
			return p.putLogEvents(ctx, input)
		}
		return err
	}

	return nil
}

// createLogGroup will attempt to create a log group and not return an error if it already exists.
func (p *BatchLogPusher) createLogGroup(ctx context.Context) error {
	_, err := p.cwLogsClient.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(p.Group),
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
func (p *BatchLogPusher) createLogStream(ctx context.Context) error {
	_, err := p.cwLogsClient.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(p.Group),
		LogStreamName: aws.String(p.Stream),
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

func (p *BatchLogPusher) updateEventsSize(event awstypes.InputLogEvent) {
	line := int64(len(*event.Message))
	p.eventsSize = p.eventsSize + line
}

// calculatePayloadSize calculates the approximate payload size.
func (p *BatchLogPusher) calculatePayloadSize() int64 {
	// size is calculated as the sum of all event messages in UTF-8, plus 26 bytes for each log event.
	bytesOverhead := (len(p.events) + 1) * 26
	return p.eventsSize + int64(bytesOverhead)
}

// clearEvents clears the events buffer.
func (p *BatchLogPusher) clearEvents() {
	p.events = []awstypes.InputLogEvent{}
	p.eventsSize = 0
}

// sortEvents chronologically.
func (p *BatchLogPusher) sortEvents() {
	sort.Slice(p.events, func(i, j int) bool {
		a := *p.events[i].Timestamp
		b := *p.events[j].Timestamp
		return a < b
	})
}
