package creator

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/prometheus/common/log"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/types"
)

// LogStreamCreator defines the log creator creator.
type LogStreamCreator struct {
	logger log.Logger
	cwLogsClient types.CloudwatchLogStreamsInterface
}

// NewLogStreamCreator creates a new log creator creator.
func NewLogStreamCreator(logger log.Logger, cwLogsClient types.CloudwatchLogStreamsInterface) *LogStreamCreator {
	return &LogStreamCreator{
		logger: logger,
		cwLogsClient: cwLogsClient,
	}
}

// Ensure checks if log creator exists, and if not creates one.
func (c *LogStreamCreator) Ensure(ctx context.Context, name string) error {
	stream, err := c.Fetch(ctx, name)
	if err != nil {
		return err
	}
	if stream == nil {
		return c.Create(ctx, name)
	}
	return nil
}

// Fetch fetches a log creator by name.
func (c *LogStreamCreator) Fetch(ctx context.Context, name string) (*awstypes.LogStream, error) {
	out, err := c.cwLogsClient.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{
		LogStreamNamePrefix: aws.String(name),
	})
	if err != nil {
		return &awstypes.LogStream{}, fmt.Errorf("failed describing log group %s: %w", name, err)
	}
	for _, g := range out.LogStreams {
		// Check we have a match.
		if *g.LogStreamName == name {
			return &g, nil
		}
	}
	return &awstypes.LogStream{}, nil
}

// Create creates a log creator.
func (c *LogStreamCreator) Create(ctx context.Context, name string) error {
	_, err := c.cwLogsClient.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogStreamName: aws.String(name),
	})
	if err != nil {
		var awsErr *awstypes.ResourceAlreadyExistsException
		if errors.As(err, &awsErr) {
			return nil
		}
		return fmt.Errorf("failed creating stream %s: %w", name, err)
	}

	return nil
}
