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

// LogGroupCreator defines the log creator creator.
type LogGroupCreator struct {
	logger log.Logger
	cwLogsClient types.CloudwatchLogGroupsInterface
}

// NewLogGroupCreator creates a new log creator creator.
func NewLogGroupCreator(logger log.Logger, cwLogsClient types.CloudwatchLogGroupsInterface) *LogGroupCreator {
	return &LogGroupCreator{
		logger: logger,
		cwLogsClient: cwLogsClient,
	}
}

// Ensure checks if log creator exists, and if not creates one.
func (c *LogGroupCreator) Ensure(ctx context.Context, name string) error {
	group, err := c.Fetch(ctx, name)
	if err != nil {
		return err
	}
	if group == nil {
		return c.Create(ctx, name)
	}
	return nil
}

// Fetch fetches a log creator by name.
func (c *LogGroupCreator) Fetch(ctx context.Context, name string) (*awstypes.LogGroup, error) {
	out, err := c.cwLogsClient.DescribeLogGroups(ctx, &cloudwatchlogs.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String(name),
	})
	if err != nil {
		return &awstypes.LogGroup{}, fmt.Errorf("failed describing log group %s: %w", name, err)
	}
	for _, g := range out.LogGroups {
		// Check we have a match.
		if *g.LogGroupName == name {
			return &g, nil
		}
	}
	return &awstypes.LogGroup{}, nil
}

// Create creates a log creator.
func (c *LogGroupCreator) Create(ctx context.Context, name string) error {
	_, err := c.cwLogsClient.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(name),
	})
	if err != nil {
		var awsErr *awstypes.ResourceAlreadyExistsException
		if errors.As(err, &awsErr) {
			c.logger.Warnf("log group %s already exists", name)
			return nil
		}
		return fmt.Errorf("failed creating log group %s: %w", name, err)
	}

	return nil
}
