package types

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
)

// CloudwatchLogsPutInterface provides an interface for the cloudwatch logs cwLogsClient.
type CloudwatchLogsPutInterface interface {
	PutLogEvents(ctx context.Context, params *cloudwatchlogs.PutLogEventsInput, optFns ...func(options *cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error)
}

// CloudwatchLogGroupsInterface provides an interface for creating log groups.
type CloudwatchLogGroupsInterface interface {
	CreateLogGroup(ctx context.Context, params *cloudwatchlogs.CreateLogGroupInput, optFns ...func(options *cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogGroupOutput, error)
	DescribeLogGroups(ctx context.Context, params *cloudwatchlogs.DescribeLogGroupsInput, optFns ...func(options *cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogGroupsOutput, error)
}

// CloudwatchLogStreamsInterface provides an interface for creating log streams.
type CloudwatchLogStreamsInterface interface {
	CreateLogStream(ctx context.Context, params *cloudwatchlogs.CreateLogStreamInput, optFns ...func(options *cloudwatchlogs.Options)) (*cloudwatchlogs.CreateLogStreamOutput, error)
	DescribeLogStreams(ctx context.Context, params *cloudwatchlogs.DescribeLogStreamsInput, optFns ...func(options *cloudwatchlogs.Options)) (*cloudwatchlogs.DescribeLogStreamsOutput, error)
}
