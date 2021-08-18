package pusher

import (
	"context"
	"errors"
	"log"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go/aws"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/types"
)

// CreateLogGroup for events to be stored.
func CreateLogGroup(ctx context.Context, client types.CloudwatchLogsInterface, groupName string) error {
	_, err := client.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: aws.String(groupName),
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

// CreateLogStream for events to be stored.
func CreateLogStream(ctx context.Context, client types.CloudwatchLogsInterface, groupName, streamName string) error {
	_, err := client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
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

// PutLogEvents while handling event sorting, refresh tokens and clearing events.
func PutLogEvents(ctx context.Context, client types.CloudwatchLogsInterface, input *cloudwatchlogs.PutLogEventsInput) error {
	if len(input.LogEvents) == 0 {
		return nil
	}

	sort.Slice(input.LogEvents, func(i, j int) bool {
		a := *input.LogEvents[i].Timestamp
		b := *input.LogEvents[j].Timestamp
		return a < b
	})

	if input.SequenceToken != nil {
		log.Printf("Sending %d logs to %s/%s with sequence token: %s", len(input.LogEvents), *input.LogGroupName, *input.LogStreamName, *input.SequenceToken)
	} else {
		log.Printf("Sending %d logs to %s/%s", len(input.LogEvents), *input.LogGroupName, *input.LogStreamName)
	}

	resp, err := client.PutLogEvents(ctx, input)
	if err != nil {
		var seqTokenError *awstypes.InvalidSequenceTokenException
		if errors.As(err, &seqTokenError) {
			log.Printf("Invalid token. Refreshing token for %s/%s\n", *input.LogGroupName, *input.LogStreamName)
			input.SequenceToken = seqTokenError.ExpectedSequenceToken
			return PutLogEvents(ctx, client, input)
		}

		var alreadyAccErr *awstypes.DataAlreadyAcceptedException
		if errors.As(err, &alreadyAccErr) {
			log.Printf("Data already accepted. Refreshing token for %s/%s\n", *input.LogGroupName, *input.LogStreamName)
			input.SequenceToken = alreadyAccErr.ExpectedSequenceToken
			return PutLogEvents(ctx, client, input)
		}

		return err
	}

	// The request was successful so we are setting the sequence token so we are
	// configured to the next put request.
	input.SequenceToken = resp.NextSequenceToken

	// The request was successful so we can clear out the data in the request object.
	input.LogEvents = nil

	return nil
}
