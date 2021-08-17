package pusher

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/pusher/mock"
)

func TestBatchLogPusher_Add(t *testing.T) {
	cwlogs := mock.NewCloudwatchLogs()
	group := "foo"
	stream := "bar"
	batchSize := 3
	ctx := context.TODO()
	logPusher, err := NewBatchLogPusher(ctx, cwlogs, group, stream, batchSize)
	assert.NoError(t, err)

	// Add 3 events.
	err = logPusher.Add(ctx, types.InputLogEvent{
		Message:   aws.String("foo"),
		Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond/time.Nanosecond)),
	})
	assert.NoError(t, err)
	err = logPusher.Add(ctx, types.InputLogEvent{
		Message:   aws.String("foo"),
		Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond/time.Nanosecond)),
	})
	assert.NoError(t, err)
	err = logPusher.Add(ctx, types.InputLogEvent{
		Message:   aws.String("foo"),
		Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond/time.Nanosecond)),
	})
	assert.NoError(t, err)

	// Check we have no events in our buffer.
	assert.Empty(t, logPusher.events)

}
