package pusher

import (
	"context"
	"fmt"
	"runtime"
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
	for i := 0; i < 3; i++ {
		err = logPusher.Add(ctx, types.InputLogEvent{
			Message:   aws.String("foo"),
			Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond/time.Nanosecond)),
		})
		assert.NoError(t, err)
	}
	// Check we have no events in our buffer.
	assert.Empty(t, logPusher.events)

}

func TestBatchLogPusher_AddMany(t *testing.T) {
	PrintMemUsage()
	cwlogs := mock.NewCloudwatchLogs()
	group := "foo"
	stream := "bar"
	batchSize := 3
	ctx := context.TODO()
	logPusher, err := NewBatchLogPusher(ctx, cwlogs, group, stream, batchSize)
	assert.NoError(t, err)

	// Add 3 events.
	for i := 0; i < 10_000_000; i++ {
		err = logPusher.Add(ctx, types.InputLogEvent{
			Message:   aws.String("foo"),
			Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond/time.Nanosecond)),
		})
		assert.NoError(t, err)
	}
	// Check we have no events in our buffer.

	PrintMemUsage()

	// Force GC to clear up, should see a memory drop
	runtime.GC()
	PrintMemUsage()
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
