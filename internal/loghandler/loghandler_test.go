package loghandler

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"testing"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/stretchr/testify/assert"
)

func TestChunkMessages(t *testing.T) {
	unchunked := []*cloudwatchlogs.InputLogEvent{}

	for i := 0; i < 15; i++ {
		m := &cloudwatchlogs.InputLogEvent{
			Message: aws.String(fmt.Sprintf("item %d", i)),
		}
		unchunked = append(unchunked, m)
	}

	chunked := chunkMessages(unchunked, 5)
	assert.Equal(t, 3, len(chunked), "number of chunks correct")
	for _, chunk := range chunked {
		assert.Equal(t, 5, len(chunk), "number of items in chunk correct")
	}
}