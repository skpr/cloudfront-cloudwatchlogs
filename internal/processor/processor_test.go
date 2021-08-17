package processor

import (
	"io/ioutil"
	"testing"


	"github.com/stretchr/testify/assert"

	"github.com/codedropau/cloudfront-cloudwatchlogs/internal/processor/mock"
)

func TestProcessLines(t *testing.T) {
	contents, err := ioutil.ReadFile("testdata/test-logs.gz")
	assert.NoError(t, err)
	processor := mock.NewProcessor()
	err = ProcessLines(contents, processor.Process)
	assert.NoError(t, err)
	logEvents := processor.GetEvents()
	// Length should be number of lines minus 2 for the comments at the top.
	assert.Len(t, logEvents, 58)
	// Log date should be converted to a timestamp.
	assert.Equal(t, int64(1592451493000), *logEvents[0].Timestamp)
	// Logs are sorted in chronilogical order.
	assert.Less(t, *logEvents[0].Timestamp, *logEvents[len(logEvents)-1].Timestamp)
}
