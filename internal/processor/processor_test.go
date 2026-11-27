package processor

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/skpr/cloudfront-cloudwatchlogs/internal/processor/mock"
)

// TestProcessLines tests the ProcessLines function switching between the two log formats.
func TestProcessLines(t *testing.T) {
	files := []string{"testdata/test-logs-v1.gz", "testdata/test-logs-v2.gz"}
	for _, file := range files {
		contents, err := os.ReadFile(file)
		assert.NoError(t, err)
		processor := mock.NewProcessor()
		err = ProcessLines(contents, processor.Process)
		assert.NoError(t, err)
	}
}

// TestProcessLinesLegacy tests the ProcessLines function with the legacy log format.
func TestProcessLinesLegacy(t *testing.T) {
	contents, err := os.ReadFile("testdata/test-logs-v1.gz")
	assert.NoError(t, err)
	processor := mock.NewProcessor()
	err = ProcessLines(contents, processor.Process)
	assert.NoError(t, err)
	logEvents := processor.GetEvents()
	// Length should be number of lines minus 2 for the comments at the top.
	assert.Len(t, logEvents, 58)
	// Log date should be converted to a timestamp.
	assert.Equal(t, int64(1592451493000), *logEvents[0].Timestamp)
	// Logs are sorted in chronological order.
	assert.Less(t, *logEvents[0].Timestamp, *logEvents[len(logEvents)-1].Timestamp)
}

// TestProcessLinesJson tests the ProcessLines function with the cloudfront v2 log format.
func TestProcessLinesJson(t *testing.T) {
	contents, err := os.ReadFile("testdata/test-logs-v2.gz")
	assert.NoError(t, err)
	processor := mock.NewProcessor()
	err = ProcessLines(contents, processor.Process)
	assert.NoError(t, err)
	logEvents := processor.GetEvents()
	// Length should be number of lines minus 2 for the comments at the top.
	assert.Len(t, logEvents, 10)
	// Log date should be converted to a timestamp.
	assert.Equal(t, int64(1781049792000), *logEvents[0].Timestamp)
	// Logs are sorted in chronological order.
	assert.Less(t, *logEvents[0].Timestamp, *logEvents[len(logEvents)-1].Timestamp)
}
