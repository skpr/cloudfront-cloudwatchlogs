package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseLogGroupAndStream(t *testing.T) {
	key := "/skpr/my-cluster/my-project/dev/E38J4Y0L8GXH9D.2020-06-08-07.d51ccc94.gz"
	logGroup, logStream := parseLogGroupAndStream(key)

	assert.Equal(t, "/skpr/my-cluster/my-project/dev", logGroup)
	assert.Equal(t, "E38J4Y0L8GXH9D.2020-06-08-07.d51ccc94", logStream)
}
