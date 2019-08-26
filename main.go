package main

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/skpr/cloudfront-cloudwatchlogs/cmd/version"
	"github.com/skpr/cloudfront-cloudwatchlogs/cmd/watchqueue"
)

func main() {
	app := kingpin.New("cloudfront-cloudwatchlogs", "utility to synchronise cloudfront logs to cloudwatch")

	version.Cmd(app)
	watchqueue.Cmd(app)

	kingpin.MustParse(app.Parse(os.Args[1:]))
}
