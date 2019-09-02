package main

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/skpr/cloudfront-cloudwatchlogs/cmd/discover"
	"github.com/skpr/cloudfront-cloudwatchlogs/cmd/discoverwatch"
	"github.com/skpr/cloudfront-cloudwatchlogs/cmd/version"
)

func main() {
	app := kingpin.New("cloudfront-cloudwatchlogs", "utility to synchronise cloudfront logs to cloudwatch")

	version.Cmd(app)
	discoverwatch.Cmd(app)
	discover.Cmd(app)

	kingpin.MustParse(app.Parse(os.Args[1:]))
}
