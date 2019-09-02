package watch

import (
	"context"
	"github.com/pkg/errors"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudfront"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/skpr/cloudfront-cloudwatchlogs/internal/discovery"
	l "github.com/skpr/cloudfront-cloudwatchlogs/internal/logger"
	"github.com/skpr/cloudfront-cloudwatchlogs/internal/watcher"
)

const (
	// DefaultRegion is the default AWS region to use for cloudwatchlogs.
	DefaultRegion           string = "ap-southeast-2"
	// DefaultVerbosity is the default logging verbosity of the app.
	DefaultVerbosity	    string = "info"
)

type cmdWatch struct {
	// Verbosity defines the log level.
	Verbosity string
	// Region defines the cloudwatch region.
	Region string
	// Distribution is the ID of the cloudfront distro.
	Distribution string
	// LogGroup defines the logGroup.
	LogGroup string
	// LogStream defines the logStream.
	LogStream string
}

func (cmd *cmdWatch) run(c *kingpin.ParseContext) error {
	loggerKeys := map[string]string{"region": cmd.Region}
	logger := l.NewWithKeys(loggerKeys)
	_ = logger.SetLevel(cmd.Verbosity)
	logger.Debug("initialising")

	config := &aws.Config{
		Region: aws.String(cmd.Region),
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return errors.Wrap(err, "unable to initialise aws session")
	}

	clientCloudfront := cloudfront.New(sess)
	clientS3 := s3.New(sess)
	clientSqs := sqs.New(sess)
	clientCloudwatch := cloudwatchlogs.New(sess)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	loggerKeys["distribution"] = cmd.Distribution
	distlogger := l.NewWithKeys(loggerKeys)
	wg := sync.WaitGroup{}
	_ = distlogger.SetLevel(cmd.Verbosity)
	w := watcher.Watcher{
		Logger:           distlogger,
		Wg:               &wg,
		ClientS3:         clientS3,
		ClientSQS:        clientSqs,
		ClientCloudwatch: clientCloudwatch,
		DistributionID:   &cmd.Distribution,
		LogGroup: &cmd.LogGroup,
		LogStream: &cmd.LogStream,
	}
	distlogger.Info("candidate distribution found")

	in := &discovery.GetDistributionLogQueueInput{
		ClientS3:         clientS3,
		ClientCloudfront: clientCloudfront,
		DistributionID:   &cmd.Distribution,
	}
	queue, err := discovery.GetDistributionLogQueue(in)
	if err != nil {
		distlogger.Errorf("couldn't find sqs queue for distribution logs")
		return err
	}

	w.QueueARN = queue
	distlogger.Debugf("sqs queue for logs is %s", *queue)
	err = w.Watch(ctx)
	return err
}

// Cmd declares the "watch" sub command.
func Cmd(app *kingpin.Application) {
	cmd := new(cmdWatch)
	c := app.Command("watch", "Discover CloudFront distributions with logging configured").Action(cmd.run)
	c.Flag("region", "AWS region for cloudwatchlogs").Default(DefaultRegion).
		Envar("AWS_REGION").
		StringVar(&cmd.Region)
	c.Flag("distribution", "ID of the cloudfront distribution").
		Required().
		StringVar(&cmd.Distribution)
	c.Flag("group", "Tag name on cloudfront distribution to use for log group").
		Required().
		StringVar(&cmd.LogGroup)
	c.Flag("stream", "Tag name on cloudfront distribution to use for log stream").
		Required().
		StringVar(&cmd.LogStream)
	c.Flag("verbosity", "Verbosity level").
		Default(DefaultVerbosity).
		HintOptions("panic", "fatal", "error",  "warn", "warning", "info", "debug", "trace").
		StringVar(&cmd.Verbosity)
}
