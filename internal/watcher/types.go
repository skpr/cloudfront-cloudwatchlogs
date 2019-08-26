package watcher

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/arn"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/prometheus/common/log"
	"github.com/skpr/cloudfront-cloudwatchlogs/internal/loghandler"
	"golang.org/x/net/context"
)

type Watcher struct {
	Wg *sync.WaitGroup
	Logger 			 log.Logger

	ClientSQS        *sqs.SQS
	ClientS3         *s3.S3
	ClientCloudwatch *cloudwatchlogs.CloudWatchLogs

	QueueARN       *string
	DistributionId *string
	LogGroup       *string
	LogStream      *string
}

func (w *Watcher) Watch(ctx context.Context) error {
	defer w.Wg.Done()
	defer w.Logger.Debug("watch loop complete")

	// First up, get the Queue URL.
	queueArn, err := arn.Parse(*w.QueueARN)
	if err != nil {
		return err
	}
	getQueueUrlIn := &sqs.GetQueueUrlInput{
		QueueName: &queueArn.Resource,
	}
	queueUrl, err := w.ClientSQS.GetQueueUrl(getQueueUrlIn)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	for {
		w.Logger.Debug("polling for messages...")

		// Receive messages.
		receiveMessageIn := &sqs.ReceiveMessageInput{
			MaxNumberOfMessages: aws.Int64(10),
			QueueUrl:            queueUrl.QueueUrl,
			WaitTimeSeconds:     aws.Int64(20),
		}
		message, err := w.ClientSQS.ReceiveMessage(receiveMessageIn)
		if err != nil {
			w.Logger.Debug("polling for messages...")
			continue
		}
		if len(message.Messages) == 0 {
			// @todo no messages in the wait time. Backoff.
			w.Logger.Debug("no messages in queue")
			continue
		}

		for _, msg := range message.Messages {
			wg.Add(1)
			messageLogger := w.Logger.With("message", *msg.MessageId)
			workerIn := loghandler.WorkerInput{
				Logger:		      messageLogger,
				ClientS3:         w.ClientS3,
				ClientSQS: w.ClientSQS,
				ClientCloudwatch: w.ClientCloudwatch,

				QueueUrl: queueUrl.QueueUrl,
				Message:          msg,
				DistributionId: w.DistributionId,
				LogGroup:       w.LogGroup,
				LogStream:      w.LogStream,
			}
			go loghandler.Worker(ctx, wg, workerIn)
		}
	}

	wg.Wait()

	fmt.Println("Doing the thing")
	return nil
}
