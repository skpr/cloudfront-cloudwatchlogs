package loghandler

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"github.com/prometheus/common/log"

	cwl "github.com/skpr/cloudfront-cloudwatchlogs/internal/cloudwatchlogs"
	m "github.com/skpr/cloudfront-cloudwatchlogs/internal/sqs"
)

// WorkerInput defines parameters for Worker().
type WorkerInput struct {
	Logger 			 log.Logger
	ClientS3         *s3.S3
	ClientCloudwatch *cloudwatchlogs.CloudWatchLogs
	ClientSQS *sqs.SQS

	Message        *sqs.Message
	QueueURL       *string
	DistributionID *string
	LogGroup       *string
	LogStream      *string
}

// Worker runs the queue worker.
func Worker(ctx context.Context, wg *sync.WaitGroup, in WorkerInput) error {
	defer wg.Done()
	defer DeleteMessage(ctx, in)
	defer in.Logger.Debug("log worker complete")

	message := m.Message{}
	body := []byte(*in.Message.Body)
	err := json.Unmarshal(body, &message)
	if err != nil {
		errmsg := "unable to unmarshal message body"
		in.Logger.Error(errmsg)
		return errors.Wrap(err, errmsg)
	}

	// Download the s3 object.
	item := message.Records[0]
	in.Logger.Infof("fetching from s3://%s/%s", item.S3.Bucket.Name, item.S3.Object.Key)
	downloadIn := &s3.GetObjectInput{
		Bucket: aws.String(item.S3.Bucket.Name),
		Key:    aws.String(item.S3.Object.Key),
	}
	downloader := s3manager.NewDownloaderWithClient(in.ClientS3)
	buffRaw := &aws.WriteAtBuffer{}
	_, err = downloader.DownloadWithContext(ctx, buffRaw, downloadIn)
	if err != nil {
		errmsg := "unable to download log file"
		in.Logger.Error(errmsg)
		return errors.Wrap(err, errmsg)
	}
	buff := bytes.Buffer{}
	err = gunzipWrite(&buff, buffRaw.Bytes())
	if err != nil {
		errmsg := "unable to uncompress log file"
		in.Logger.Error(errmsg)
		return errors.Wrap(err, errmsg)
	}

	logLogger := in.Logger
	logLogger.With("loggroup", *in.LogGroup)
	logLogger.With("logstream", *in.LogStream)
	logWriter := cwl.Writer{
		Logger: logLogger,
		Client: in.ClientCloudwatch,
		LogGroup: in.LogGroup,
		LogStream: in.LogStream,
	}
	err = logWriter.EnsureLogStream(ctx)
	if err != nil {
		return errors.Wrap(err, "could not create log stream")
	}
	split := []byte("\n")
	lines := bytes.Split(buff.Bytes(), split)
	messages := make([]*cloudwatchlogs.InputLogEvent, 0)
	for _, line := range lines {
		if len(line) < 1 {
			// Nothing in this line - probably just a newline.
			continue
		}
		// @todo replace with hasPrefix in streams package.
		if string(line[0]) == "#" {
			// Comment - ignore.
			continue
		}
		message := string(line)

		// Parse date out of cloudfront access log line.
		date, err := parseDate(message)
		if err != nil {
			// Couldn't parse date, default to now.
			date = time.Now()
		}

		messages = append(messages, &cloudwatchlogs.InputLogEvent{
			Message: &message,
			Timestamp: aws.Int64(aws.TimeUnixMilli(date)),
		})
	}

	// Sort the messages chronologically.
	sort.Slice(messages, func(i, j int) bool {
		a := *messages[i].Timestamp
		b := *messages[j].Timestamp
		return a > b
	})

	err = logWriter.Stream(ctx, messages)
	if err != nil {
		errmsg := "could not push log events to cloudwatch"
		in.Logger.Error(errmsg, " ", err.Error())
		return errors.Wrap(err, errmsg)
	}
	return nil
}

// DeleteMessage removes a completes message from the queue.
func DeleteMessage(ctx context.Context, in WorkerInput) error {
	in.Logger.Debugf("deleting message from queue: %s", *in.Message.MessageId)
	delIn := &sqs.DeleteMessageInput{
		QueueUrl:      in.QueueURL,
		ReceiptHandle: in.Message.ReceiptHandle,
	}
	_, err := in.ClientSQS.DeleteMessageWithContext(ctx, delIn)
	return err
}

func parseDate(line string) (time.Time, error) {
	// #Fields: date time x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes time-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type cs-protocol-version fle-status fle-encrypted-fields
	// 2014-05-23 01:13:11 FRA2 182 192.0.2.10 GET d111111abcdef8.cloudfront.net /view/my/file.html 200 www.displaymyfiles.com Mozilla/4.0%20(compatible;%20MSIE%205.0b1;%20Mac_PowerPC) - zip=98101 RefreshHit MRVMF7KydIvxMWfJIglgwHQwZsbG2IhRJ07sn9AkKUFSHS9EXAMPLE== d111111abcdef8.cloudfront.net http - 0.001 - - - RefreshHit HTTP/1.1 Processed 1

	// Grab the first two components of the line.
	dateParts := strings.SplitN(line, "\t", 3)
	if len(dateParts) < 3 {
		return time.Time{}, errors.New("unable to parse date")
	}
	layout := "2006-01-02 15:04:05"
	return time.Parse(layout, fmt.Sprintf("%s %s", dateParts[0], dateParts[1]))
}

func gunzipWrite(w io.Writer, data []byte) error {
	gr, err := gzip.NewReader(bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	defer gr.Close()

	data, err = ioutil.ReadAll(gr)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}