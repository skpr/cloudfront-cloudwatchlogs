package main

import (
	"bytes"
	"context"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs/cloudwatchlogsiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
)

// HandleEvents sent from AWS S3.
func HandleEvents(ctx context.Context, event events.S3Event) error {
	sess, err := session.NewSession()
	if err != nil {
		return fmt.Errorf("failed to setup client: %d", err)
	}
	for _, record := range event.Records {
		fmt.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, record.S3.Bucket.Name, record.S3.Object.Key)
		err := handleEvent(ctx, s3.New(sess), cloudwatchlogs.New(sess), record)
		if err != nil {
			return err
		}
	}
	return nil
}
// Helper function to handle a single S3 event.
func handleEvent(ctx context.Context, s3client s3iface.S3API, cwclient cloudwatchlogsiface.CloudWatchLogsAPI, record events.S3EventRecord) error {
	// Validate the record.
	// Extract metadata.
	// Extract file contents.
	downloadIn := &s3.GetObjectInput{
		Bucket: aws.String(record.S3.Bucket.Name),
		Key:    aws.String(record.S3.Object.Key),
	}
	downloader := s3manager.NewDownloaderWithClient(s3client)
	buffRaw := &aws.WriteAtBuffer{}
	_, err := downloader.DownloadWithContext(ctx, buffRaw, downloadIn)
	if err != nil {
		errmsg := "unable to download log file"
		// in.Logger.Error(errmsg)
		return errors.Wrap(err, errmsg)
	}
	// Loop and push to CloudWatch Logs.
	contents, err := ioutil.ReadFile("./E2IZT1FG9IZCS6.2020-06-12-00.1937b23d.gz")
	if err != nil {
		panic(err)
	}
	messages, err := parseMessages(contents)
	if err != nil {
		panic(err)
	}

	// Print all messages.
	for _, m := range messages {
		fmt.Println(m)
	}

	return nil
}

func main() {
	lambda.Start(HandleEvents)
}

// parseMessages from a gzip file contents.
func parseMessages(contents []byte) ([]string, error) {
	buff := bytes.Buffer{}
	err := gunzipWrite(&buff, contents)
	if err != nil {
		return []string{}, err
	}

	// Split the file by newline.
	split := []byte("\n")
	lines := bytes.Split(buff.Bytes(), split)
	messages := make([]string, 0)

	// Loop over the lines and parse out the timestamp and message.
	for _, line := range lines {
		if len(line) < 1 {
			// Nothing in this line - probably just a newline.
			continue
		}
		message := string(line)
		if strings.HasPrefix(string(message), "#") {
			// Comment - ignore.
			continue
		}
		// Parse date out of cloudfront access log line.
		date, message, err := parseDateAndMessage(message)
		if err != nil {
			// Couldn't parse date, default to now.
			date = time.Now()
		}
		messages = append(messages, fmt.Sprintf("Date: %d Message: %s", aws.TimeUnixMilli(date), message))
	}

	return messages, nil
}

// gunzipWrite a gzip file to a buffer.
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

// parseDateAndMessage from a cloudfront log string.
func parseDateAndMessage(line string) (time.Time, string, error) {
	// #Fields: date date x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes date-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type cs-protocol-version fle-status fle-encrypted-fields
	// 2014-05-23 01:13:11 FRA2 182 192.0.2.10 GET d111111abcdef8.cloudfront.net /view/my/file.html 200 www.displaymyfiles.com Mozilla/4.0%20(compatible;%20MSIE%205.0b1;%20Mac_PowerPC) - zip=98101 RefreshHit MRVMF7KydIvxMWfJIglgwHQwZsbG2IhRJ07sn9AkKUFSHS9EXAMPLE== d111111abcdef8.cloudfront.net http - 0.001 - - - RefreshHit HTTP/1.1 Processed 1

	// Grab the first two components of the line.
	sep := "\t"
	lineParts := strings.SplitN(line, sep, 3)
	if len(lineParts) < 3 {
		return time.Time{}, "", errors.New("unable to parse date")
	}
	layout := "2006-01-02 15:04:05"
	date, err := time.Parse(layout, fmt.Sprintf("%s %s", lineParts[0], lineParts[1]))

	// Join the rest of the message back together.
	message := strings.Join(lineParts[2:], sep)

	return date, message, err
}
