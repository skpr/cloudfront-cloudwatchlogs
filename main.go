package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

func main() {
	contents, err := ioutil.ReadFile("./E2IZT1FG9IZCS6.2020-06-12-00.1937b23d.gz")
	if err != nil {
		panic(err)
	}
	buff := bytes.Buffer{}
	err = gunzipWrite(&buff, contents)
	if err != nil {
		panic(err)
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
		// @todo replace with hasPrefix in streams package.
		if string(line[0]) == "#" {
			// Comment - ignore.
			continue
		}
		message := string(line)
		// Parse date out of cloudfront access log line.
		date, message, err := parseDateAndMessage(message)
		if err != nil {
			// Couldn't parse date, default to now.
			date = time.Now()
		}
		messages = append(messages, fmt.Sprintf("Date: %d Message: %s", aws.TimeUnixMilli(date), message))
	}

	// Print all messages.
	for _, m := range messages {
		fmt.Println(m)
	}
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