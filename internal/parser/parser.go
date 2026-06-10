package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type JsonLog struct {
	Date string `json:"date"`
	Time string `json:"time"`
}

// ParseDateAndMessage parses the date and message from a log string.
func ParseDateAndMessage(line string) (time.Time, string, error) {
	if line[0:1] == "{" {
		return ParseDateAndMessageJson(line)
	}
	return ParseDateAndMessageLegacy(line)
}

// ParseDateAndMessageLegacy from a cloudfront log string.
func ParseDateAndMessageLegacy(line string) (time.Time, string, error) {
	// #Fields: date date x-edge-location sc-bytes c-ip cs-method cs(Host) cs-uri-stem sc-status cs(Referer) cs(User-Agent) cs-uri-query cs(Cookie) x-edge-result-type x-edge-request-id x-host-header cs-protocol cs-bytes date-taken x-forwarded-for ssl-protocol ssl-cipher x-edge-response-result-type cs-protocol-version fle-status fle-encrypted-fields
	// 2014-05-23 01:13:11 FRA2 182 192.0.2.10 GET d111111abcdef8.cloudfront.net /view/my/file.html 200 www.displaymyfiles.com Mozilla/4.0%20(compatible;%20MSIE%205.0b1;%20Mac_PowerPC) - zip=98101 RefreshHit MRVMF7KydIvxMWfJIglgwHQwZsbG2IhRJ07sn9AkKUFSHS9EXAMPLE== d111111abcdef8.cloudfront.net http - 0.001 - - - RefreshHit HTTP/1.1 Processed 1

	// Grab the first two components of the line.
	sep := "\t"
	lineParts := strings.SplitN(line, sep, 3)
	if len(lineParts) < 3 {
		return time.Time{}, "", errors.New("unable to parse date")
	}
	date, err := parseDateTime(lineParts[0], lineParts[1])
	if err != nil {
		return time.Time{}, "", err
	}

	// Join the rest of the message back together.
	message := strings.Join(lineParts[2:], sep)

	return date, message, nil
}

// ParseDateAndMessageJson from a cloudfront v2 log string.
func ParseDateAndMessageJson(line string) (time.Time, string, error) {
	logDetails := &JsonLog{}
	err := json.Unmarshal([]byte(line), logDetails)
	if err != nil {
		return time.Time{}, "", err
	}

	date, err := parseDateTime(logDetails.Date, logDetails.Time)
	if err != nil {
		return time.Time{}, "", err
	}

	message := line
	return date, message, nil
}

// GetLogGroupName from the s3 object key.
func GetLogGroupName(key string) string {
	sep := "/"

	// Split the key up by slash.
	keyParts := strings.Split(key, sep)

	// LogGroup is the whole key excluding the filename.
	logGroup := strings.Join(keyParts[:len(keyParts)-1], sep)

	// Ensure the logGroup is prefixed with a slash.
	if !strings.HasPrefix(logGroup, "/") {
		logGroup = fmt.Sprintf("/%s", logGroup)
	}

	return logGroup
}

func parseDateTime(inputDate, inputTime string) (time.Time, error) {
	return time.Parse("2006-01-02 15:04:05", fmt.Sprintf("%s %s", inputDate, inputTime))
}
