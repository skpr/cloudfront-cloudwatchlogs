package cloudwatchlogs

import (
	"context"
	"github.com/pkg/errors"
	"strings"

	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/prometheus/common/log"
)

type Writer struct {
	Client *cloudwatchlogs.CloudWatchLogs
	Logger log.Logger
	LogGroup *string
	LogStream *string
	SequenceToken *string
}

func(w *Writer) Stream(ctx context.Context, messages []*cloudwatchlogs.InputLogEvent) error {
	// 1. If you don't have a valid token or don't have a token at all (you're just starting) describe the stream to find out the token
	// 2. Push using the token you've got. If the push is successful update the token
	// 3. If the push is not successful go to 1), get a new token and try again. You may need to try multiple times (ie loop) if multiple producers.
	refreshSequenceToken := false

	logIn := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  w.LogGroup,
		LogStreamName: w.LogStream,
		LogEvents: messages,
	}
	for remainingRetries := 3; remainingRetries > 0; remainingRetries-- {
		if logIn.SequenceToken == nil || refreshSequenceToken {
			w.Logger.Debug("updating cloudwatchlogs sequence token")
			sequenceToken, err:= w.UpdateSequenceToken(ctx)
			if err != nil {
				errmsg := "could not update sequence token"
				w.Logger.Error(errmsg, ": ", err.Error())
				return errors.Wrap(err, errmsg)
			}
			logIn.SequenceToken = sequenceToken
		}

		if len(logIn.LogEvents) < 1 {
			// No lines to push - abort
			w.Logger.Debug("no lines to push")
			return nil
		}

		_, err := w.Client.PutLogEventsWithContext(ctx, logIn)
		if err != nil {
			if strings.Contains(err.Error(), "InvalidSequenceTokenException") {
				// Invalid token, so re-run the loop.
				if remainingRetries == 0 {
					return errors.New("exceeded retry limit when updating sequence token")
				}
				refreshSequenceToken = true
				continue
			}

			errmsg := "could not push log events to cloudwatch"
			w.Logger.Error(errmsg, " ", err.Error())
			return errors.Wrap(err, errmsg)
		}
		w.Logger.Infof("pushed %d lines", len(logIn.LogEvents))
		break
	}

	return nil
}

// EnsureLogGroup
func(w *Writer) EnsureLogGroup(ctx context.Context) error {
	in := &cloudwatchlogs.CreateLogGroupInput{
		LogGroupName:  w.LogGroup,
	}

	_, err := w.Client.CreateLogGroupWithContext(ctx, in)
	if err != nil && !strings.Contains(err.Error(), cloudwatchlogs.ErrCodeResourceAlreadyExistsException) {
		return err
	}

	return nil
}

// EnsureLogStream
func(w *Writer) EnsureLogStream(ctx context.Context) error {
	err := w.EnsureLogGroup(ctx)
	if err != nil {
		return err
	}

	in := &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  w.LogGroup,
		LogStreamName: w.LogStream,
	}

	_, err = w.Client.CreateLogStreamWithContext(ctx, in)
	if err != nil && !strings.Contains(err.Error(), cloudwatchlogs.ErrCodeResourceAlreadyExistsException) {
		return err
	}

	return nil
}

func(w *Writer) UpdateSequenceToken(ctx context.Context) (*string, error) {
	describeIn := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        w.LogGroup,
		LogStreamNamePrefix: w.LogStream,
	}
	describeRet, err := w.Client.DescribeLogStreamsWithContext(ctx, describeIn)
	if err != nil {
		// Couldn't fetch the next token.
		w.Logger.Error(err.Error())
		return nil, err
	}

	return describeRet.LogStreams[0].UploadSequenceToken, nil
}