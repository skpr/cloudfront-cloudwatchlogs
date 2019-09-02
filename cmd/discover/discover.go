package discover

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudfront"
	"github.com/pkg/errors"
	d "github.com/skpr/cloudfront-cloudwatchlogs/internal/discovery"
	"gopkg.in/alecthomas/kingpin.v2"
)

type cmdDiscover struct {
	// LogGroup defines the tag to use to specify the logGroup.
	TagNameLogGroup string
	// LogStream defines the tag to use to specify the logStream.
	TagNameLogStream string
}

type response struct {
	ID   string            `json:"id"`
	Tags map[string]string `json:"tags"`
}

func (cmd *cmdDiscover) run(c *kingpin.ParseContext) error {
	config := &aws.Config{}
	sess, err := session.NewSession(config)
	if err != nil {
		return errors.Wrap(err, "unable to initialise aws session")
	}

	clientCloudfront := cloudfront.New(sess)
	distributions, err := d.GetDistributionsWithTags(clientCloudfront, []string{cmd.TagNameLogGroup, cmd.TagNameLogStream})
	if err != nil {
		return err
	}

	responses := []response{}
	for _, dist := range distributions {
		r := response{
			ID:   *dist.DistributionSummary.Id,
			Tags: make(map[string]string, 0),
		}
		for _, tag := range dist.Tags.Items {
			r.Tags[*tag.Key] = *tag.Value
		}
		responses = append(responses, r)
	}

	data, err := json.MarshalIndent(responses, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(data))

	return nil
}

// Cmd declares the "version" sub command.
func Cmd(app *kingpin.Application) {
	cmd := new(cmdDiscover)
	c := app.Command("discover", "Discovers distributions with required tags and required resources").Action(cmd.run)
	c.Flag("tag-group", "Tag name on cloudfront distribution to use for log group").
		Default(d.DefaultTagNameLogGroup).
		StringVar(&cmd.TagNameLogGroup)
	c.Flag("tag-stream", "Tag name on cloudfront distribution to use for log stream").
		Default(d.DefaultTagNameLogStream).
		StringVar(&cmd.TagNameLogStream)
}
