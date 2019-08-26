package discovery

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/cloudfront"
	"github.com/aws/aws-sdk-go/service/s3"
	"strings"
)

// GetDistributionLogBucket finds the bucket used to store logs for a cloudfront distribution.
func GetDistributionLogBucket(client *cloudfront.CloudFront, DistributionID *string) (*string, error) {
	// Look up configured s3 bucket for logs.
	distConfig, err := client.GetDistributionConfig(&cloudfront.GetDistributionConfigInput{
		Id: DistributionID,
	})
	if err != nil {
		return nil, err
	}
	// The bucket name comes in like bucket-name.s3.amazonaws.com so strip off the suffix.
	bucket := strings.Replace(*distConfig.DistributionConfig.Logging.Bucket, ".s3.amazonaws.com", "", 1)
	return &bucket, nil
}

// GetBucketNotificationQueue finds the SQS queue used for s3 notifications.
func GetBucketNotificationQueue(client *s3.S3, BucketName *string) (*string, error) {
	notificationConfig, err := client.GetBucketNotificationConfiguration(&s3.GetBucketNotificationConfigurationRequest{
		Bucket: BucketName,
	})
	if err != nil {
		return nil, err
	}

	if len(notificationConfig.QueueConfigurations) == 0 {
		return nil, fmt.Errorf("no notification queues configured for %s", *BucketName)
	}

	// Just use the first queue.
	// @todo figure out if there is a more robust way to handle multiple queues.
	return notificationConfig.QueueConfigurations[0].QueueArn, nil
}

type GetDistributionLogQueueInput struct {
	ClientS3         *s3.S3
	ClientCloudfront *cloudfront.CloudFront
	Distribution     *cloudfront.DistributionSummary
}

func GetDistributionLogQueue(in *GetDistributionLogQueueInput) (*string, error) {
	// Look up configured s3 bucket for logs.
	bucket, err := GetDistributionLogBucket(in.ClientCloudfront, in.Distribution.Id)
	if err != nil {
		return nil, err
	}

	// Look up object event notifications for that bucket
	queue, err := GetBucketNotificationQueue(in.ClientS3, bucket)
	if err != nil {
		return nil, err
	}

	return queue, nil
}

type DistributionWithTags struct {
	DistributionSummary *cloudfront.DistributionSummary
	Tags                *cloudfront.Tags
}

// GetDistributionsWithTags returns distributions with any provided tags.
func GetDistributionsWithTags(client *cloudfront.CloudFront, tags []string) ([]DistributionWithTags, error) {
	distributions := make([]DistributionWithTags, 0)
	dists, err := client.ListDistributions(&cloudfront.ListDistributionsInput{})
	if err != nil {
		return distributions, err
	}

	for _, dist := range dists.DistributionList.Items {
		distTags, err := client.ListTagsForResource(&cloudfront.ListTagsForResourceInput{
			Resource: dist.ARN,
		})
		if err != nil {
			continue
		}

		for _, tag := range distTags.Tags.Items {
			if contains(tags, *tag.Key) {
				item := DistributionWithTags{
					DistributionSummary: dist,
					Tags:                distTags.Tags,
				}
				distributions = append(distributions, item)
				break
			}
		}
	}

	return distributions, nil
}

// contains tells whether a contains x.
func contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
