# CloudFront to CloudWatch Logs

Service to synchronise CloudFront logs to CloudWatch.

## Configuration

This tool requires the following cloud resources configured.

* One or more **CloudFront distributions**
    * Must have logging enabled (see notes on s3 bucket below) 
    * Must have tags indicating the log group and log stream. Default tag names are
        * `edge.skpr.io/loggroup` (configurable with `--tag-group` flag)
        * `edge.skpr.io/logstream` (configurable with `--tag-stream` flag)
* A **S3 bucket**
    * Must have event notifications configure to create a SQS message on `s3:ObjectCreated:*` events.
* A **SQS queue**
* **IAM credentials** with permissions to
    * List/Get cloudfront distributions
    * Get objects from log s3 bucket
    * Receive/delete SQS messages

## Usage

```bash
# Ensure AWS credential chain is configured with environment variables.
cloudfront-cloudwatchlogs watch --region=ap-southeast-2
```