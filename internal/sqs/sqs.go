package sqs

// Message encapsulates the message.
type Message struct {
	Records []Record `json:"Records"`
}

// Record encapsulates the record.
type Record struct {
	S3 S3 `json:"s3"`
}

// S3 encapsulates the s3 metadata.
type S3 struct {
	Bucket Bucket `json:"bucket"`
	Object Object `json:"object"`
}

// Bucket encapsulates the bucket details.
type Bucket struct {
	Name string `json:"name"`
	Arn  string `json:"arn"`
}

// Object encapsulates the Object details.
type Object struct {
	Key  string `json:"key"`
	Size int    `json:"size"`
	Etag string `json:"eTag"`
}
