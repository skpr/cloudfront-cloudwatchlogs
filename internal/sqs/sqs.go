package sqs

type Message struct {
	Records []Record `json:"Records"`
}

type Record struct {
	S3 S3 `json:"s3"`
}

type S3 struct {
	Bucket Bucket `json:"bucket"`
	Object Object `json:"object"`
}

type Bucket struct {
	Name string `json:"name"`
	Arn  string `json:"arn"`
}

type Object struct {
	Key  string `json:"key"`
	Size int    `json:"size"`
	Etag string `json:"eTag"`
}
