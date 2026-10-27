package store

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Objects uploads job inputs to S3. The uploader streams large datasets as multipart uploads.
type Objects struct {
	uploader *manager.Uploader
	bucket   string
}

func NewObjects(client *s3.Client, bucket string) *Objects {
	return &Objects{uploader: manager.NewUploader(client), bucket: bucket}
}

func (o *Objects) Put(ctx context.Context, key string, body io.Reader) error {
	_, err := o.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}
