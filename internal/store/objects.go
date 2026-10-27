package store

import (
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/DevOops-Team/Backend/internal/api"
)

// s3Reader is the subset of *s3.Client used for reading back job inputs.
type s3Reader interface {
	s3.ListObjectsV2APIClient
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// Objects stores job inputs in S3. The uploader streams large datasets as multipart uploads.
type Objects struct {
	uploader *manager.Uploader
	client   s3Reader
	bucket   string
}

func NewObjects(client *s3.Client, bucket string) *Objects {
	return &Objects{uploader: manager.NewUploader(client), client: client, bucket: bucket}
}

func (o *Objects) Put(ctx context.Context, key string, body io.Reader) error {
	_, err := o.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

func (o *Objects) List(ctx context.Context, prefix string) ([]api.ObjectInfo, error) {
	var out []api.ObjectInfo
	pages := s3.NewListObjectsV2Paginator(o.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(o.bucket),
		Prefix: aws.String(prefix),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			out = append(out, api.ObjectInfo{Key: aws.ToString(obj.Key), Size: aws.ToInt64(obj.Size)})
		}
	}
	return out, nil
}

// Get reads at most limit bytes and closes the body early, so previewing a large dataset
// does not download all of it.
func (o *Objects) Get(ctx context.Context, key string, limit int64) ([]byte, int64, error) {
	res, err := o.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(o.bucket), Key: aws.String(key)})
	var missing *types.NoSuchKey
	if errors.As(err, &missing) {
		return nil, 0, api.ErrNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, limit))
	if err != nil {
		return nil, 0, err
	}
	return data, aws.ToInt64(res.ContentLength), nil
}
