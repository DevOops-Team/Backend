package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/DevOops-Team/Backend/internal/model"
)

// rangeBucket answers GetObject like S3 does for "Range: bytes=0-N": 206 with Content-Range, 416 for an empty object.
type rangeBucket struct {
	s3Reader
	objects map[string][]byte
	ranges  []string
}

func (b *rangeBucket) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	b.ranges = append(b.ranges, aws.ToString(in.Range))
	data, ok := b.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	if in.Range == nil {
		return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data)), ContentLength: aws.Int64(int64(len(data)))}, nil
	}
	var start, end int64
	if _, err := fmt.Sscanf(aws.ToString(in.Range), "bytes=%d-%d", &start, &end); err != nil {
		return nil, err
	}
	if start >= int64(len(data)) {
		return nil, &smithy.OperationError{ServiceID: "S3", OperationName: "GetObject", Err: &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{Response: &smithyhttp.Response{Response: &http.Response{StatusCode: 416}},
				Err: &smithy.GenericAPIError{Code: "InvalidRange", Message: "The requested range is not satisfiable"}}}}
	}
	end = min(end, int64(len(data))-1)
	part := data[start : end+1]
	return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(part)), ContentLength: aws.Int64(int64(len(part))),
		ContentRange: aws.String(fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))}, nil
}

func TestGetAsksS3OnlyForTheFirstLimitBytesAndReportsTheWholeSize(t *testing.T) {
	b := &rangeBucket{objects: map[string][]byte{"big": bytes.Repeat([]byte("x"), 1000), "small": []byte("abc")}}
	o := &Objects{client: b, bucket: "bucket"}

	data, size, err := o.Get(t.Context(), "big", 8)
	if err != nil || len(data) != 8 || size != 1000 {
		t.Fatalf("Get(big, 8) = %d bytes, size %d, %v; want 8 bytes, size 1000", len(data), size, err)
	}
	data, size, err = o.Get(t.Context(), "small", 100)
	if err != nil || string(data) != "abc" || size != 3 {
		t.Fatalf("Get(small, 100) = %q, size %d, %v; want abc, size 3", data, size, err)
	}
	if want := []string{"bytes=0-7", "bytes=0-99"}; fmt.Sprint(b.ranges) != fmt.Sprint(want) {
		t.Errorf("ranges = %v, want %v", b.ranges, want)
	}
}

func TestGetOfAnEmptyObjectIsEmptyNotAnError(t *testing.T) {
	o := &Objects{client: &rangeBucket{objects: map[string][]byte{"empty": {}}}, bucket: "bucket"}
	data, size, err := o.Get(t.Context(), "empty", 1)
	if err != nil || len(data) != 0 || size != 0 {
		t.Fatalf("Get(empty) = %q, size %d, %v; want empty, size 0, nil", data, size, err)
	}
}

func TestGetOfAMissingKeyIsNotFound(t *testing.T) {
	o := &Objects{client: &rangeBucket{objects: map[string][]byte{}}, bucket: "bucket"}
	if _, _, err := o.Get(t.Context(), "nope", 1); !errors.Is(err, model.ErrNotFound) {
		t.Fatalf("Get(nope) err = %v, want ErrNotFound", err)
	}
}
