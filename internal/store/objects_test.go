package store

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/DevOops-Team/Backend/internal/api"
)

type fakeS3 struct {
	objects map[string]string
	pages   int
}

func (f *fakeS3) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	// One object per page so pagination is exercised.
	f.pages++
	var keys []string
	for k := range f.objects {
		if strings.HasPrefix(k, aws.ToString(in.Prefix)) && k > aws.ToString(in.StartAfter) && (in.ContinuationToken == nil || k > *in.ContinuationToken) {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return &s3.ListObjectsV2Output{}, nil
	}
	first := keys[0]
	for _, k := range keys {
		if k < first {
			first = k
		}
	}
	out := &s3.ListObjectsV2Output{Contents: []types.Object{{Key: aws.String(first), Size: aws.Int64(int64(len(f.objects[first])))}}}
	if len(keys) > 1 {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = aws.String(first)
	}
	return out, nil
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	v, ok := f.objects[aws.ToString(in.Key)]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(v)), ContentLength: aws.Int64(int64(len(v)))}, nil
}

func TestObjectsListFollowsPagination(t *testing.T) {
	client := &fakeS3{objects: map[string]string{
		"code/abc/job.yaml": "kind: JobSet", "code/abc/train.py": "print()", "code/other/train.py": "x",
	}}
	o := &Objects{client: client, bucket: "b"}

	got, err := o.List(context.Background(), "code/abc/")
	if err != nil {
		t.Fatal(err)
	}

	want := []api.ObjectInfo{{Key: "code/abc/job.yaml", Size: 12}, {Key: "code/abc/train.py", Size: 7}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("List = %+v, want %+v", got, want)
	}
}

func TestObjectsGetReadsAtMostLimitAndReportsFullSize(t *testing.T) {
	o := &Objects{client: &fakeS3{objects: map[string]string{"datasets/abc/d.csv": "0123456789"}}, bucket: "b"}

	data, size, err := o.Get(context.Background(), "datasets/abc/d.csv", 4)
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != "0123" || size != 10 {
		t.Errorf("Get = %q size %d, want \"0123\" size 10", data, size)
	}
}

func TestObjectsGetMapsMissingKeyToNotFound(t *testing.T) {
	o := &Objects{client: &fakeS3{objects: map[string]string{}}, bucket: "b"}

	_, _, err := o.Get(context.Background(), "code/abc/missing", 4)

	if err != api.ErrNotFound {
		t.Errorf("err = %v, want api.ErrNotFound", err)
	}
}
