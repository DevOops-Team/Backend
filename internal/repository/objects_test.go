package repository

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Signing is local (no request is sent), so this checks the real SDK output offline.
func TestPresignSavesUnderTheFileNameAndExpires(t *testing.T) {
	client := s3.New(s3.Options{
		Region:      "us-east-2",
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", "secret", "")),
	})
	raw, err := NewObjects(client, "bucket").Presign(t.Context(), "jobs/0cb3b0c3/gen-0002/step-00000694/__0_0.distcp", "__0_0.distcp", 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if !strings.HasSuffix(u.Path, "/jobs/0cb3b0c3/gen-0002/step-00000694/__0_0.distcp") || q.Get("X-Amz-Expires") != "900" ||
		q.Get("response-content-disposition") != "attachment; filename=__0_0.distcp" || q.Get("X-Amz-Signature") == "" {
		t.Errorf("presigned URL = %s", raw)
	}
}
