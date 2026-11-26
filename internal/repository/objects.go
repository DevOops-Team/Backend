package repository

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/DevOops-Team/Backend/internal/model"
)

// s3Reader is the subset of *s3.Client used for reading back job inputs and deleting a job's objects.
type s3Reader interface {
	s3.ListObjectsV2APIClient
	s3.ListObjectVersionsAPIClient
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, opts ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	s3.ListPartsAPIClient
	DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, opts ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// Objects stores job inputs in S3. The uploader streams large datasets as multipart uploads.
type Objects struct {
	uploader  *manager.Uploader
	client    s3Reader
	presigner *s3.PresignClient
	bucket    string
}

func NewObjects(client *s3.Client, bucket string) *Objects {
	return &Objects{uploader: manager.NewUploader(client), client: client, presigner: s3.NewPresignClient(client), bucket: bucket}
}

func (o *Objects) Put(ctx context.Context, key string, body io.Reader) error {
	_, err := o.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	return err
}

func (o *Objects) List(ctx context.Context, prefix string) ([]model.ObjectInfo, error) {
	var out []model.ObjectInfo
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
			out = append(out, model.ObjectInfo{Key: aws.ToString(obj.Key), Size: aws.ToInt64(obj.Size)})
		}
	}
	return out, nil
}

// Get reads at most limit bytes and the object's whole size. It asks S3 for only those bytes (Range: bytes=0-limit-1),
// so previewing a large dataset or reading a safetensors header does not pull the rest of the file over the network,
// and the connection is reused because the body is read to its end.
func (o *Objects) Get(ctx context.Context, key string, limit int64) ([]byte, int64, error) {
	limit = max(limit, 1)
	res, err := o.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(o.bucket), Key: aws.String(key),
		Range: aws.String(fmt.Sprintf("bytes=0-%d", limit-1))})
	var missing *types.NoSuchKey
	if errors.As(err, &missing) {
		return nil, 0, model.ErrNotFound
	}
	if invalidRange(err) {
		return []byte{}, 0, nil // S3 answers 416 to a range of an empty object
	}
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, limit))
	if err != nil {
		return nil, 0, err
	}
	// Reaches EOF so the connection goes back to the pool; bounded in case a server ignored the Range.
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 4096))
	return data, objectSize(res), nil
}

// objectSize is the whole object's size: the total of Content-Range ("bytes 0-7/1234") for a partial answer,
// Content-Length when the server sent the whole object.
func objectSize(res *s3.GetObjectOutput) int64 {
	if cr := aws.ToString(res.ContentRange); cr != "" {
		if i := strings.LastIndexByte(cr, '/'); i >= 0 {
			if n, err := strconv.ParseInt(cr[i+1:], 10, 64); err == nil {
				return n
			}
		}
	}
	return aws.ToInt64(res.ContentLength)
}

func invalidRange(err error) bool {
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "InvalidRange" {
		return true
	}
	var re *awshttp.ResponseError
	return errors.As(err, &re) && re.HTTPStatusCode() == http.StatusRequestedRangeNotSatisfiable
}

// Presign signs a GET for key that the browser downloads straight from S3, saved as filename. The URL also
// stops working when the backend's Roles Anywhere session (at most 1 hour) expires, so callers sign at
// request time with a short ttl. S3 checks the signature when the download starts, not while it runs.
func (o *Objects) Presign(ctx context.Context, key, filename string, ttl time.Duration) (string, error) {
	req, err := o.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket:                     aws.String(o.bucket),
		Key:                        aws.String(key),
		ResponseContentDisposition: aws.String(mime.FormatMediaType("attachment", map[string]string{"filename": filename})),
		ResponseContentType:        aws.String("application/octet-stream"),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

// PresignPut signs a PUT the browser sends straight to S3 (GPU-278). Content-Length and x-amz-checksum-sha256
// are signed headers: S3 refuses another size, and refuses a body whose SHA-256 is not the given one. Like
// Presign, the URL also stops working when the backend's Roles Anywhere session expires; S3 checks the
// signature when the upload starts.
func (o *Objects) PresignPut(ctx context.Context, key string, size int64, sha256Hex string, ttl time.Duration) (string, map[string]string, error) {
	b64, err := hexToBase64(sha256Hex)
	if err != nil {
		return "", nil, err
	}
	req, err := o.presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:         aws.String(o.bucket),
		Key:            aws.String(key),
		ContentLength:  aws.Int64(size),
		ChecksumSHA256: aws.String(b64),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", nil, err
	}
	// The browser sets Host and Content-Length itself; it only has to send the checksum header.
	return req.URL, map[string]string{"x-amz-checksum-sha256": b64}, nil
}

// Head reads the size and the full-object SHA-256 checksum (single PUT with x-amz-checksum-sha256).
func (o *Objects) Head(ctx context.Context, key string) (model.ObjectHead, error) {
	res, err := o.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(o.bucket), Key: aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled})
	var missing *types.NotFound
	if errors.As(err, &missing) {
		return model.ObjectHead{}, model.ErrNotFound
	}
	var api smithy.APIError
	if errors.As(err, &api) && api.ErrorCode() == "NotFound" {
		return model.ObjectHead{}, model.ErrNotFound
	}
	if err != nil {
		return model.ObjectHead{}, err
	}
	h := model.ObjectHead{Size: aws.ToInt64(res.ContentLength)}
	if sum, err := base64.StdEncoding.DecodeString(aws.ToString(res.ChecksumSHA256)); err == nil && len(sum) == 32 {
		h.SHA256 = hex.EncodeToString(sum)
	}
	return h, nil
}

// CreateMultipart starts a multipart upload whose parts each carry a SHA-256 checksum (composite checksum).
func (o *Objects) CreateMultipart(ctx context.Context, key string) (string, error) {
	res, err := o.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(o.bucket), Key: aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256})
	if err != nil {
		return "", err
	}
	return aws.ToString(res.UploadId), nil
}

// PresignUploadPart signs one part PUT with its size and SHA-256 (like PresignPut).
func (o *Objects) PresignUploadPart(ctx context.Context, key, uploadID string, number int32, size int64, sha256Hex string, ttl time.Duration) (string, map[string]string, error) {
	b64, err := hexToBase64(sha256Hex)
	if err != nil {
		return "", nil, err
	}
	req, err := o.presigner.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(o.bucket), Key: aws.String(key), UploadId: aws.String(uploadID), PartNumber: aws.Int32(number),
		ContentLength: aws.Int64(size), ChecksumSHA256: aws.String(b64),
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return "", nil, err
	}
	return req.URL, map[string]string{"x-amz-checksum-sha256": b64}, nil
}

func (o *Objects) ListParts(ctx context.Context, key, uploadID string) ([]model.UploadedPart, error) {
	var out []model.UploadedPart
	pages := s3.NewListPartsPaginator(o.client, &s3.ListPartsInput{Bucket: aws.String(o.bucket), Key: aws.String(key), UploadId: aws.String(uploadID)})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		var missing *types.NoSuchUpload
		if errors.As(err, &missing) {
			return nil, model.ErrNotFound
		}
		if err != nil {
			return nil, err
		}
		for _, p := range page.Parts {
			part := model.UploadedPart{Number: aws.ToInt32(p.PartNumber), ETag: aws.ToString(p.ETag), Size: aws.ToInt64(p.Size)}
			if sum, err := base64.StdEncoding.DecodeString(aws.ToString(p.ChecksumSHA256)); err == nil && len(sum) == 32 {
				part.SHA256 = hex.EncodeToString(sum)
			}
			out = append(out, part)
		}
	}
	return out, nil
}

func (o *Objects) CompleteMultipart(ctx context.Context, key, uploadID string, parts []model.UploadedPart) error {
	done := make([]types.CompletedPart, 0, len(parts))
	for _, p := range parts {
		b64, err := hexToBase64(p.SHA256)
		if err != nil {
			return err
		}
		done = append(done, types.CompletedPart{PartNumber: aws.Int32(p.Number), ETag: aws.String(p.ETag), ChecksumSHA256: aws.String(b64)})
	}
	_, err := o.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(o.bucket), Key: aws.String(key),
		UploadId: aws.String(uploadID), MultipartUpload: &types.CompletedMultipartUpload{Parts: done}})
	return err
}

func hexToBase64(sha256Hex string) (string, error) {
	sum, err := hex.DecodeString(sha256Hex)
	if err != nil || len(sum) != 32 {
		return "", fmt.Errorf("sha256 must be 64 hex characters")
	}
	return base64.StdEncoding.EncodeToString(sum), nil
}

// deleteBatch is the most keys one DeleteObjects call takes.
const deleteBatch = 1000

// DeleteAll removes every version and delete marker under prefix, so the data is gone for good even though
// the bucket is versioned. Each page of the version listing (at most 1000) is one DeleteObjects call.
func (o *Objects) DeleteAll(ctx context.Context, prefix string) error {
	pages := s3.NewListObjectVersionsPaginator(o.client, &s3.ListObjectVersionsInput{
		Bucket:  aws.String(o.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(deleteBatch),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list versions: %w", err)
		}
		ids := make([]types.ObjectIdentifier, 0, len(page.Versions)+len(page.DeleteMarkers))
		for _, v := range page.Versions {
			ids = append(ids, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, m := range page.DeleteMarkers {
			ids = append(ids, types.ObjectIdentifier{Key: m.Key, VersionId: m.VersionId})
		}
		for start := 0; start < len(ids); start += deleteBatch {
			batch := ids[start:min(start+deleteBatch, len(ids))]
			res, err := o.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(o.bucket),
				Delete: &types.Delete{Objects: batch, Quiet: aws.Bool(true)},
			})
			if err != nil {
				return fmt.Errorf("delete objects: %w", err)
			}
			if len(res.Errors) > 0 {
				e := res.Errors[0]
				return fmt.Errorf("delete %s (%d failed): %s %s", aws.ToString(e.Key), len(res.Errors), aws.ToString(e.Code), aws.ToString(e.Message))
			}
		}
	}
	return nil
}
