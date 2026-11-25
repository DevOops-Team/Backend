package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

// fakeObjects is an in-memory bucket.
type fakeObjects map[string]string

func (f fakeObjects) Put(context.Context, string, io.Reader) error { return nil }
func (f fakeObjects) List(context.Context, string) ([]model.ObjectInfo, error) {
	return nil, nil
}
func (f fakeObjects) Get(_ context.Context, key string, limit int64) ([]byte, int64, error) {
	v, ok := f[key]
	if !ok {
		return nil, 0, model.ErrNotFound
	}
	return []byte(v[:min(int64(len(v)), limit)]), int64(len(v)), nil
}

func (f fakeObjects) DeleteAll(context.Context, string) error { return nil }

// PresignPut returns a fake URL; the checksum header carries the hex sum so tests can see it.
func (f fakeObjects) PresignPut(_ context.Context, key string, size int64, sha string, ttl time.Duration) (string, map[string]string, error) {
	return fmt.Sprintf("https://s3.example/%s?put&size=%d&ttl=%s", key, size, ttl), map[string]string{"x-amz-checksum-sha256": sha}, nil
}

// Multipart in the in-memory bucket: "mpu/<upload id>" holds the key, "mpu/<upload id>/<n>" a part.
func (f fakeObjects) CreateMultipart(_ context.Context, key string) (string, error) {
	id := fmt.Sprintf("up%d", len(f))
	f["mpu/"+id] = key
	return id, nil
}

func (f fakeObjects) PresignUploadPart(_ context.Context, key, uploadID string, number int32, size int64, sha string, _ time.Duration) (string, map[string]string, error) {
	return fmt.Sprintf("https://s3.example/%s?uploadId=%s&partNumber=%d&size=%d", key, uploadID, number, size), map[string]string{"x-amz-checksum-sha256": sha}, nil
}

func (f fakeObjects) ListParts(_ context.Context, key, uploadID string) ([]model.UploadedPart, error) {
	if f["mpu/"+uploadID] != key {
		return nil, model.ErrNotFound
	}
	var parts []model.UploadedPart
	for n := int32(1); ; n++ {
		v, ok := f[fmt.Sprintf("mpu/%s/%d", uploadID, n)]
		if !ok {
			break
		}
		sum := sha256.Sum256([]byte(v))
		parts = append(parts, model.UploadedPart{Number: n, ETag: fmt.Sprintf("etag%d", n), Size: int64(len(v)), SHA256: hex.EncodeToString(sum[:])})
	}
	return parts, nil
}

func (f fakeObjects) CompleteMultipart(_ context.Context, key, uploadID string, parts []model.UploadedPart) error {
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(f[fmt.Sprintf("mpu/%s/%d", uploadID, p.Number)])
		delete(f, fmt.Sprintf("mpu/%s/%d", uploadID, p.Number))
	}
	delete(f, "mpu/"+uploadID)
	f[key] = b.String()
	return nil
}

// Head reports an object's size and, as its checksum, the SHA-256 of what is stored (what S3 would check).
func (f fakeObjects) Head(_ context.Context, key string) (model.ObjectHead, error) {
	v, ok := f[key]
	if !ok {
		return model.ObjectHead{}, model.ErrNotFound
	}
	sum := sha256.Sum256([]byte(v))
	return model.ObjectHead{Size: int64(len(v)), SHA256: hex.EncodeToString(sum[:])}, nil
}

// Presign returns a fake URL that records what was signed.
func (f fakeObjects) Presign(_ context.Context, key, filename string, ttl time.Duration) (string, error) {
	return "https://s3.example/" + key + "?filename=" + filename + "&ttl=" + ttl.String(), nil
}

const (
	modelJob    = "0cb3b0c3"
	modelPrefix = "jobs/0cb3b0c3/gen-0002/step-00000694/"
	modelManif  = `{"job_id":"0cb3b0c3","generation":2,"step":694,"format":"fsdp-dcp","world_size":2,
		"files":[{"name":".metadata","size_bytes":4,"sha256":"aa"},{"name":"__0_0.distcp","size_bytes":6,"sha256":"bb"}]}`
)

func modelService(status model.Status, rec fakeRecords, objs fakeObjects) *JobService {
	sched := fakeScheduler{[]model.JobStatus{{ID: modelJob, Name: "qwen-0cb3b0c3", Generation: 2, Status: status, CreatedAt: t0}}}
	return NewJobService(Config{}, Deps{Scheduler: sched, Records: rec, Objects: objs, Events: fakeEvents{}})
}

func finished() fakeRecords {
	return fakeRecords{final: &model.FinalCheckpoint{Generation: 2, Step: 694, Format: "fsdp-dcp", WorldSize: 2,
		ManifestKey: modelPrefix + "manifest.json", SizeBytes: 10, ApprovedAt: t0}}
}

func bucket() fakeObjects {
	return fakeObjects{
		modelPrefix + "manifest.json": modelManif,
		modelPrefix + ".metadata":     "meta",
		modelPrefix + "__0_0.distcp":  "shard0",
	}
}

func kindOf(err error) Kind {
	var se *Error
	if errors.As(err, &se) {
		return se.Kind
	}
	return 0
}

func TestModelListsTheFinalCheckpointOfAFinishedJob(t *testing.T) {
	got, err := modelService(model.StatusSucceeded, finished(), bucket()).Model(t.Context(), modelJob)
	if err != nil {
		t.Fatal(err)
	}
	if got.JobID != modelJob || got.Generation != 2 || got.Step != 694 || got.Format != "fsdp-dcp" || got.WorldSize != 2 ||
		got.SizeBytes != 10 || !got.ApprovedAt.Equal(t0) {
		t.Errorf("model = %+v", got)
	}
	if len(got.Files) != 2 || got.Files[0] != (model.ModelFile{Name: ".metadata", Size: 4, SHA256: "aa"}) {
		t.Errorf("files = %+v", got.Files)
	}
}

func TestModelRefusesJobsThatHaveNotFinished(t *testing.T) {
	for _, st := range []model.Status{model.StatusRunning, model.StatusRecovering, model.StatusFailed} {
		_, err := modelService(st, finished(), bucket()).Model(t.Context(), modelJob)
		if kindOf(err) != KindConflict {
			t.Errorf("%s: err = %v, want conflict", st, err)
		}
	}
}

func TestModelNotFoundCases(t *testing.T) {
	cases := map[string]*JobService{
		"unknown job":     modelService(model.StatusSucceeded, finished(), bucket()),
		"no checkpoint":   modelService(model.StatusSucceeded, fakeRecords{}, bucket()),
		"manifest absent": modelService(model.StatusSucceeded, finished(), fakeObjects{}),
	}
	for name, s := range cases {
		id := modelJob
		if name == "unknown job" {
			id = "ffffffff"
		}
		if _, err := s.Model(t.Context(), id); kindOf(err) != KindNotFound {
			t.Errorf("%s: err = %v, want not found", name, err)
		}
	}
}

// A manifest key outside the job's own prefix is never followed.
func TestModelRejectsAManifestOutsideTheJob(t *testing.T) {
	rec := finished()
	rec.final.ManifestKey = "jobs/other/gen-0001/step-00000001/manifest.json"
	if _, err := modelService(model.StatusSucceeded, rec, bucket()).Model(t.Context(), modelJob); kindOf(err) != KindUpstream {
		t.Errorf("err = %v, want upstream", err)
	}
}

func TestModelFileURLSignsOnlyFilesInTheManifest(t *testing.T) {
	s := modelService(model.StatusSucceeded, finished(), bucket())
	url, err := s.ModelFileURL(t.Context(), modelJob, "__0_0.distcp")
	if err != nil {
		t.Fatal(err)
	}
	if want := "https://s3.example/" + modelPrefix + "__0_0.distcp?filename=__0_0.distcp&ttl=15m0s"; url != want {
		t.Errorf("url = %q, want %q", url, want)
	}
	for _, name := range []string{"manifest.json", "../config.json", "missing.distcp"} {
		if _, err := s.ModelFileURL(t.Context(), modelJob, name); kindOf(err) != KindNotFound && kindOf(err) != KindInvalid {
			t.Errorf("%s: err = %v, want not found or invalid", name, err)
		}
	}
}

func TestModelFileURLRefusesUnfinishedJobs(t *testing.T) {
	_, err := modelService(model.StatusRunning, finished(), bucket()).ModelFileURL(t.Context(), modelJob, "__0_0.distcp")
	if kindOf(err) != KindConflict {
		t.Errorf("err = %v, want conflict", err)
	}
}
