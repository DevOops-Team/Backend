package service

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

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
func (f fakeObjects) Open(_ context.Context, key string) (io.ReadCloser, int64, error) {
	v, ok := f[key]
	if !ok {
		return nil, 0, model.ErrNotFound
	}
	return io.NopCloser(strings.NewReader(v)), int64(len(v)), nil
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

func TestModelFileStreamsOnlyFilesInTheManifest(t *testing.T) {
	s := modelService(model.StatusSucceeded, finished(), bucket())
	f, err := s.ModelFile(t.Context(), modelJob, "__0_0.distcp")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Body.Close()
	body, _ := io.ReadAll(f.Body)
	if string(body) != "shard0" || f.Size != 6 || f.Name != "__0_0.distcp" {
		t.Errorf("file = %q size %d name %q", body, f.Size, f.Name)
	}
	for _, name := range []string{"manifest.json", "../config.json", "missing.distcp"} {
		if _, err := s.ModelFile(t.Context(), modelJob, name); kindOf(err) != KindNotFound && kindOf(err) != KindInvalid {
			t.Errorf("%s: err = %v, want not found or invalid", name, err)
		}
	}
}

func TestModelArchiveZipsEveryFileWithTheManifest(t *testing.T) {
	a, err := modelService(model.StatusSucceeded, finished(), bucket()).ModelArchive(t.Context(), modelJob)
	if err != nil {
		t.Fatal(err)
	}
	if a.Name != "qwen-0cb3b0c3-step-694.zip" {
		t.Errorf("name = %q", a.Name)
	}
	var buf bytes.Buffer
	if err := a.Write(&buf); err != nil {
		t.Fatal(err)
	}
	zr, err := zip.NewReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, f := range zr.File {
		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(rc)
		rc.Close()
		got[f.Name] = string(b)
	}
	want := map[string]string{
		"qwen-0cb3b0c3-step-694/manifest.json": modelManif,
		"qwen-0cb3b0c3-step-694/.metadata":     "meta",
		"qwen-0cb3b0c3-step-694/__0_0.distcp":  "shard0",
	}
	if len(got) != len(want) {
		t.Fatalf("entries = %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}
