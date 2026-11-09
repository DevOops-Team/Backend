package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/settings"
)

const testManifest = `{"dataset_id":"ds-test","version":1,"name":"Tiny Shakespeare","format":"text-jsonl","files":[{"name":"train-000.jsonl","size_bytes":11,"sha256":"aa"}]}`

// --- fakes -----------------------------------------------------------------

type fakeObjects struct {
	puts   map[string]string
	err    error
	putErr error
}

func (f *fakeObjects) Put(_ context.Context, key string, body io.Reader) error {
	if f.err != nil {
		return f.err
	}
	if f.putErr != nil {
		return f.putErr
	}
	b, _ := io.ReadAll(body)
	f.puts[key] = string(b)
	return nil
}

func (f *fakeObjects) List(_ context.Context, prefix string) ([]model.ObjectInfo, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []model.ObjectInfo
	for k, v := range f.puts {
		if strings.HasPrefix(k, prefix) {
			out = append(out, model.ObjectInfo{Key: k, Size: int64(len(v))})
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Key < out[b].Key })
	return out, nil
}

func (f *fakeObjects) Get(_ context.Context, key string, limit int64) ([]byte, int64, error) {
	if f.err != nil {
		return nil, 0, f.err
	}
	v, ok := f.puts[key]
	if !ok {
		return nil, 0, model.ErrNotFound
	}
	b := []byte(v)
	if int64(len(b)) > limit {
		b = b[:limit]
	}
	return b, int64(len(v)), nil
}

type fakeScheduler struct {
	submitted []*unstructured.Unstructured
	jobs      []model.JobStatus
	err       error
}

func (f *fakeScheduler) Submit(_ context.Context, js *unstructured.Unstructured) error {
	if f.err != nil {
		return f.err
	}
	f.submitted = append(f.submitted, js)
	return nil
}

func (f *fakeScheduler) List(context.Context) ([]model.JobStatus, error) { return f.jobs, f.err }

type fakeProgress struct {
	items map[string]model.Progress
	err   error
}

func (f *fakeProgress) Get(_ context.Context, ids []string) (map[string]model.Progress, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]model.Progress{}
	for _, id := range ids {
		if p, ok := f.items[id]; ok {
			out[id] = p
		}
	}
	return out, nil
}

type fakeEvents struct {
	puts  []model.JobEvent
	items map[string][]model.JobEvent
	err   error
}

func (f *fakeEvents) Put(_ context.Context, e model.JobEvent) error {
	if f.err != nil {
		return f.err
	}
	f.puts = append(f.puts, e)
	return nil
}

func (f *fakeEvents) List(_ context.Context, id string) ([]model.JobEvent, error) {
	return f.items[id], f.err
}

type deps struct {
	objects  *fakeObjects
	sched    *fakeScheduler
	progress *fakeProgress
	events   *fakeEvents
}

// newDeps has what the seed script uploads: train.py, the gpt2 files and one dataset.
func newDeps() *deps {
	return &deps{
		&fakeObjects{puts: map[string]string{
			"code/_shared/train.py":                    "print('train')",
			"datasets/_shared/models/gpt2/config.json": `{"model_type":"gpt2"}`,
			"datasets/ds-test/v1/manifest.json":        testManifest,
			"datasets/ds-test/v1/train-000.jsonl":      `{"text":"a"}`,
		}},
		&fakeScheduler{}, &fakeProgress{}, &fakeEvents{},
	}
}

func (d *deps) service(t *testing.T) *JobService {
	t.Helper()
	s, err := settings.Load("")
	if err != nil {
		t.Fatal(err)
	}
	return NewJobService(Config{
		Namespace:     "training",
		Region:        "us-east-2",
		Bucket:        "devoops-ml",
		ProgressTable: "devoops-job-progress",
		Settings:      s,
		NewID:         func() string { return "3f9a1c2e" },
		Now:           func() time.Time { return time.Date(2026, 11, 5, 2, 12, 39, 0, time.UTC) },
	}, d.objects, d.sched, d.progress, d.events, d.events)
}

// defaultRequest is what the frontend sends when the user changes nothing.
func defaultRequest() model.SubmitJobRequest {
	return model.SubmitJobRequest{
		Model: "gpt2", Strategy: "fsdp", GPUs: 2, Dataset: &model.DatasetRef{ID: "ds-test", Version: 1},
	}
}

// requireError는 err가 kind·code를 가진 *Error인지 확인하고, 추가 검사를 위해 돌려준다.
func requireError(t *testing.T, err error, kind Kind, code string) *Error {
	t.Helper()
	var se *Error
	if !errors.As(err, &se) || se.Kind != kind || se.ErrorCode() != code {
		t.Fatalf("err = %#v, want kind %v code %s", err, kind, code)
	}
	return se
}

// wantError는 반환값이 필요 없을 때 쓴다. (*Error가 error를 구현해 errcheck가 버린 반환값을 잡기 때문)
func wantError(t *testing.T, err error, kind Kind, code string) {
	t.Helper()
	_ = requireError(t, err, kind, code)
}

// --- Submit ----------------------------------------------------------------

func TestSubmitWritesConfigWithEveryKeyFromProfileDefaults(t *testing.T) {
	d := newDeps()

	created, err := d.service(t).Submit(context.Background(), defaultRequest())
	if err != nil {
		t.Fatal(err)
	}

	var got map[string]any
	if err := json.Unmarshal([]byte(d.objects.puts["code/3f9a1c2e/config.json"]), &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"schema_version": 1.0, "model": "gpt2", "training_mode": "fsdp", "checkpoint_format": "fsdp-dcp",
		"epochs": 10.0, "max_steps": nil, "batch_size_per_gpu": 4.0, "learning_rate": 3e-05,
		"warmup_steps": 100.0, "checkpoint_every_steps": 250.0, "seed": 42.0, "world_size": 2.0,
	}
	for k, v := range want {
		if gotV, ok := got[k]; !ok || gotV != v {
			t.Errorf("config[%s] = %v, want %v", k, gotV, v)
		}
	}
	if len(got) != len(want)+1 { // + dataset; train.py rejects missing and unknown keys
		t.Errorf("config keys = %v", got)
	}
	ds := got["dataset"].(map[string]any)
	sum := sha256.Sum256([]byte(testManifest))
	if ds["uri"] != "s3://devoops-ml/datasets/ds-test/v1/" || ds["format"] != "text-jsonl" || ds["manifest_sha256"] != hex.EncodeToString(sum[:]) {
		t.Errorf("config.dataset = %v", ds)
	}
	if string(created.Config) != d.objects.puts["code/3f9a1c2e/config.json"] {
		t.Error("response config differs from the stored config.json")
	}
}

func TestSubmitHashesTheExactConfigBytes(t *testing.T) {
	d := newDeps()

	created, err := d.service(t).Submit(context.Background(), defaultRequest())
	if err != nil {
		t.Fatal(err)
	}

	sum := sha256.Sum256([]byte(d.objects.puts["code/3f9a1c2e/config.json"]))
	want := hex.EncodeToString(sum[:])
	if created.ConfigSHA256 != want {
		t.Errorf("configSha256 = %s, want %s", created.ConfigSHA256, want)
	}
	if got := d.sched.submitted[0].GetAnnotations()[model.ConfigSHA256Annotation]; got != want {
		t.Errorf("annotation = %s, want %s", got, want)
	}
}

func TestSubmitCopiesSharedTrainScriptNextToConfig(t *testing.T) {
	d := newDeps()

	if _, err := d.service(t).Submit(context.Background(), defaultRequest()); err != nil {
		t.Fatal(err)
	}

	if d.objects.puts["code/3f9a1c2e/train.py"] != "print('train')" {
		t.Errorf("train.py = %q", d.objects.puts["code/3f9a1c2e/train.py"])
	}
}

func TestSubmitStoresTheJobSetItSubmits(t *testing.T) {
	d := newDeps()

	created, err := d.service(t).Submit(context.Background(), defaultRequest())
	if err != nil {
		t.Fatal(err)
	}

	if len(d.sched.submitted) != 1 || d.sched.submitted[0].GetName() != "job-3f9a1c2e-g0001" {
		t.Fatalf("submitted = %v", d.sched.submitted)
	}
	want, _ := json.Marshal(d.sched.submitted[0].Object)
	var stored any
	if err := json.Unmarshal([]byte(d.objects.puts["code/3f9a1c2e/jobset.json"]), &stored); err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(stored)
	var wantNorm any
	_ = json.Unmarshal(want, &wantNorm)
	wantBytes, _ := json.Marshal(wantNorm)
	if string(got) != string(wantBytes) {
		t.Errorf("jobset.json differs from the submitted object")
	}
	if created.JobsetName != "job-3f9a1c2e-g0001" || created.Generation != 1 || created.Status != model.StatusQueued ||
		created.Queue != "queue-auto" || created.ID != "3f9a1c2e" {
		t.Errorf("created = %+v", created)
	}
}

func TestSubmitDefaultsNameAndStrategy(t *testing.T) {
	d := newDeps()
	req := defaultRequest()
	req.Strategy = ""

	created, err := d.service(t).Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	if created.Name != "gpt2-3f9a1c2e" {
		t.Errorf("name = %q, want {model}-{id}", created.Name)
	}
	if d.sched.submitted[0].GetAnnotations()[model.StrategyAnnotation] != "fsdp" {
		t.Errorf("annotations = %v", d.sched.submitted[0].GetAnnotations())
	}
}

func TestSubmitOverlaysHyperparametersTheRequestChanged(t *testing.T) {
	d := newDeps()
	req := defaultRequest()
	req.Hyperparameters = map[string]any{"epochs": 3.0, "learning_rate": 0.001}

	created, err := d.service(t).Submit(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	var cfg model.TrainConfig
	if err := json.Unmarshal(created.Config, &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Epochs != 3 || cfg.LearningRate != 0.001 || cfg.BatchSizePerGPU != 4 {
		t.Errorf("config = %+v", cfg)
	}
	if got := d.sched.submitted[0].GetAnnotations()[model.HyperparametersAnnotation]; !strings.Contains(got, `"epochs":3`) {
		t.Errorf("hyperparameters annotation = %s", got)
	}
}

func TestSubmitRunsDDPWithItsOwnCheckpointFormat(t *testing.T) {
	for _, gpus := range []int{1, 2} {
		d := newDeps()
		req := defaultRequest()
		req.Strategy, req.GPUs = "ddp", gpus

		created, err := d.service(t).Submit(context.Background(), req)
		if err != nil {
			t.Fatalf("gpus %d: %v", gpus, err)
		}

		var cfg model.TrainConfig
		if err := json.Unmarshal(created.Config, &cfg); err != nil {
			t.Fatal(err)
		}
		if cfg.TrainingMode != "ddp" || cfg.CheckpointFormat != "ddp-pt" || cfg.WorldSize != gpus {
			t.Errorf("config = %+v", cfg)
		}
		if d.sched.submitted[0].GetAnnotations()[model.StrategyAnnotation] != "ddp" {
			t.Errorf("annotations = %v", d.sched.submitted[0].GetAnnotations())
		}
	}
}

func TestSubmitExplainsThatFourGPUsExceedCurrentQuota(t *testing.T) {
	d := newDeps()
	req := defaultRequest()
	req.GPUs = 4

	_, err := d.service(t).Submit(context.Background(), req)

	se := requireError(t, err, KindInvalid, "VALIDATION")
	if !strings.Contains(se.Fields[0].Message, "2") {
		t.Errorf("message = %q, want the current maximum", se.Fields[0].Message)
	}
}

func TestSubmitRejectsBadInputBeforeWritingAnything(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*model.SubmitJobRequest)
		field string
	}{
		{"missing model", func(r *model.SubmitJobRequest) { r.Model = "" }, "model"},
		{"unknown model", func(r *model.SubmitJobRequest) { r.Model = "gpt5" }, "model"},
		{"unknown strategy", func(r *model.SubmitJobRequest) { r.Strategy = "horovod" }, "strategy"},
		{"gpu count not offered", func(r *model.SubmitJobRequest) { r.GPUs = 3 }, "gpus"},
		{"fsdp on one gpu", func(r *model.SubmitJobRequest) { r.GPUs = 1 }, "gpus"},
		{"missing gpus", func(r *model.SubmitJobRequest) { r.GPUs = 0 }, "gpus"},
		{"too many gpus", func(r *model.SubmitJobRequest) { r.GPUs = 4 }, "gpus"},
		{"missing dataset", func(r *model.SubmitJobRequest) { r.Dataset = nil }, "dataset"},
		{"bad name", func(r *model.SubmitJobRequest) { r.Name = "Has Spaces" }, "name"},
		{"unknown hyperparameter", func(r *model.SubmitJobRequest) { r.Hyperparameters = map[string]any{"seed": 1.0} }, "hyperparameters.seed"},
		{"fractional integer", func(r *model.SubmitJobRequest) { r.Hyperparameters = map[string]any{"epochs": 1.5} }, "hyperparameters.epochs"},
		{"wrong type", func(r *model.SubmitJobRequest) { r.Hyperparameters = map[string]any{"epochs": "3"} }, "hyperparameters.epochs"},
		{"above max", func(r *model.SubmitJobRequest) { r.Hyperparameters = map[string]any{"epochs": 21.0} }, "hyperparameters.epochs"},
		{"at exclusive min", func(r *model.SubmitJobRequest) { r.Hyperparameters = map[string]any{"learning_rate": 0.0} }, "hyperparameters.learning_rate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newDeps()
			before := len(d.objects.puts)
			req := defaultRequest()
			tc.edit(&req)

			_, err := d.service(t).Submit(context.Background(), req)

			se := requireError(t, err, KindInvalid, "VALIDATION")
			if len(se.Fields) != 1 || se.Fields[0].Field != tc.field {
				t.Errorf("fields = %+v, want %s", se.Fields, tc.field)
			}
			if len(d.objects.puts) != before || len(d.sched.submitted) != 0 {
				t.Error("wrote or submitted something for a rejected request")
			}
		})
	}
}

func TestSubmitReportsUnknownDatasetVersionAsNotFound(t *testing.T) {
	d := newDeps()
	req := defaultRequest()
	req.Dataset.Version = 9

	_, err := d.service(t).Submit(context.Background(), req)

	wantError(t, err, KindNotFound, "NOT_FOUND")
}

func TestSubmitRejectsDatasetFormatTheModelDoesNotRead(t *testing.T) {
	d := newDeps()
	d.objects.puts["datasets/ds-test/v1/manifest.json"] = `{"dataset_id":"ds-test","version":1,"format":"csv-numeric","files":[]}`

	_, err := d.service(t).Submit(context.Background(), defaultRequest())

	wantError(t, err, KindUnprocessable, "DATASET_FORMAT_MISMATCH")
}

func TestSubmitExplainsMissingTrainScript(t *testing.T) {
	d := newDeps()
	delete(d.objects.puts, "code/_shared/train.py")

	_, err := d.service(t).Submit(context.Background(), defaultRequest())

	se := requireError(t, err, KindNotFound, "NOT_FOUND")
	if !strings.Contains(se.Msg, "code/_shared/train.py") {
		t.Errorf("msg = %q, want the missing key", se.Msg)
	}
	if len(d.sched.submitted) != 0 {
		t.Error("submitted without train.py")
	}
}

func TestSubmitDoesNotSubmitWhenUploadFails(t *testing.T) {
	d := newDeps()
	d.objects.putErr = errors.New("s3 down")

	_, err := d.service(t).Submit(context.Background(), defaultRequest())

	wantError(t, err, KindUpstream, "UPSTREAM")
	if len(d.sched.submitted) != 0 {
		t.Error("submitted after a failed upload")
	}
}

func TestSubmitReportsAPIServerFailure(t *testing.T) {
	d := newDeps()
	d.sched.err = errors.New("k3s unreachable")

	_, err := d.service(t).Submit(context.Background(), defaultRequest())

	wantError(t, err, KindUpstream, "SUBMIT_FAILED")
}

func TestSubmitReportsDuplicateJobSetAsConflict(t *testing.T) {
	d := newDeps()
	d.sched.err = model.ErrAlreadyExists

	_, err := d.service(t).Submit(context.Background(), defaultRequest())

	wantError(t, err, KindConflict, "ALREADY_EXISTS")
}

func TestSubmitRecordsSubmittedAsFirstTimelineStep(t *testing.T) {
	d := newDeps()

	if _, err := d.service(t).Submit(context.Background(), defaultRequest()); err != nil {
		t.Fatal(err)
	}

	if len(d.events.puts) != 1 {
		t.Fatalf("events = %+v", d.events.puts)
	}
	e := d.events.puts[0]
	if e.JobID != "3f9a1c2e" || e.Reason != "Submitted" || e.Key != "2026-11-05T02:12:39.000Z#k3s#submitted" ||
		!strings.Contains(e.Message, "queue-auto") || e.Object != "JobSet/job-3f9a1c2e-g0001" {
		t.Errorf("event = %+v", e)
	}
}

func TestSubmitStillSucceedsWhenTimelineWriteFails(t *testing.T) {
	d := newDeps()
	d.events.err = errors.New("dynamodb throttled")

	if _, err := d.service(t).Submit(context.Background(), defaultRequest()); err != nil {
		t.Errorf("err = %v, the job is already queued", err)
	}
}

// --- Event source ----------------------------------------------------------

// Reading and writing are separate so the read side can move to Kafka on its own.
func TestEventsReadFromReaderAndSubmitWritesToWriter(t *testing.T) {
	d := newDeps()
	reader := &fakeEvents{items: map[string][]model.JobEvent{"abc123": {{JobID: "abc123", Source: "kafka"}}}}
	writer := &fakeEvents{}
	s, err := settings.Load("")
	if err != nil {
		t.Fatal(err)
	}
	svc := NewJobService(Config{Bucket: "b", Settings: s, NewID: func() string { return "abc123" }}, d.objects, d.sched, d.progress, reader, writer)

	if _, err := svc.Submit(context.Background(), defaultRequest()); err != nil {
		t.Fatal(err)
	}
	got, err := svc.Events(context.Background(), "abc123")

	if err != nil || len(got) != 1 || got[0].Source != "kafka" {
		t.Errorf("events = %+v err = %v", got, err)
	}
	if len(writer.puts) != 1 || len(reader.puts) != 0 {
		t.Errorf("writer got %d, reader got %d; Submitted must go to the writer", len(writer.puts), len(reader.puts))
	}
}

// --- Models ----------------------------------------------------------------

func TestModelsDescribeProfileAndItsFilesInS3(t *testing.T) {
	models, err := newDeps().service(t).Models(context.Background())
	if err != nil || len(models) != 1 {
		t.Fatalf("models = %+v err = %v", models, err)
	}
	m := models[0]
	if m.StrategyMinGPUs["fsdp"] != 2 || m.StrategyMinGPUs["ddp"] != 1 || len(m.GPUs.Options) != 3 {
		t.Errorf("gpu rules = %+v %+v", m.GPUs, m.StrategyMinGPUs)
	}
	if m.Name != "gpt2" || m.DefaultStrategy != "fsdp" || m.GPUs.Default != 2 || m.URI != "s3://devoops-ml/datasets/_shared/models/gpt2/" {
		t.Errorf("model = %+v", m)
	}
	if len(m.Files) != 1 || m.Files[0].Name != "config.json" {
		t.Errorf("files = %+v", m.Files)
	}
	var epochs *model.HyperparameterDef
	for i := range m.Hyperparameters {
		if m.Hyperparameters[i].Key == "epochs" {
			epochs = &m.Hyperparameters[i]
		}
	}
	if epochs == nil || epochs.Default != int64(10) || epochs.Level != "basic" {
		t.Errorf("epochs = %+v", epochs)
	}
}

func TestModelFilePreviewStaysInsideModelDirectory(t *testing.T) {
	s := newDeps().service(t)

	got, err := s.ModelFile(context.Background(), "gpt2", "config.json")
	if err != nil || !strings.Contains(string(got.Data), "gpt2") {
		t.Errorf("got %q err = %v", got.Data, err)
	}
	_, err = s.ModelFile(context.Background(), "gpt2", "../../../code/_shared/train.py")
	wantError(t, err, KindInvalid, "VALIDATION")
	_, err = s.ModelFile(context.Background(), "nope", "config.json")
	wantError(t, err, KindNotFound, "NOT_FOUND")
}

// --- Datasets --------------------------------------------------------------

func TestDatasetsListsSeededVersionsFromManifests(t *testing.T) {
	d := newDeps()
	d.objects.puts["datasets/ds-test/v2/manifest.json"] = strings.ReplaceAll(testManifest, `"version":1`, `"version":2`)

	got, err := d.service(t).Datasets(context.Background())

	if err != nil || len(got) != 1 {
		t.Fatalf("datasets = %+v err = %v", got, err)
	}
	if got[0].ID != "ds-test" || got[0].Name != "Tiny Shakespeare" || got[0].Format != "text-jsonl" || got[0].LatestVersion != 2 || got[0].Versions != nil {
		t.Errorf("dataset = %+v", got[0])
	}
}

func TestDatasetDetailHasVersionsAndFiles(t *testing.T) {
	got, err := newDeps().service(t).Dataset(context.Background(), "ds-test")
	if err != nil || len(got.Versions) != 1 {
		t.Fatalf("dataset = %+v err = %v", got, err)
	}
	v := got.Versions[0]
	if v.Version != 1 || v.FileCount != 1 || v.TotalBytes != 11 || v.ManifestSHA256 == "" || v.Source != "seed" {
		t.Errorf("version = %+v", v)
	}
	if len(v.Files) != 1 || v.Files[0].Name != "train-000.jsonl" {
		t.Errorf("files = %+v", v.Files)
	}
}

func TestDatasetReportsUnknownIDAsNotFound(t *testing.T) {
	_, err := newDeps().service(t).Dataset(context.Background(), "ds-missing")

	wantError(t, err, KindNotFound, "NOT_FOUND")
}

func TestDatasetFilePreviewRejectsPathsOutsideTheVersion(t *testing.T) {
	s := newDeps().service(t)

	got, err := s.DatasetFile(context.Background(), "ds-test", 1, "train-000.jsonl")
	if err != nil || string(got.Data) != `{"text":"a"}` {
		t.Errorf("got %q err = %v", got.Data, err)
	}
	_, err = s.DatasetFile(context.Background(), "../code", 1, "x")
	wantError(t, err, KindInvalid, "VALIDATION")
	_, err = s.DatasetFile(context.Background(), "ds-test", 1, "../../x")
	wantError(t, err, KindInvalid, "VALIDATION")
}

// --- List / Get ------------------------------------------------------------

func TestListMergesQueueStateWithProgress(t *testing.T) {
	d := newDeps()
	updated := time.Unix(300, 0).UTC()
	d.sched.jobs = []model.JobStatus{{ID: "abc123", Phase: model.PhaseRunning, Status: model.StatusRunning, Cluster: "devoops-a", CreatedAt: time.Unix(100, 0)}}
	d.progress.items = map[string]model.Progress{"abc123": {Step: 1500, TotalSteps: 5000, LastCheckpointStep: 1000, UpdatedAt: updated}}

	jobs, err := d.service(t).List(context.Background())

	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs = %+v err = %v", jobs, err)
	}
	if jobs[0].Progress == nil || jobs[0].Progress.Step != 1500 || !jobs[0].UpdatedAt.Equal(updated) {
		t.Errorf("job = %+v", jobs[0])
	}
}

func TestListWithoutProgressUsesCreationTimeAsUpdatedAt(t *testing.T) {
	d := newDeps()
	created := time.Unix(100, 0).UTC()
	d.sched.jobs = []model.JobStatus{{ID: "new1", CreatedAt: created}}

	jobs, _ := d.service(t).List(context.Background())

	if len(jobs) != 1 || jobs[0].Progress != nil || !jobs[0].UpdatedAt.Equal(created) {
		t.Errorf("jobs = %+v", jobs)
	}
}

func TestListReturnsNewestFirst(t *testing.T) {
	d := newDeps()
	d.sched.jobs = []model.JobStatus{{ID: "old", CreatedAt: time.Unix(100, 0)}, {ID: "new", CreatedAt: time.Unix(200, 0)}}

	jobs, _ := d.service(t).List(context.Background())

	if jobs[0].ID != "new" || jobs[1].ID != "old" {
		t.Errorf("order = [%s %s]", jobs[0].ID, jobs[1].ID)
	}
}

func TestListStillReturnsJobsWhenProgressStoreFails(t *testing.T) {
	d := newDeps()
	d.sched.jobs = []model.JobStatus{{ID: "abc123"}}
	d.progress.err = errors.New("dynamodb throttled")

	jobs, err := d.service(t).List(context.Background())

	if err != nil || len(jobs) != 1 || jobs[0].Progress != nil {
		t.Errorf("jobs = %+v err = %v", jobs, err)
	}
}

func TestListReturnsEmptySliceNotNil(t *testing.T) {
	jobs, err := newDeps().service(t).List(context.Background())

	if err != nil || jobs == nil {
		t.Errorf("jobs = %#v err = %v", jobs, err)
	}
}

func TestListReportsSchedulerFailureAsUpstream(t *testing.T) {
	d := newDeps()
	d.sched.err = errors.New("k3s unreachable")

	_, err := d.service(t).List(context.Background())

	wantError(t, err, KindUpstream, "UPSTREAM")
}

func TestGetAddsConfigAndImages(t *testing.T) {
	d := newDeps()
	d.sched.jobs = []model.JobStatus{{ID: "abc123"}, {ID: "other"}}
	d.objects.puts["code/abc123/config.json"] = `{"epochs":3}`

	j, err := d.service(t).Get(context.Background(), "abc123")

	if err != nil || j.ID != "abc123" || string(j.Config) != `{"epochs":3}` {
		t.Errorf("job = %+v err = %v", j, err)
	}
	if j.Images["trainer"] == "" || j.Images["progressReporter"] == "" {
		t.Errorf("images = %v", j.Images)
	}
}

func TestGetStillReturnsJobWhenConfigCannotBeRead(t *testing.T) {
	d := newDeps()
	d.sched.jobs = []model.JobStatus{{ID: "abc123"}}

	j, err := d.service(t).Get(context.Background(), "abc123")

	if err != nil || j.ID != "abc123" || j.Config != nil {
		t.Errorf("job = %+v err = %v", j, err)
	}
}

func TestGetReportsUnknownIDAsNotFound(t *testing.T) {
	_, err := newDeps().service(t).Get(context.Background(), "missing")

	wantError(t, err, KindNotFound, "NOT_FOUND")
}

// --- Events ----------------------------------------------------------------

func TestEventsReturnsTimeline(t *testing.T) {
	d := newDeps()
	d.events.items = map[string][]model.JobEvent{"abc123": {{JobID: "abc123", Source: "backend"}, {JobID: "abc123", Source: "karpenter"}}}

	got, err := d.service(t).Events(context.Background(), "abc123")

	if err != nil || len(got) != 2 || got[1].Source != "karpenter" {
		t.Errorf("got %+v err = %v", got, err)
	}
}

func TestEventsReturnsEmptySliceNotNil(t *testing.T) {
	got, err := newDeps().service(t).Events(context.Background(), "abc123")

	if err != nil || got == nil {
		t.Errorf("got %#v err = %v", got, err)
	}
}

func TestEventsRejectsInvalidID(t *testing.T) {
	_, err := newDeps().service(t).Events(context.Background(), "../x")

	wantError(t, err, KindInvalid, "VALIDATION")
}

// --- Job files -------------------------------------------------------------

func storedJob() *deps {
	d := newDeps()
	d.objects.puts["code/abc123/config.json"] = `{"epochs":3}`
	d.objects.puts["code/abc123/train.py"] = "print('hi')"
	d.objects.puts["code/other/train.py"] = "not this job"
	d.sched.jobs = []model.JobStatus{{ID: "abc123", Dataset: &model.DatasetRef{ID: "ds-test", Version: 1}}}
	return d
}

func TestFilesListsCodeOfOneJobThenItsDatasetManifest(t *testing.T) {
	files, err := storedJob().service(t).Files(context.Background(), "abc123")

	want := []model.File{
		{Kind: "code", Name: "config.json", Size: 12},
		{Kind: "code", Name: "train.py", Size: 11},
		{Kind: "data", Name: "manifest.json", Size: int64(len(testManifest))},
	}
	if err != nil || len(files) != len(want) {
		t.Fatalf("files = %+v err = %v, want %+v", files, err, want)
	}
	for i := range want {
		if files[i] != want[i] {
			t.Errorf("files[%d] = %+v, want %+v", i, files[i], want[i])
		}
	}
}

func TestFilesRejectsInvalidID(t *testing.T) {
	_, err := storedJob().service(t).Files(context.Background(), "../other")

	wantError(t, err, KindInvalid, "VALIDATION")
}

func TestFileReturnsConfigAndDatasetManifest(t *testing.T) {
	s := storedJob().service(t)

	code, err := s.File(context.Background(), "abc123", "code", "config.json")
	if err != nil || string(code.Data) != `{"epochs":3}` || code.Truncated {
		t.Errorf("code = %+v err = %v", code, err)
	}
	data, err := s.File(context.Background(), "abc123", "data", "manifest.json")
	if err != nil || string(data.Data) != testManifest {
		t.Errorf("data = %+v err = %v", data, err)
	}
}

func TestFileTruncatesLargeFiles(t *testing.T) {
	d := storedJob()
	d.objects.puts["code/abc123/big.json"] = strings.Repeat("x", MaxPreviewBytes+10)

	got, err := d.service(t).File(context.Background(), "abc123", "code", "big.json")

	if err != nil || len(got.Data) != MaxPreviewBytes || !got.Truncated || got.TotalSize != MaxPreviewBytes+10 {
		t.Errorf("len = %d truncated = %v total = %d err = %v", len(got.Data), got.Truncated, got.TotalSize, err)
	}
}

func TestFileRejectsOtherKindsAndEscapingNames(t *testing.T) {
	s := storedJob().service(t)
	for _, tc := range [][3]string{
		{"abc123", "ckpt", "x"},
		{"abc123", "code", "../other/train.py"},
		{"abc123", "code", ""},
		{"../other", "code", "train.py"},
		{"abc123", "data", "train-000.jsonl"},
	} {
		_, err := s.File(context.Background(), tc[0], tc[1], tc[2])
		wantError(t, err, KindInvalid, "VALIDATION")
	}
}

func TestFileReportsMissingFileAsNotFound(t *testing.T) {
	_, err := storedJob().service(t).File(context.Background(), "abc123", "code", "nope.json")

	wantError(t, err, KindNotFound, "NOT_FOUND")
}
