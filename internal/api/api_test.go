package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const validJobSet = `apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: demo
spec:
  replicatedJobs:
  - name: workers
    template:
      spec:
        template:
          spec:
            containers:
            - name: trainer
              image: pytorch/pytorch:latest
`

// --- fakes -----------------------------------------------------------------

type fakeObjects struct {
	puts map[string]string
	err  error
}

func (f *fakeObjects) Put(_ context.Context, key string, body io.Reader) error {
	if f.err != nil {
		return f.err
	}
	b, _ := io.ReadAll(body)
	if f.puts == nil {
		f.puts = map[string]string{}
	}
	f.puts[key] = string(b)
	return nil
}

func (f *fakeObjects) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	if f.err != nil {
		return nil, f.err
	}
	var out []ObjectInfo
	for k, v := range f.puts {
		if strings.HasPrefix(k, prefix) {
			out = append(out, ObjectInfo{Key: k, Size: int64(len(v))})
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
		return nil, 0, ErrNotFound
	}
	b := []byte(v)
	if int64(len(b)) > limit {
		b = b[:limit]
	}
	return b, int64(len(v)), nil
}

type fakeScheduler struct {
	submitted []*unstructured.Unstructured
	jobs      []JobStatus
	err       error
}

func (f *fakeScheduler) Submit(_ context.Context, js *unstructured.Unstructured) error {
	if f.err != nil {
		return f.err
	}
	f.submitted = append(f.submitted, js)
	return nil
}

func (f *fakeScheduler) List(context.Context) ([]JobStatus, error) { return f.jobs, f.err }

type fakeProgress struct {
	items map[string]Progress
	err   error
}

func (f *fakeProgress) Get(_ context.Context, ids []string) (map[string]Progress, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := map[string]Progress{}
	for _, id := range ids {
		if p, ok := f.items[id]; ok {
			out[id] = p
		}
	}
	return out, nil
}

func newTestServer(objects *fakeObjects, sched *fakeScheduler, progress *fakeProgress) http.Handler {
	return NewServer(Config{
		Namespace:     "training",
		QueueName:     "queue-auto",
		ProgressTable: "devoops-job-progress",
		AllowedOrigin: "http://localhost:5173",
		NewID:         func() string { return "abc123" },
	}, objects, sched, progress)
}

type upload struct {
	field, filename, content string
}

func multipartRequest(t *testing.T, files ...upload) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, f := range files {
		part, err := w.CreateFormFile(f.field, f.filename)
		if err != nil {
			t.Fatal(err)
		}
		part.Write([]byte(f.content))
	}
	w.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", &buf)
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req
}

func validUpload(t *testing.T) *http.Request {
	return multipartRequest(t,
		upload{"job", "job.yaml", validJobSet},
		upload{"train", "train.py", "print('hi')"},
		upload{"data", "train.csv", "1,2,3"},
	)
}

// --- POST /api/jobs --------------------------------------------------------

func TestCreateJobUploadsCodeAndDataUnderJobID(t *testing.T) {
	objects := &fakeObjects{}
	srv := newTestServer(objects, &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, validUpload(t))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if objects.puts["code/abc123/train.py"] != "print('hi')" {
		t.Errorf("train.py not uploaded to code/abc123/: %v", objects.puts)
	}
	if objects.puts["datasets/abc123/train.csv"] != "1,2,3" {
		t.Errorf("data not uploaded to datasets/abc123/: %v", objects.puts)
	}
}

func TestCreateJobKeepsSubmittedJobYAMLNextToCode(t *testing.T) {
	objects := &fakeObjects{}
	srv := newTestServer(objects, &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, validUpload(t))

	if objects.puts["code/abc123/job.yaml"] != validJobSet {
		t.Errorf("job.yaml as submitted not stored at code/abc123/job.yaml: %v", objects.puts)
	}
}

func TestCreateJobSubmitsPreparedJobSetToQueue(t *testing.T) {
	sched := &fakeScheduler{}
	srv := newTestServer(&fakeObjects{}, sched, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, validUpload(t))

	if len(sched.submitted) != 1 {
		t.Fatalf("submitted %d JobSets, want 1", len(sched.submitted))
	}
	js := sched.submitted[0]
	if js.GetName() != "job-abc123" || js.GetNamespace() != "training" {
		t.Errorf("submitted %s/%s, want training/job-abc123", js.GetNamespace(), js.GetName())
	}
	if js.GetLabels()["kueue.x-k8s.io/queue-name"] != "queue-auto" {
		t.Errorf("queue label = %q", js.GetLabels()["kueue.x-k8s.io/queue-name"])
	}

	var body map[string]string
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body["id"] != "abc123" || body["name"] != "job-abc123" {
		t.Errorf("response = %v, want id abc123 name job-abc123", body)
	}
}

func TestCreateJobRejectsMissingTrainScript(t *testing.T) {
	objects := &fakeObjects{}
	srv := newTestServer(objects, &fakeScheduler{}, &fakeProgress{})
	req := multipartRequest(t,
		upload{"job", "job.yaml", validJobSet},
		upload{"data", "train.csv", "1,2,3"},
	)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if len(objects.puts) != 0 {
		t.Errorf("uploaded %v before validating input", objects.puts)
	}
}

func TestCreateJobRejectsMissingData(t *testing.T) {
	srv := newTestServer(&fakeObjects{}, &fakeScheduler{}, &fakeProgress{})
	req := multipartRequest(t,
		upload{"job", "job.yaml", validJobSet},
		upload{"train", "train.py", "print('hi')"},
	)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestCreateJobRejectsInvalidJobSetBeforeUploading(t *testing.T) {
	objects := &fakeObjects{}
	srv := newTestServer(objects, &fakeScheduler{}, &fakeProgress{})
	req := multipartRequest(t,
		upload{"job", "job.yaml", "apiVersion: batch/v1\nkind: Job\n"},
		upload{"train", "train.py", "print('hi')"},
		upload{"data", "train.csv", "1,2,3"},
	)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
	if len(objects.puts) != 0 {
		t.Errorf("uploaded %v for an invalid job", objects.puts)
	}
}

func TestCreateJobStripsDirectoriesFromDataFilenames(t *testing.T) {
	objects := &fakeObjects{}
	srv := newTestServer(objects, &fakeScheduler{}, &fakeProgress{})
	req := multipartRequest(t,
		upload{"job", "job.yaml", validJobSet},
		upload{"train", "train.py", "print('hi')"},
		upload{"data", "../../etc/passwd", "x"},
	)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if _, ok := objects.puts["datasets/abc123/passwd"]; !ok {
		t.Errorf("expected key datasets/abc123/passwd, got %v", objects.puts)
	}
}

func TestCreateJobReportsUploadFailureAsBadGateway(t *testing.T) {
	sched := &fakeScheduler{}
	srv := newTestServer(&fakeObjects{err: errors.New("s3 down")}, sched, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, validUpload(t))

	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
	if len(sched.submitted) != 0 {
		t.Error("submitted a job whose inputs failed to upload")
	}
}

func TestCreateJobReportsSubmitFailureAsBadGateway(t *testing.T) {
	srv := newTestServer(&fakeObjects{}, &fakeScheduler{err: errors.New("k3s unreachable")}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, validUpload(t))

	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
}

// --- GET /api/jobs ---------------------------------------------------------

func TestListJobsMergesQueueStateWithProgress(t *testing.T) {
	sched := &fakeScheduler{jobs: []JobStatus{
		{ID: "abc123", Name: "job-abc123", Phase: PhaseRunning, Cluster: "devoops-a", CreatedAt: time.Unix(100, 0)},
	}}
	progress := &fakeProgress{items: map[string]Progress{
		"abc123": {Status: "RUNNING", Step: 1500, TotalSteps: 5000, Loss: 0.42, LastCheckpointStep: 1000, NodeName: "ip-10-0-1-2"},
	}}
	srv := newTestServer(&fakeObjects{}, sched, progress)

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var jobs []Job
	if err := json.Unmarshal(rec.Body.Bytes(), &jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}
	j := jobs[0]
	if j.Cluster != "devoops-a" || j.Progress == nil || j.Progress.Step != 1500 || j.Progress.LastCheckpointStep != 1000 {
		t.Errorf("job = %+v progress = %+v", j, j.Progress)
	}
}

func TestListJobsShowsJobWithoutProgressYet(t *testing.T) {
	sched := &fakeScheduler{jobs: []JobStatus{{ID: "new1", Name: "job-new1", Phase: PhasePending}}}
	srv := newTestServer(&fakeObjects{}, sched, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))

	var jobs []Job
	json.Unmarshal(rec.Body.Bytes(), &jobs)
	if len(jobs) != 1 || jobs[0].Progress != nil {
		t.Errorf("jobs = %+v, want one pending job without progress", jobs)
	}
}

func TestListJobsReturnsNewestFirst(t *testing.T) {
	sched := &fakeScheduler{jobs: []JobStatus{
		{ID: "old", CreatedAt: time.Unix(100, 0)},
		{ID: "new", CreatedAt: time.Unix(200, 0)},
	}}
	srv := newTestServer(&fakeObjects{}, sched, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))

	var jobs []Job
	json.Unmarshal(rec.Body.Bytes(), &jobs)
	ids := []string{jobs[0].ID, jobs[1].ID}
	if ids[0] != "new" || ids[1] != "old" {
		t.Errorf("order = %v, want [new old]", ids)
	}
}

func TestListJobsStillReturnsJobsWhenProgressStoreFails(t *testing.T) {
	sched := &fakeScheduler{jobs: []JobStatus{{ID: "abc123", Phase: PhaseRunning}}}
	srv := newTestServer(&fakeObjects{}, sched, &fakeProgress{err: errors.New("dynamodb throttled")})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (progress is best-effort)", rec.Code)
	}
}

// --- GET /api/jobs/{id} ----------------------------------------------------

func TestGetJobReturnsSingleJob(t *testing.T) {
	sched := &fakeScheduler{jobs: []JobStatus{{ID: "abc123", Phase: PhaseRunning}, {ID: "other"}}}
	srv := newTestServer(&fakeObjects{}, sched, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs/abc123", nil))

	var j Job
	json.Unmarshal(rec.Body.Bytes(), &j)
	if rec.Code != http.StatusOK || j.ID != "abc123" {
		t.Errorf("status = %d job = %+v", rec.Code, j)
	}
}

func TestGetJobReturns404ForUnknownID(t *testing.T) {
	srv := newTestServer(&fakeObjects{}, &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs/missing", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// --- GET /api/jobs/{id}/files ----------------------------------------------

func storedJob() *fakeObjects {
	return &fakeObjects{puts: map[string]string{
		"code/abc123/job.yaml":      validJobSet,
		"code/abc123/train.py":      "print('hi')",
		"datasets/abc123/train.csv": "1,2,3",
		"code/other/train.py":       "not this job",
	}}
}

func TestListFilesReturnsCodeAndDataOfOneJob(t *testing.T) {
	srv := newTestServer(storedJob(), &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs/abc123/files", nil))

	var files []File
	json.Unmarshal(rec.Body.Bytes(), &files)
	want := []File{
		{Kind: "code", Name: "job.yaml", Size: int64(len(validJobSet))},
		{Kind: "code", Name: "train.py", Size: 11},
		{Kind: "data", Name: "train.csv", Size: 5},
	}
	if rec.Code != http.StatusOK || len(files) != len(want) {
		t.Fatalf("status = %d files = %+v, want %+v", rec.Code, files, want)
	}
	for i := range want {
		if files[i] != want[i] {
			t.Errorf("files[%d] = %+v, want %+v", i, files[i], want[i])
		}
	}
}

func TestGetFileReturnsContentAsText(t *testing.T) {
	srv := newTestServer(storedJob(), &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs/abc123/files/code/train.py", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != "print('hi')" {
		t.Errorf("status = %d body = %q", rec.Code, rec.Body)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain") {
		t.Errorf("content-type = %q, want text/plain", rec.Header().Get("Content-Type"))
	}
	if rec.Header().Get("X-Truncated") != "false" {
		t.Errorf("X-Truncated = %q, want false", rec.Header().Get("X-Truncated"))
	}
}

func TestGetFileTruncatesLargeDatasets(t *testing.T) {
	big := strings.Repeat("1,2,3\n", 20000) // 120000 bytes
	objects := &fakeObjects{puts: map[string]string{"datasets/abc123/big.csv": big}}
	srv := newTestServer(objects, &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs/abc123/files/data/big.csv", nil))

	if rec.Code != http.StatusOK || rec.Body.Len() != maxPreviewBytes {
		t.Errorf("status = %d len = %d, want %d bytes", rec.Code, rec.Body.Len(), maxPreviewBytes)
	}
	if rec.Header().Get("X-Truncated") != "true" || rec.Header().Get("X-Total-Size") != "120000" {
		t.Errorf("X-Truncated = %q X-Total-Size = %q", rec.Header().Get("X-Truncated"), rec.Header().Get("X-Total-Size"))
	}
}

func TestGetFileRejectsKindsOtherThanCodeAndData(t *testing.T) {
	objects := storedJob()
	objects.puts["checkpoints/abc123/ckpt-1000.pt"] = "weights"
	srv := newTestServer(objects, &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs/abc123/files/checkpoints/ckpt-1000.pt", nil))

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestGetFileNeverServesAnotherJobsFiles(t *testing.T) {
	srv := newTestServer(storedJob(), &fakeScheduler{}, &fakeProgress{})

	for _, path := range []string{
		"/api/jobs/abc123/files/code/..%2F..%2Fcode%2Fother%2Ftrain.py",
		"/api/jobs/..%2Fother/files/code/train.py",
		"/api/jobs/abc123/files/code/%2E%2E",
	} {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))

		if rec.Code == http.StatusOK || strings.Contains(rec.Body.String(), "not this job") {
			t.Errorf("%s: status = %d body = %q, want rejection", path, rec.Code, rec.Body)
		}
	}
}

func TestGetFileReturns404ForMissingFile(t *testing.T) {
	srv := newTestServer(storedJob(), &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs/abc123/files/data/missing.csv", nil))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

// --- misc ------------------------------------------------------------------

func TestHealthz(t *testing.T) {
	srv := newTestServer(&fakeObjects{}, &fakeScheduler{}, &fakeProgress{})

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestCORSPreflightAllowsFrontendOrigin(t *testing.T) {
	srv := newTestServer(&fakeObjects{}, &fakeScheduler{}, &fakeProgress{})
	req := httptest.NewRequest(http.MethodOptions, "/api/jobs", nil)
	req.Header.Set("Origin", "http://localhost:5173")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:5173" {
		t.Errorf("allow-origin = %q", rec.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORSExposesPreviewHeadersToFrontend(t *testing.T) {
	srv := newTestServer(storedJob(), &fakeScheduler{}, &fakeProgress{})
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/abc123/files/code/train.py", nil)
	req.Header.Set("Origin", "http://localhost:5173")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Expose-Headers"); !strings.Contains(got, "X-Truncated") || !strings.Contains(got, "X-Total-Size") {
		t.Errorf("expose-headers = %q, browser cannot read truncation headers", got)
	}
}
