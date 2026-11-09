package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service"
)

// fakeJobs records what the handler passes in and returns canned results.
type fakeJobs struct {
	submitted *model.SubmitJobRequest
	created   model.CreatedJob
	jobs      []model.Job
	events    []model.JobEvent
	list      []model.File
	models    []model.Model
	datasets  []model.Dataset
	content   model.FileContent
	gotArgs   []string
	err       error
}

func (f *fakeJobs) Submit(_ context.Context, req model.SubmitJobRequest) (model.CreatedJob, error) {
	f.submitted = &req
	return f.created, f.err
}

func (f *fakeJobs) Models(context.Context) ([]model.Model, error) { return f.models, f.err }

func (f *fakeJobs) ModelFile(_ context.Context, name, file string) (model.FileContent, error) {
	f.gotArgs = []string{name, file}
	return f.content, f.err
}

func (f *fakeJobs) Datasets(context.Context) ([]model.Dataset, error) { return f.datasets, f.err }

func (f *fakeJobs) Dataset(_ context.Context, id string) (model.Dataset, error) {
	f.gotArgs = []string{id}
	if len(f.datasets) == 0 {
		return model.Dataset{}, f.err
	}
	return f.datasets[0], f.err
}

func (f *fakeJobs) DatasetFile(_ context.Context, id string, version int, name string) (model.FileContent, error) {
	f.gotArgs = []string{id, strconv.Itoa(version), name}
	return f.content, f.err
}

func (f *fakeJobs) List(context.Context) ([]model.Job, error) { return f.jobs, f.err }

func (f *fakeJobs) Get(_ context.Context, id string) (model.Job, error) {
	f.gotArgs = []string{id}
	if len(f.jobs) == 0 {
		return model.Job{}, f.err
	}
	return f.jobs[0], f.err
}

func (f *fakeJobs) Events(_ context.Context, id string) ([]model.JobEvent, error) {
	f.gotArgs = []string{id}
	return f.events, f.err
}

func (f *fakeJobs) Files(_ context.Context, id string) ([]model.File, error) {
	f.gotArgs = []string{id}
	return f.list, f.err
}

func (f *fakeJobs) File(_ context.Context, id, kind, name string) (model.FileContent, error) {
	f.gotArgs = []string{id, kind, name}
	return f.content, f.err
}

func newRouter(jobs JobService) *gin.Engine {
	return NewRouter(jobs, "http://localhost:5173")
}

func do(r http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

func get(r http.Handler, path string) *httptest.ResponseRecorder {
	return do(r, httptest.NewRequest(http.MethodGet, path, nil))
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body, err)
	}
	return v
}

// --- POST /api/jobs --------------------------------------------------------

func postJob(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func TestCreateJobPassesJSONRequestToService(t *testing.T) {
	jobs := &fakeJobs{created: model.CreatedJob{ID: "3f9a1c2e", JobsetName: "job-3f9a1c2e-g0001", Config: []byte(`{"epochs":3}`)}}

	rec := do(newRouter(jobs), postJob(`{"name":"gpt2-demo","model":"gpt2","strategy":"fsdp","gpus":2,
		"dataset":{"id":"ds-test","version":1},"hyperparameters":{"epochs":3}}`))

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	got := jobs.submitted
	if got.Name != "gpt2-demo" || got.Model != "gpt2" || got.Strategy != "fsdp" || got.GPUs != 2 ||
		got.Dataset == nil || got.Dataset.ID != "ds-test" || got.Dataset.Version != 1 || got.Hyperparameters["epochs"] != 3.0 {
		t.Errorf("request = %+v", got)
	}
	body := decode[map[string]any](t, rec)
	if body["jobsetName"] != "job-3f9a1c2e-g0001" || body["config"].(map[string]any)["epochs"] != 3.0 {
		t.Errorf("body = %v", body)
	}
}

func TestCreateJobRejectsMalformedBodyAndUnknownKeys(t *testing.T) {
	for _, body := range []string{`not json`, ``, `{"model":"gpt2","gpus":"two"}`, `{"modle":"gpt2"}`} {
		jobs := &fakeJobs{}

		rec := do(newRouter(jobs), postJob(body))

		if rec.Code != http.StatusBadRequest || decode[model.ErrorResponse](t, rec).Code != "VALIDATION" {
			t.Errorf("%q: status = %d body = %s", body, rec.Code, rec.Body)
		}
		if jobs.submitted != nil {
			t.Errorf("%q: reached the service", body)
		}
	}
}

func TestCreateJobReturnsFieldErrors(t *testing.T) {
	jobs := &fakeJobs{err: &service.Error{Kind: service.KindInvalid, Msg: "bad", Fields: []model.FieldError{{Field: "gpus", Message: "2~2"}}}}

	rec := do(newRouter(jobs), postJob(`{"model":"gpt2"}`))

	got := decode[model.ErrorResponse](t, rec)
	if rec.Code != http.StatusBadRequest || got.Code != "VALIDATION" || len(got.Fields) != 1 || got.Fields[0].Field != "gpus" {
		t.Errorf("status = %d body = %s", rec.Code, rec.Body)
	}
}

// --- error mapping ---------------------------------------------------------

func TestServiceErrorsMapToStatus(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
		api  string
		msg  string
	}{
		{&service.Error{Kind: service.KindInvalid, Msg: "invalid job id"}, http.StatusBadRequest, "VALIDATION", "invalid job id"},
		{&service.Error{Kind: service.KindNotFound, Msg: "job not found"}, http.StatusNotFound, "NOT_FOUND", "job not found"},
		{&service.Error{Kind: service.KindConflict, Msg: "exists"}, http.StatusConflict, "ALREADY_EXISTS", "exists"},
		{&service.Error{Kind: service.KindUnprocessable, Msg: "format"}, http.StatusUnprocessableEntity, "DATASET_FORMAT_MISMATCH", "format"},
		{&service.Error{Kind: service.KindUpstream, Code: "SUBMIT_FAILED", Msg: "boom"}, http.StatusBadGateway, "SUBMIT_FAILED", "boom"},
		{errors.New("unexpected"), http.StatusInternalServerError, "INTERNAL", "internal error"},
	} {
		rec := get(newRouter(&fakeJobs{err: tc.err}), "/api/jobs/abc123")

		got := decode[model.ErrorResponse](t, rec)
		if rec.Code != tc.code || got.Error != tc.msg || got.Code != tc.api {
			t.Errorf("%v: status = %d body = %s, want %d %s %q", tc.err, rec.Code, rec.Body, tc.code, tc.api, tc.msg)
		}
	}
}

// --- GET -------------------------------------------------------------------

func TestListJobsWrapsItems(t *testing.T) {
	jobs := &fakeJobs{jobs: []model.Job{{JobStatus: model.JobStatus{ID: "abc123", Status: model.StatusRunning, CreatedAt: time.Unix(100, 0).UTC()}}}}

	rec := get(newRouter(jobs), "/api/jobs")

	got := decode[map[string]any](t, rec)
	items, _ := got["items"].([]any)
	if rec.Code != http.StatusOK || len(items) != 1 {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body)
	}
	item := items[0].(map[string]any)
	if item["id"] != "abc123" || item["status"] != "RUNNING" {
		t.Errorf("item = %v", item)
	}
	if v, ok := item["progress"]; !ok || v != nil {
		t.Error("progress must be present (null) so the frontend can tell 'no record yet'")
	}
	if v, ok := got["nextCursor"]; !ok || v != nil {
		t.Errorf("nextCursor = %v, want null", v)
	}
}

func TestListModelsWrapsItems(t *testing.T) {
	jobs := &fakeJobs{models: []model.Model{{Name: "gpt2", DefaultStrategy: "fsdp"}}}

	rec := get(newRouter(jobs), "/api/models")

	got := decode[model.ItemList[model.Model]](t, rec)
	if rec.Code != http.StatusOK || len(got.Items) != 1 || got.Items[0].Name != "gpt2" {
		t.Errorf("status = %d body = %s", rec.Code, rec.Body)
	}
}

func TestModelFilePassesPathAndReturnsText(t *testing.T) {
	jobs := &fakeJobs{content: model.FileContent{Data: []byte("{}"), TotalSize: 2}}

	rec := get(newRouter(jobs), "/api/models/gpt2/files/config.json")

	if rec.Code != http.StatusOK || rec.Body.String() != "{}" || strings.Join(jobs.gotArgs, " ") != "gpt2 config.json" {
		t.Errorf("status = %d body = %q args = %v", rec.Code, rec.Body, jobs.gotArgs)
	}
}

func TestDatasetRoutes(t *testing.T) {
	jobs := &fakeJobs{datasets: []model.Dataset{{ID: "ds-test", LatestVersion: 1}}, content: model.FileContent{Data: []byte("line")}}
	r := newRouter(jobs)

	if got := decode[model.ItemList[model.Dataset]](t, get(r, "/api/datasets")); len(got.Items) != 1 {
		t.Errorf("list = %+v", got)
	}
	if got := decode[model.Dataset](t, get(r, "/api/datasets/ds-test")); got.ID != "ds-test" || jobs.gotArgs[0] != "ds-test" {
		t.Errorf("detail = %+v args = %v", got, jobs.gotArgs)
	}
	rec := get(r, "/api/datasets/ds-test/versions/1/files/train-000.jsonl")
	if rec.Body.String() != "line" || strings.Join(jobs.gotArgs, " ") != "ds-test 1 train-000.jsonl" {
		t.Errorf("file = %q args = %v", rec.Body, jobs.gotArgs)
	}
}

func TestDatasetFileRejectsNonNumericVersion(t *testing.T) {
	rec := get(newRouter(&fakeJobs{}), "/api/datasets/ds-test/versions/latest/files/a.jsonl")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d", rec.Code)
	}
}

func TestGetJobPassesPathID(t *testing.T) {
	jobs := &fakeJobs{jobs: []model.Job{{JobStatus: model.JobStatus{ID: "abc123"}}}}

	rec := get(newRouter(jobs), "/api/jobs/abc123")

	if rec.Code != http.StatusOK || jobs.gotArgs[0] != "abc123" {
		t.Errorf("status = %d args = %v", rec.Code, jobs.gotArgs)
	}
}

func TestListEventsReturnsEmptyArrayAsJSON(t *testing.T) {
	rec := get(newRouter(&fakeJobs{events: []model.JobEvent{}}), "/api/jobs/abc123/events")

	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Errorf("status = %d body = %s, want []", rec.Code, rec.Body)
	}
}

func TestListFilesReturnsFiles(t *testing.T) {
	jobs := &fakeJobs{list: []model.File{{Kind: "code", Name: "train.py", Size: 11}}}

	rec := get(newRouter(jobs), "/api/jobs/abc123/files")

	if got := decode[[]model.File](t, rec); rec.Code != http.StatusOK || len(got) != 1 || got[0] != jobs.list[0] {
		t.Errorf("status = %d files = %+v", rec.Code, got)
	}
}

func TestGetFileReturnsTextWithTruncationHeaders(t *testing.T) {
	jobs := &fakeJobs{content: model.FileContent{Data: []byte("1,2,3"), Truncated: true, TotalSize: 120000}}

	rec := get(newRouter(jobs), "/api/jobs/abc123/files/data/big.csv")

	if rec.Code != http.StatusOK || rec.Body.String() != "1,2,3" {
		t.Errorf("status = %d body = %q", rec.Code, rec.Body)
	}
	if strings.Join(jobs.gotArgs, " ") != "abc123 data big.csv" {
		t.Errorf("service args = %v", jobs.gotArgs)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content-type = %q", ct)
	}
	if rec.Header().Get("X-Truncated") != "true" || rec.Header().Get("X-Total-Size") != "120000" {
		t.Errorf("X-Truncated = %q X-Total-Size = %q", rec.Header().Get("X-Truncated"), rec.Header().Get("X-Total-Size"))
	}
}

// Encoded slashes in the name must not reach the service as a path.
func TestGetFileDoesNotRouteEncodedSlashes(t *testing.T) {
	jobs := &fakeJobs{content: model.FileContent{Data: []byte("secret")}}

	rec := get(newRouter(jobs), "/api/jobs/abc123/files/code/..%2F..%2Fcode%2Fother%2Ftrain.py")

	if rec.Code == http.StatusOK {
		t.Errorf("status = %d args = %v, want no match", rec.Code, jobs.gotArgs)
	}
}

func TestHealthz(t *testing.T) {
	rec := get(newRouter(&fakeJobs{}), "/healthz")

	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("status = %d body = %q", rec.Code, rec.Body)
	}
}

func TestCORSPreflightReachesMiddleware(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/api/jobs", nil)
	req.Header.Set("Origin", "http://localhost:5173")

	rec := do(newRouter(&fakeJobs{}), req)

	if rec.Code != http.StatusNoContent || rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:5173" {
		t.Errorf("status = %d allow-origin = %q", rec.Code, rec.Header().Get("Access-Control-Allow-Origin"))
	}
}
