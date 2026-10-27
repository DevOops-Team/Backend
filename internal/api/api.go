// Package api serves the HTTP API the frontend uses to submit and watch training jobs.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/jobspec"
)

// ObjectStore stores uploaded job inputs (S3).
type ObjectStore interface {
	Put(ctx context.Context, key string, body io.Reader) error
}

// Scheduler submits JobSets to the management cluster and reads their queue state (k3s + Kueue).
type Scheduler interface {
	Submit(ctx context.Context, js *unstructured.Unstructured) error
	List(ctx context.Context) ([]JobStatus, error)
}

// ProgressStore reads the per-step progress train.py writes (DynamoDB).
type ProgressStore interface {
	Get(ctx context.Context, jobIDs []string) (map[string]Progress, error)
}

// Phase is the job state derived from the JobSet and its Kueue Workload.
type Phase string

const (
	PhasePending   Phase = "Pending"   // waiting in queue-auto for a worker to admit it
	PhaseAdmitted  Phase = "Admitted"  // a worker reserved quota; pods are starting
	PhaseRunning   Phase = "Running"   // JobSet has active pods
	PhaseSucceeded Phase = "Succeeded" // JobSet completed
	PhaseFailed    Phase = "Failed"    // JobSet failed (restarts exhausted)
)

// JobStatus is what the management cluster knows about a job.
type JobStatus struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Phase     Phase     `json:"phase"`
	Cluster   string    `json:"cluster,omitempty"` // worker MultiKueue picked
	Restarts  int64     `json:"restarts"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

// Progress is the item train.py (rank 0) overwrites every step.
type Progress struct {
	Status             string  `json:"status"`
	Step               int64   `json:"step"`
	TotalSteps         int64   `json:"totalSteps"`
	Loss               float64 `json:"loss"`
	LastCheckpointStep int64   `json:"lastCheckpointStep"`
	NodeName           string  `json:"nodeName,omitempty"`
	UpdatedAt          int64   `json:"updatedAt"` // epoch ms
}

// Job is the API view: queue state plus training progress (nil until train.py first writes).
type Job struct {
	JobStatus
	Progress *Progress `json:"progress"`
}

type Config struct {
	Namespace     string
	QueueName     string
	ProgressTable string
	Region        string
	AllowedOrigin string
	NewID         func() string
}

// maxUploadBytes caps one submission (job.yaml + train.py + data).
const maxUploadBytes = 2 << 30

type server struct {
	cfg      Config
	objects  ObjectStore
	sched    Scheduler
	progress ProgressStore
}

func NewServer(cfg Config, objects ObjectStore, sched Scheduler, progress ProgressStore) http.Handler {
	s := &server{cfg: cfg, objects: objects, sched: sched, progress: progress}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /api/jobs", s.createJob)
	mux.HandleFunc("GET /api/jobs", s.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	return s.cors(mux)
}

func (s *server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" && origin == s.cfg.AllowedOrigin {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) createJob(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "expected multipart form: "+err.Error())
		return
	}
	defer r.MultipartForm.RemoveAll()

	jobFile, trainFile, dataFiles := one(r.MultipartForm, "job"), one(r.MultipartForm, "train"), r.MultipartForm.File["data"]
	switch {
	case jobFile == nil:
		writeError(w, http.StatusBadRequest, "job (job.yaml) is required")
		return
	case trainFile == nil:
		writeError(w, http.StatusBadRequest, "train (train.py) is required")
		return
	case len(dataFiles) == 0:
		writeError(w, http.StatusBadRequest, "at least one data file is required")
		return
	}

	manifest, err := readAll(jobFile)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	id := s.cfg.NewID()
	js, err := jobspec.Prepare(manifest, jobspec.Options{
		JobID:         id,
		Namespace:     s.cfg.Namespace,
		QueueName:     s.cfg.QueueName,
		ProgressTable: s.cfg.ProgressTable,
		Region:        s.cfg.Region,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Inputs must be in S3 before the JobSet exists: the pods mount code/<id> and datasets/<id>.
	if err := s.upload(r.Context(), "code/"+id+"/train.py", trainFile); err != nil {
		s.badGateway(w, "upload train.py", err)
		return
	}
	for _, f := range dataFiles {
		name := safeName(f.Filename)
		if name == "" {
			writeError(w, http.StatusBadRequest, "invalid data filename: "+f.Filename)
			return
		}
		if err := s.upload(r.Context(), "datasets/"+id+"/"+name, f); err != nil {
			s.badGateway(w, "upload "+name, err)
			return
		}
	}

	if err := s.sched.Submit(r.Context(), js); err != nil {
		s.badGateway(w, "submit JobSet", err)
		return
	}

	slog.Info("job submitted", "id", id, "name", js.GetName())
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "name": js.GetName()})
}

func (s *server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.jobs(r.Context())
	if err != nil {
		s.badGateway(w, "list jobs", err)
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *server) getJob(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.jobs(r.Context())
	if err != nil {
		s.badGateway(w, "list jobs", err)
		return
	}
	for _, j := range jobs {
		if j.ID == r.PathValue("id") {
			writeJSON(w, http.StatusOK, j)
			return
		}
	}
	writeError(w, http.StatusNotFound, "job not found")
}

// jobs returns queue state merged with progress, newest first.
// Progress is best-effort: a DynamoDB error still returns the queue state.
func (s *server) jobs(ctx context.Context) ([]Job, error) {
	statuses, err := s.sched.List(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(statuses))
	for i, st := range statuses {
		ids[i] = st.ID
	}
	progress, err := s.progress.Get(ctx, ids)
	if err != nil {
		slog.Warn("progress lookup failed", "err", err)
		progress = nil
	}

	jobs := make([]Job, len(statuses))
	for i, st := range statuses {
		jobs[i] = Job{JobStatus: st}
		if p, ok := progress[st.ID]; ok {
			jobs[i].Progress = &p
		}
	}
	sort.SliceStable(jobs, func(a, b int) bool { return jobs[a].CreatedAt.After(jobs[b].CreatedAt) })
	return jobs, nil
}

func (s *server) upload(ctx context.Context, key string, fh *multipart.FileHeader) error {
	f, err := fh.Open()
	if err != nil {
		return err
	}
	defer f.Close()
	return s.objects.Put(ctx, key, f)
}

func (s *server) badGateway(w http.ResponseWriter, what string, err error) {
	slog.Error(what+" failed", "err", err)
	writeError(w, http.StatusBadGateway, what+" failed: "+err.Error())
}

func one(form *multipart.Form, field string) *multipart.FileHeader {
	if fs := form.File[field]; len(fs) > 0 {
		return fs[0]
	}
	return nil
}

func readAll(fh *multipart.FileHeader) ([]byte, error) {
	f, err := fh.Open()
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// safeName keeps only the base name so uploads cannot escape datasets/<id>/.
func safeName(filename string) string {
	name := path.Base(strings.ReplaceAll(filename, "\\", "/"))
	if name == "." || name == "/" || name == ".." {
		return ""
	}
	return name
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil && !errors.Is(err, http.ErrHandlerTimeout) {
		slog.Warn("write response", "err", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
