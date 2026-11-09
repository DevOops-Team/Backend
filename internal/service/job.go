// Package service holds the business logic (use cases) for training jobs. It depends on the
// repositories only through the interfaces below, so tests swap them for fakes.
package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service/jobspec"
	"github.com/DevOops-Team/Backend/internal/settings"
)

// ObjectStore stores job inputs and reads prepared models and datasets (S3).
type ObjectStore interface {
	Put(ctx context.Context, key string, body io.Reader) error
	List(ctx context.Context, prefix string) ([]model.ObjectInfo, error)
	// Get returns at most limit bytes of the object and its full size, or model.ErrNotFound.
	Get(ctx context.Context, key string, limit int64) ([]byte, int64, error)
}

// Scheduler submits JobSets to the management cluster and reads their queue state (k3s + Kueue).
type Scheduler interface {
	// Submit returns model.ErrAlreadyExists when the JobSet name is taken.
	Submit(ctx context.Context, js *unstructured.Unstructured) error
	List(ctx context.Context) ([]model.JobStatus, error)
}

// ProgressStore reads the per-step progress train.py writes (DynamoDB).
type ProgressStore interface {
	Get(ctx context.Context, jobIDs []string) (map[string]model.Progress, error)
}

// EventReader reads a job's delivery timeline, oldest first (now DynamoDB; Kafka is planned).
// To change the source, implement this and pass it in cmd/api/main.go; the service and API stay the same.
type EventReader interface {
	List(ctx context.Context, jobID string) ([]model.JobEvent, error)
}

// EventWriter records what the backend itself did, such as submitting a job.
type EventWriter interface {
	Put(ctx context.Context, e model.JobEvent) error
}

const (
	// MaxPreviewBytes caps how much of one file the viewer gets; datasets can be GBs.
	MaxPreviewBytes = 64 << 10

	maxMetadataBytes = 4 << 20 // config files, manifests and train.py are small
	firstGeneration  = 1       // 임시 구성: 복구가 없어 세대는 항상 1이다.
	configSchema     = 1
)

var (
	validJobID  = regexp.MustCompile(`^[0-9a-z]+$`)
	validName   = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	validDataID = regexp.MustCompile(`^ds-[a-z0-9-]+$`)
	manifestKey = regexp.MustCompile(`^datasets/(ds-[a-z0-9-]+)/v([0-9]+)/manifest\.json$`)
)

type Config struct {
	Namespace     string
	Region        string
	Bucket        string
	ProgressTable string
	Settings      settings.Settings
	NewID         func() string
	Now           func() time.Time // nil = time.Now
}

type JobService struct {
	cfg      Config
	objects  ObjectStore
	sched    Scheduler
	progress ProgressStore
	events   EventReader
	record   EventWriter
}

func NewJobService(cfg Config, objects ObjectStore, sched Scheduler, progress ProgressStore, events EventReader, record EventWriter) *JobService {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &JobService{cfg: cfg, objects: objects, sched: sched, progress: progress, events: events, record: record}
}

// Submit validates the request, writes config.json and jobset.json to S3 and creates the JobSet.
// Validation comes first, so nothing is written for a rejected request.
func (s *JobService) Submit(ctx context.Context, req model.SubmitJobRequest) (model.CreatedJob, error) {
	profile, strategy, err := s.validate(req)
	if err != nil {
		return model.CreatedJob{}, err
	}
	merged, fields := mergeHyperparameters(profile, req.Hyperparameters)
	if len(fields) > 0 {
		return model.CreatedJob{}, invalidFields(fields)
	}
	queue, err := s.cfg.Settings.QueueFor(req.GPUs)
	if err != nil {
		return model.CreatedJob{}, invalidFields([]model.FieldError{{Field: "gpus", Message: err.Error()}})
	}
	manifest, manifestSHA, err := s.readManifest(ctx, req.Dataset.ID, req.Dataset.Version)
	if err != nil {
		return model.CreatedJob{}, err
	}
	if !slices.Contains(profile.DatasetFormats, manifest.Format) {
		return model.CreatedJob{}, &Error{Kind: KindUnprocessable, Msg: fmt.Sprintf("model %s does not read dataset format %s", req.Model, manifest.Format)}
	}
	trainScript, err := s.readTrainScript(ctx)
	if err != nil {
		return model.CreatedJob{}, err
	}

	id := s.cfg.NewID()
	name := req.Name
	if name == "" {
		name = req.Model + "-" + id
	}
	checkpointFormat := s.cfg.Settings.Strategies[strategy].CheckpointFormat
	configBytes, err := json.MarshalIndent(model.TrainConfig{
		SchemaVersion:    configSchema,
		Model:            req.Model,
		TrainingMode:     strategy,
		CheckpointFormat: checkpointFormat,
		Dataset: model.ConfigDataset{
			URI:            fmt.Sprintf("s3://%s/%s", s.cfg.Bucket, datasetPrefix(req.Dataset.ID, req.Dataset.Version)),
			Format:         manifest.Format,
			ManifestSHA256: manifestSHA,
		},
		Epochs:               merged.Epochs,
		MaxSteps:             merged.MaxSteps,
		BatchSizePerGPU:      merged.BatchSizePerGPU,
		LearningRate:         merged.LearningRate,
		WarmupSteps:          merged.WarmupSteps,
		CheckpointEverySteps: merged.CheckpointEverySteps,
		Seed:                 merged.Seed,
		WorldSize:            req.GPUs,
	}, "", "  ")
	if err != nil {
		return model.CreatedJob{}, fmt.Errorf("encode config: %w", err)
	}
	configSHA := sha256Hex(configBytes)

	js, err := jobspec.Render(jobspec.Input{
		JobID: id, Generation: firstGeneration, Name: name, Model: req.Model, Strategy: strategy, GPUs: req.GPUs,
		DatasetID: req.Dataset.ID, DatasetVersion: req.Dataset.Version, ConfigSHA256: configSHA,
		CheckpointFormat: checkpointFormat, Hyperparameters: req.Hyperparameters, ModelSubPath: profile.ModelSubPath,
		Queue: queue, Namespace: s.cfg.Namespace, Region: s.cfg.Region, ProgressTable: s.cfg.ProgressTable,
		Settings: s.cfg.Settings,
	})
	if err != nil {
		return model.CreatedJob{}, fmt.Errorf("render JobSet: %w", err)
	}
	jobsetBytes, err := json.MarshalIndent(js.Object, "", "  ")
	if err != nil {
		return model.CreatedJob{}, fmt.Errorf("encode JobSet: %w", err)
	}

	// Everything the pods read must be in S3 before the JobSet exists.
	for _, f := range []struct {
		name string
		body []byte
	}{{"config.json", configBytes}, {"train.py", trainScript}, {"jobset.json", jobsetBytes}} {
		if err := s.objects.Put(ctx, "code/"+id+"/"+f.name, bytes.NewReader(f.body)); err != nil {
			return model.CreatedJob{}, upstream("upload "+f.name, err)
		}
	}

	if err := s.sched.Submit(ctx, js); err != nil {
		if errors.Is(err, model.ErrAlreadyExists) {
			return model.CreatedJob{}, &Error{Kind: KindConflict, Msg: "JobSet " + js.GetName() + " already exists", Err: err}
		}
		return model.CreatedJob{}, &Error{Kind: KindUpstream, Code: "SUBMIT_FAILED", Msg: "submit JobSet failed: " + err.Error(), Err: err}
	}
	s.recordSubmitted(ctx, id, js.GetName(), queue)

	slog.Info("job submitted", "id", id, "name", js.GetName(), "strategy", strategy, "gpus", req.GPUs)
	return model.CreatedJob{
		ID: id, Name: name, Status: model.StatusQueued, Generation: firstGeneration, JobsetName: js.GetName(),
		Queue: queue, Config: configBytes, ConfigSHA256: configSHA, CreatedAt: s.cfg.Now().UTC(),
	}, nil
}

// validate checks everything that needs no I/O and resolves the strategy.
func (s *JobService) validate(req model.SubmitJobRequest) (settings.Profile, string, error) {
	var fields []model.FieldError
	bad := func(field, msg string) { fields = append(fields, model.FieldError{Field: field, Message: msg}) }

	if req.Name != "" && !validName.MatchString(req.Name) {
		bad("name", "1~63자, 소문자 · 숫자 · '-'만 쓸 수 있습니다")
	}
	profile, ok := s.cfg.Settings.ModelProfiles[req.Model]
	strategy := req.Strategy
	if !ok {
		bad("model", "등록되지 않은 모델입니다")
	} else {
		if strategy == "" {
			strategy = profile.DefaultStrategy
		}
		if !slices.Contains(profile.Strategies, strategy) {
			bad("strategy", "지원 방식: "+strings.Join(profile.Strategies, ", "))
		}
		if msg := gpuProblem(profile, s.cfg.Settings.Strategies[strategy], strategy, req.GPUs); msg != "" {
			bad("gpus", msg)
		}
	}
	if req.Dataset == nil || req.Dataset.ID == "" || req.Dataset.Version < 1 {
		bad("dataset", "{id, version}이 필요합니다")
	} else if !validDataID.MatchString(req.Dataset.ID) {
		bad("dataset", "잘못된 데이터셋 ID입니다")
	}
	if len(fields) > 0 {
		return settings.Profile{}, "", invalidFields(fields)
	}
	return profile, strategy, nil
}

// gpuProblem says why this GPU count cannot run, or "" when it can.
func gpuProblem(p settings.Profile, st settings.Strategy, strategy string, gpus int) string {
	switch {
	case !slices.Contains(p.GPUs.Options, gpus):
		return fmt.Sprintf("선택 가능한 GPU 수: %v", p.GPUs.Options)
	case gpus > p.GPUs.Max:
		return fmt.Sprintf("현재 GPU 할당량으로는 최대 %d개까지 쓸 수 있습니다", p.GPUs.Max)
	case gpus < st.MinGPUs:
		return fmt.Sprintf("%s는 GPU %d개 이상이 필요합니다", strings.ToUpper(strategy), st.MinGPUs)
	}
	return ""
}

// mergeHyperparameters overlays the keys the profile opens on its defaults.
func mergeHyperparameters(p settings.Profile, in map[string]any) (settings.Defaults, []model.FieldError) {
	merged := p.Defaults
	var fields []model.FieldError
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		field := "hyperparameters." + key
		i := slices.IndexFunc(p.Hyperparameters, func(h settings.Hyperparameter) bool { return h.Key == key })
		if i < 0 {
			fields = append(fields, model.FieldError{Field: field, Message: "변경할 수 없는 값입니다"})
			continue
		}
		def := p.Hyperparameters[i]
		v, ok := in[key].(float64)
		switch {
		case !ok:
			fields = append(fields, model.FieldError{Field: field, Message: "숫자여야 합니다"})
		case def.Type == "integer" && v != math.Trunc(v):
			fields = append(fields, model.FieldError{Field: field, Message: "정수여야 합니다"})
		case def.Min != nil && v < *def.Min, def.Max != nil && v > *def.Max, def.ExclusiveMin != nil && v <= *def.ExclusiveMin:
			fields = append(fields, model.FieldError{Field: field, Message: "범위를 벗어났습니다"})
		default:
			switch key {
			case "epochs":
				merged.Epochs = int64(v)
			case "batch_size_per_gpu":
				merged.BatchSizePerGPU = int64(v)
			case "learning_rate":
				merged.LearningRate = v
			default:
				fields = append(fields, model.FieldError{Field: field, Message: "변경할 수 없는 값입니다"})
			}
		}
	}
	return merged, fields
}

func (s *JobService) readTrainScript(ctx context.Context) ([]byte, error) {
	key := s.cfg.Settings.TrainScriptKey
	data, size, err := s.objects.Get(ctx, key, maxMetadataBytes)
	if errors.Is(err, model.ErrNotFound) {
		return nil, notFound(key + " not found: upload the test assets first (examples/fsdp/seed_assets.py)")
	}
	if err != nil {
		return nil, upstream("read "+key, err)
	}
	if int64(len(data)) < size {
		return nil, invalid(key + " is too large")
	}
	return data, nil
}

// recordSubmitted writes the first timeline step. Best-effort: the job is already queued,
// so a DynamoDB error only loses this entry.
func (s *JobService) recordSubmitted(ctx context.Context, id, name, queue string) {
	now := s.cfg.Now().UTC().Truncate(time.Millisecond)
	err := s.record.Put(ctx, model.JobEvent{
		JobID: id, Key: now.Format(model.EventKeyTimeFormat) + "#" + model.ManagerCluster + "#submitted",
		Time: now, LastTime: now, Cluster: model.ManagerCluster, Source: "backend", Reason: "Submitted",
		Message: "config.json and jobset.json stored in S3; JobSet " + name + " created in queue " + queue,
		Type:    "Normal", Object: "JobSet/" + name, Count: 1,
	})
	if err != nil {
		slog.Warn("record submitted event", "id", id, "err", err)
	}
}

// List returns queue state merged with progress, newest first.
// Progress is best-effort: a DynamoDB error still returns the queue state.
func (s *JobService) List(ctx context.Context) ([]model.Job, error) {
	statuses, err := s.sched.List(ctx)
	if err != nil {
		return nil, upstream("list jobs", err)
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

	jobs := make([]model.Job, len(statuses))
	for i, st := range statuses {
		jobs[i] = model.Job{JobStatus: st, UpdatedAt: st.CreatedAt}
		if p, ok := progress[st.ID]; ok {
			jobs[i].Progress = &p
			jobs[i].UpdatedAt = p.UpdatedAt
		}
	}
	sort.SliceStable(jobs, func(a, b int) bool { return jobs[a].CreatedAt.After(jobs[b].CreatedAt) })
	return jobs, nil
}

// Get returns one job with its final config. The config is best-effort: S3 trouble must not hide the state.
func (s *JobService) Get(ctx context.Context, id string) (model.Job, error) {
	jobs, err := s.List(ctx)
	if err != nil {
		return model.Job{}, err
	}
	i := slices.IndexFunc(jobs, func(j model.Job) bool { return j.ID == id })
	if i < 0 {
		return model.Job{}, notFound("job not found")
	}
	job := jobs[i]
	if data, _, err := s.objects.Get(ctx, "code/"+id+"/config.json", maxMetadataBytes); err == nil {
		job.Config = data
		var cfg model.TrainConfig
		if json.Unmarshal(data, &cfg) == nil && job.Dataset != nil {
			job.Dataset.Format = cfg.Dataset.Format
			job.Dataset.ManifestSHA256 = cfg.Dataset.ManifestSHA256
		}
	} else if !errors.Is(err, model.ErrNotFound) {
		slog.Warn("config lookup failed", "id", id, "err", err)
	}
	job.Images = map[string]string{
		"trainer":          s.cfg.Settings.Images.Trainer,
		"progressReporter": s.cfg.Settings.Images.ProgressReporter,
	}
	return job, nil
}

// Events returns the job's delivery timeline, oldest first.
func (s *JobService) Events(ctx context.Context, id string) ([]model.JobEvent, error) {
	if !validJobID.MatchString(id) {
		return nil, invalid("invalid job id")
	}
	events, err := s.events.List(ctx, id)
	if err != nil {
		return nil, upstream("list events", err)
	}
	if events == nil {
		events = []model.JobEvent{}
	}
	return events, nil
}

// Files returns the job's code files and the manifest of the dataset version it used.
func (s *JobService) Files(ctx context.Context, id string) ([]model.File, error) {
	if !validJobID.MatchString(id) {
		return nil, invalid("invalid job id")
	}
	files := []model.File{}
	prefix := "code/" + id + "/"
	objs, err := s.objects.List(ctx, prefix)
	if err != nil {
		return nil, upstream("list files", err)
	}
	for _, o := range objs {
		files = append(files, model.File{Kind: "code", Name: strings.TrimPrefix(o.Key, prefix), Size: o.Size})
	}
	if ref, err := s.datasetOf(ctx, id); err == nil {
		if _, size, err := s.objects.Get(ctx, datasetPrefix(ref.ID, ref.Version)+manifestName, 1); err == nil {
			files = append(files, model.File{Kind: "data", Name: manifestName, Size: size})
		}
	}
	return files, nil
}

// File returns the first MaxPreviewBytes of one file of the job. The ID and name are checked so
// a request can never read another job's files or leave the job's prefix.
func (s *JobService) File(ctx context.Context, id, kind, name string) (model.FileContent, error) {
	if !validJobID.MatchString(id) || !safeName(name) {
		return model.FileContent{}, invalid("invalid file path")
	}
	switch {
	case kind == "code":
		return s.preview(ctx, "code/"+id+"/"+name)
	case kind == "data" && name == manifestName:
		ref, err := s.datasetOf(ctx, id)
		if err != nil {
			return model.FileContent{}, err
		}
		return s.preview(ctx, datasetPrefix(ref.ID, ref.Version)+manifestName)
	}
	return model.FileContent{}, invalid("invalid file path")
}

func (s *JobService) datasetOf(ctx context.Context, id string) (*model.DatasetRef, error) {
	statuses, err := s.sched.List(ctx)
	if err != nil {
		return nil, upstream("list jobs", err)
	}
	for _, st := range statuses {
		if st.ID == id && st.Dataset != nil {
			return st.Dataset, nil
		}
	}
	return nil, notFound("job not found")
}

func (s *JobService) preview(ctx context.Context, key string) (model.FileContent, error) {
	data, size, err := s.objects.Get(ctx, key, MaxPreviewBytes)
	if errors.Is(err, model.ErrNotFound) {
		return model.FileContent{}, notFound("file not found")
	}
	if err != nil {
		return model.FileContent{}, upstream("get file", err)
	}
	return model.FileContent{Data: data, Truncated: int64(len(data)) < size, TotalSize: size}, nil
}

// safeName accepts a plain file name: no path separators, so a request cannot leave its prefix.
func safeName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, `/\`)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
