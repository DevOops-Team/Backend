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
	"hash"
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
	"github.com/DevOops-Team/Backend/internal/service/ckptpolicy"
	"github.com/DevOops-Team/Backend/internal/service/jobspec"
	"github.com/DevOops-Team/Backend/internal/service/jobview"
	"github.com/DevOops-Team/Backend/internal/settings"
)

// ObjectStore stores job inputs (S3).
type ObjectStore interface {
	Put(ctx context.Context, key string, body io.Reader) error
	List(ctx context.Context, prefix string) ([]model.ObjectInfo, error)
	// Get returns at most limit bytes of the object and its full size, or model.ErrNotFound.
	Get(ctx context.Context, key string, limit int64) ([]byte, int64, error)
}

// Scheduler submits JobSets to the management cluster and reads their queue state (Kueue).
type Scheduler interface {
	// Submit returns model.ErrAlreadyExists when the JobSet name is taken.
	Submit(ctx context.Context, js *unstructured.Unstructured) error
	List(ctx context.Context) ([]model.JobStatus, error)
}

// JobViews is what Kafka job.lifecycle says about each job: status, worker, progress, last checkpoint
// (service/jobview, GPU-146).
type JobViews interface {
	Views(ids []string) map[string]model.JobView
}

// Fencing creates the job's first fencing item (DynamoDB); model.ErrAlreadyExists if it exists.
type Fencing interface {
	Create(ctx context.Context, jobID string) error
}

// Lifecycle reports the backend's job events to Kafka job.lifecycle.
type Lifecycle interface {
	Submitted(ctx context.Context, e model.SubmittedEvent) error
}

// EventReader reads a job's delivery timeline, oldest first.
type EventReader interface {
	List(ctx context.Context, jobID string) ([]model.JobEvent, error)
}

// EventWriter records what the backend itself did, such as submitting a job.
type EventWriter interface {
	Put(ctx context.Context, e model.JobEvent) error
}

// Upload is one file part of a submission: Field "model" (Path inside the folder, starting with the
// folder's name), "data" (a .jsonl file name) or "train" (the user's training script, a .py file name).
type Upload struct {
	Field string
	Path  string
	Body  io.Reader
}

// UploadSource yields the file parts in order; io.EOF after the last one.
type UploadSource interface {
	Next() (Upload, error)
}

const (
	// MaxPreviewBytes caps how much of one file the viewer gets; datasets can be GBs.
	MaxPreviewBytes = 64 << 10

	maxMetadataBytes = 4 << 20 // config files and manifests are small
	firstGeneration  = 1       // 복구 재제출은 재배치 실행기가 jobset.json으로 한다
	datasetVersion   = 1       // 업로드는 제출마다 새 데이터셋: datasets/<job id>/v1
	configSchema     = 2       // 2: checkpoint_policy replaces checkpoint_every_steps; model · data in the dataset
	manifestSchema   = 1
	manifestName     = "manifest.json"
	modelConfigName  = "model/config.json" // every Hugging Face model folder has it

	// The user's train.py rides in the dataset so ckpt-init downloads it (sha256 checked) to
	// /data/code/train.py with the model and data; a copy under code/<id>/ is what the file viewer shows.
	trainName     = "code/train.py"
	trainCodeName = "train.py"
	MaxTrainBytes = 1 << 20 // a script, not a model
)

var (
	validJobID = regexp.MustCompile(`^[0-9a-z]+$`)
	validName  = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	nameChars  = regexp.MustCompile(`[^a-z0-9-]+`)
)

type Config struct {
	Namespace      string
	Region         string
	Bucket         string
	FencingTable   string
	KafkaBootstrap string
	KafkaTopic     string
	Settings       settings.Settings
	NewID          func() string
	Now            func() time.Time // nil = time.Now
}

type JobService struct {
	cfg       Config
	objects   ObjectStore
	sched     Scheduler
	views     JobViews
	fencing   Fencing
	lifecycle Lifecycle
	events    EventReader
	record    EventWriter
}

// Deps are the repositories the service uses.
type Deps struct {
	Objects   ObjectStore
	Scheduler Scheduler
	Views     JobViews
	Fencing   Fencing
	Lifecycle Lifecycle
	Events    EventReader
	Record    EventWriter
}

func NewJobService(cfg Config, d Deps) *JobService {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &JobService{cfg: cfg, objects: d.Objects, sched: d.Scheduler, views: d.Views, fencing: d.Fencing,
		lifecycle: d.Lifecycle, events: d.Events, record: d.Record}
}

// Options returns what the submit screen may choose.
func (s *JobService) Options() model.TrainingOptions {
	st := s.cfg.Settings
	tr := st.Training
	names := make([]string, 0, len(st.Strategies))
	for n := range st.Strategies {
		names = append(names, n)
	}
	sort.Strings(names)
	strategies := make([]model.StrategyOption, 0, len(names))
	for _, n := range names {
		strategies = append(strategies, model.StrategyOption{Name: n, MinGPUs: st.Strategies[n].MinGPUs, CheckpointFormat: st.Strategies[n].CheckpointFormat})
	}
	defaults := map[string]any{
		"epochs": tr.Defaults.Epochs, "max_steps": tr.Defaults.MaxSteps, "batch_size_per_gpu": tr.Defaults.BatchSizePerGPU,
		"learning_rate": tr.Defaults.LearningRate, "warmup_steps": tr.Defaults.WarmupSteps, "seed": tr.Defaults.Seed,
	}
	hyper := make([]model.HyperparameterDef, 0, len(tr.Hyperparameters))
	for _, h := range tr.Hyperparameters {
		hyper = append(hyper, model.HyperparameterDef{Key: h.Key, Level: h.Level, Type: h.Type, Nullable: h.Nullable,
			Default: defaults[h.Key], Min: h.Min, ExclusiveMin: h.ExclusiveMin, Max: h.Max})
	}
	return model.TrainingOptions{
		Strategies:      strategies,
		DefaultStrategy: tr.DefaultStrategy,
		GPUs:            model.GPURange(tr.GPUs),
		DatasetFormat:   tr.DatasetFormat,
		Hyperparameters: hyper,
		Limits: model.UploadLimits{MaxModelBytes: st.Limits.MaxModelBytes, MaxDataBytes: st.Limits.MaxDataBytes,
			MaxTrainBytes: MaxTrainBytes, MaxFiles: st.Limits.MaxFiles},
		Checkpoint: model.CheckpointDefaults{SizeFactor: st.Checkpoint.SizeFactor, WriteBytesPerSecond: st.Checkpoint.WriteBytesPerSecond,
			MTBFSeconds: st.Checkpoint.MTBFSeconds, MinIntervalSeconds: st.Checkpoint.MinIntervalSeconds},
	}
}

// Submit validates the spec, streams the model folder, the data files and train.py to S3, writes the dataset
// manifest, config.json and jobset.json, creates the fencing item and the JobSet, and reports
// job.submitted. The spec is checked before any upload, so a rejected request writes nothing; a
// failure later leaves the uploaded objects (the backend may not delete) but never submits a JobSet.
func (s *JobService) Submit(ctx context.Context, spec model.SubmitSpec, src UploadSource) (model.CreatedJob, error) {
	strategy, merged, err := s.validate(spec)
	if err != nil {
		return model.CreatedJob{}, err
	}
	queue, err := s.cfg.Settings.QueueFor(spec.GPUs)
	if err != nil {
		return model.CreatedJob{}, invalidFields([]model.FieldError{{Field: "gpus", Message: err.Error()}})
	}

	id := s.cfg.NewID()
	prefix := datasetPrefix(id, datasetVersion)
	up, err := s.receive(ctx, prefix, src)
	if err != nil {
		return model.CreatedJob{}, err
	}

	manifest := model.DatasetManifest{
		SchemaVersion: manifestSchema, JobID: id, Model: up.modelName, Format: s.cfg.Settings.Training.DatasetFormat,
		Files: up.entries, ModelBytes: up.modelBytes, DataBytes: up.dataBytes,
		CreatedAt: s.cfg.Now().UTC().Format(time.RFC3339),
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return model.CreatedJob{}, fmt.Errorf("encode manifest: %w", err)
	}
	if err := s.objects.Put(ctx, prefix+manifestName, bytes.NewReader(manifestBytes)); err != nil {
		return model.CreatedJob{}, upstream("upload "+manifestName, err)
	}
	manifestSHA := sha256Hex(manifestBytes)

	name := spec.Name
	if name == "" {
		name = defaultName(up.modelName, id)
	}
	checkpointFormat := s.cfg.Settings.Strategies[strategy].CheckpointFormat
	configBytes, err := json.MarshalIndent(model.TrainConfig{
		SchemaVersion:    configSchema,
		Model:            up.modelName,
		TrainingMode:     strategy,
		CheckpointFormat: checkpointFormat,
		Dataset: model.ConfigDataset{
			URI:            fmt.Sprintf("s3://%s/%s", s.cfg.Bucket, prefix),
			Format:         s.cfg.Settings.Training.DatasetFormat,
			ManifestSHA256: manifestSHA,
		},
		Epochs:           merged.Epochs,
		MaxSteps:         merged.MaxSteps,
		BatchSizePerGPU:  merged.BatchSizePerGPU,
		LearningRate:     merged.LearningRate,
		WarmupSteps:      merged.WarmupSteps,
		Seed:             merged.Seed,
		WorldSize:        spec.GPUs,
		CheckpointPolicy: ckptpolicy.Compute(up.modelBytes, s.cfg.Settings.Checkpoint),
	}, "", "  ")
	if err != nil {
		return model.CreatedJob{}, fmt.Errorf("encode config: %w", err)
	}
	configSHA := sha256Hex(configBytes)

	js, err := jobspec.Render(jobspec.Input{
		JobID: id, Generation: firstGeneration, Name: name, Model: up.modelName, Strategy: strategy, GPUs: spec.GPUs,
		DatasetID: id, DatasetVersion: datasetVersion, DatasetManifestSHA256: manifestSHA, ConfigSHA256: configSHA,
		CheckpointFormat: checkpointFormat, Hyperparameters: spec.Hyperparameters, Queue: queue,
		Namespace: s.cfg.Namespace, Region: s.cfg.Region, Bucket: s.cfg.Bucket, FencingTable: s.cfg.FencingTable,
		KafkaBootstrap: s.cfg.KafkaBootstrap, KafkaTopic: s.cfg.KafkaTopic,
		Settings: s.cfg.Settings,
	})
	if err != nil {
		return model.CreatedJob{}, fmt.Errorf("render JobSet: %w", err)
	}
	jobsetBytes, err := json.MarshalIndent(js.Object, "", "  ")
	if err != nil {
		return model.CreatedJob{}, fmt.Errorf("encode JobSet: %w", err)
	}

	// Everything the pods read must be in S3 before the JobSet exists. jobset.json is the recovery original.
	for _, f := range []struct {
		name string
		body []byte
	}{{trainCodeName, up.train}, {"config.json", configBytes}, {"jobset.json", jobsetBytes}} {
		if err := s.objects.Put(ctx, "code/"+id+"/"+f.name, bytes.NewReader(f.body)); err != nil {
			return model.CreatedJob{}, upstream("upload "+f.name, err)
		}
	}

	if err := s.fencing.Create(ctx, id); err != nil {
		if errors.Is(err, model.ErrAlreadyExists) {
			return model.CreatedJob{}, &Error{Kind: KindConflict, Msg: "job " + id + " already has a fencing item", Err: err}
		}
		return model.CreatedJob{}, upstream("create fencing item", err)
	}

	if err := s.sched.Submit(ctx, js); err != nil {
		if errors.Is(err, model.ErrAlreadyExists) {
			return model.CreatedJob{}, &Error{Kind: KindConflict, Msg: "JobSet " + js.GetName() + " already exists", Err: err}
		}
		return model.CreatedJob{}, &Error{Kind: KindUpstream, Code: "SUBMIT_FAILED", Msg: "submit JobSet failed: " + err.Error(), Err: err}
	}
	now := s.cfg.Now().UTC()
	s.report(ctx, model.SubmittedEvent{
		JobID: id, Generation: firstGeneration, JobsetName: js.GetName(), GPUs: spec.GPUs, Strategy: strategy,
		Queue: queue, ConfigSHA256: configSHA, ImageDigest: digestOf(s.cfg.Settings.Images.Trainer), OccurredAt: now,
	})

	slog.Info("job submitted", "id", id, "name", js.GetName(), "model", up.modelName, "strategy", strategy,
		"gpus", spec.GPUs, "modelBytes", up.modelBytes, "dataBytes", up.dataBytes)
	return model.CreatedJob{
		ID: id, Name: name, Status: model.StatusQueued, Generation: firstGeneration, JobsetName: js.GetName(),
		Queue: queue, Config: configBytes, ConfigSHA256: configSHA, CreatedAt: now,
	}, nil
}

// received is what the file parts of one submission turned into.
type received struct {
	modelName             string
	entries               []model.ManifestEntry
	modelBytes, dataBytes int64
	train                 []byte // train.py, also stored in the dataset as code/train.py
}

// receive streams each part to S3 under prefix while hashing it, and enforces the limits.
func (s *JobService) receive(ctx context.Context, prefix string, src UploadSource) (received, error) {
	lim := s.cfg.Settings.Limits
	var r received
	seen := map[string]bool{}
	for {
		u, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return received{}, invalid("read upload: " + err.Error())
		}
		if len(r.entries) >= lim.MaxFiles {
			return received{}, tooLarge(fmt.Sprintf("too many files: at most %d", lim.MaxFiles))
		}

		var name string
		var budget int64
		switch u.Field {
		case "model":
			folder, rest, ok := splitModelPath(u.Path)
			if !ok {
				return received{}, invalid("model file path must be <folder>/<file>: " + u.Path)
			}
			if r.modelName == "" {
				r.modelName = folder
			} else if folder != r.modelName {
				return received{}, invalid("model files come from more than one folder: " + r.modelName + ", " + folder)
			}
			name, budget = "model/"+rest, lim.MaxModelBytes-r.modelBytes
		case "data":
			if !safeName(u.Path) || !strings.HasSuffix(u.Path, ".jsonl") {
				return received{}, invalid("training data must be .jsonl files: " + u.Path)
			}
			name, budget = "data/"+u.Path, lim.MaxDataBytes-r.dataBytes
		case "train":
			if !safeName(u.Path) || !strings.HasSuffix(u.Path, ".py") {
				return received{}, invalid("training code must be one .py file: " + u.Path)
			}
			if seen[trainName] {
				return received{}, invalid("only one training code file (train.py) can be uploaded")
			}
			name, budget = trainName, MaxTrainBytes
		default:
			return received{}, invalid("unknown upload field: " + u.Field)
		}
		if seen[name] {
			return received{}, invalid("duplicate file: " + name)
		}
		seen[name] = true

		var copied bytes.Buffer // train.py is kept for its code/<id>/ copy; it is at most MaxTrainBytes
		src := u.Body
		if u.Field == "train" {
			src = io.TeeReader(u.Body, &copied)
		}
		body := &meteredReader{r: src, left: budget, hash: sha256.New()}
		if err := s.objects.Put(ctx, prefix+name, body); err != nil {
			if body.over {
				return received{}, tooLarge(fmt.Sprintf("%s upload is over the limit (%d bytes)", u.Field, map[string]int64{"model": lim.MaxModelBytes, "data": lim.MaxDataBytes, "train": MaxTrainBytes}[u.Field]))
			}
			if body.readErr != nil {
				return received{}, invalid("read upload " + name + ": " + body.readErr.Error())
			}
			return received{}, upstream("upload "+name, err)
		}
		r.entries = append(r.entries, model.ManifestEntry{Name: name, SHA256: hex.EncodeToString(body.hash.Sum(nil)), SizeBytes: body.n})
		switch u.Field {
		case "model":
			r.modelBytes += body.n
		case "data":
			r.dataBytes += body.n
		case "train":
			r.train = copied.Bytes()
		}
	}

	var fields []model.FieldError
	if !seen[modelConfigName] {
		fields = append(fields, model.FieldError{Field: "model", Message: "Hugging Face 모델 폴더(config.json 포함)가 필요합니다"})
	}
	if !slices.ContainsFunc(r.entries, func(e model.ManifestEntry) bool { return strings.HasPrefix(e.Name, "data/") }) {
		fields = append(fields, model.FieldError{Field: "data", Message: ".jsonl 학습 데이터가 1개 이상 필요합니다"})
	}
	if !seen[trainName] {
		fields = append(fields, model.FieldError{Field: "train", Message: "학습 코드(train.py)가 필요합니다"})
	} else if len(r.train) == 0 {
		fields = append(fields, model.FieldError{Field: "train", Message: "학습 코드(train.py)가 비어 있습니다"})
	}
	if len(fields) > 0 {
		return received{}, invalidFields(fields)
	}
	sort.Slice(r.entries, func(a, b int) bool { return r.entries[a].Name < r.entries[b].Name })
	return r, nil
}

// meteredReader hashes and counts what it reads and fails once more than left bytes come through.
type meteredReader struct {
	r       io.Reader
	left    int64
	n       int64
	hash    hash.Hash
	over    bool
	readErr error
}

var errOverLimit = errors.New("upload over the limit")

func (m *meteredReader) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	if n > 0 {
		m.n += int64(n)
		_, _ = m.hash.Write(p[:n])
		if m.n > m.left {
			m.over = true
			return n, errOverLimit
		}
	}
	if err != nil && !errors.Is(err, io.EOF) {
		m.readErr = err
	}
	return n, err
}

// splitModelPath splits "gpt2/tokenizer/vocab.json" into "gpt2" and "tokenizer/vocab.json", rejecting
// anything that could leave the job's prefix.
func splitModelPath(p string) (string, string, bool) {
	parts := strings.Split(p, "/")
	if len(parts) < 2 {
		return "", "", false
	}
	for _, part := range parts {
		if !safeName(part) {
			return "", "", false
		}
	}
	return parts[0], strings.Join(parts[1:], "/"), true
}

// defaultName is "<model folder>-<id>" made Kubernetes-safe, e.g. GPT-2 small → gpt-2-small-3f9a1c2e.
func defaultName(modelName, id string) string {
	base := strings.Trim(nameChars.ReplaceAllString(strings.ToLower(modelName), "-"), "-")
	if len(base) > 50 {
		base = strings.TrimRight(base[:50], "-")
	}
	if base == "" {
		return "job-" + id
	}
	return base + "-" + id
}

// validate checks the spec (no I/O) and returns the strategy and the merged training values.
func (s *JobService) validate(spec model.SubmitSpec) (string, settings.Defaults, error) {
	var fields []model.FieldError
	bad := func(field, msg string) { fields = append(fields, model.FieldError{Field: field, Message: msg}) }
	tr := s.cfg.Settings.Training

	if spec.Name != "" && !validName.MatchString(spec.Name) {
		bad("name", "1~63자, 소문자 · 숫자 · '-'만 쓸 수 있습니다")
	}
	strategy := spec.Strategy
	if strategy == "" {
		strategy = tr.DefaultStrategy
	}
	st, ok := s.cfg.Settings.Strategies[strategy]
	if !ok {
		names := make([]string, 0, len(s.cfg.Settings.Strategies))
		for n := range s.cfg.Settings.Strategies {
			names = append(names, n)
		}
		sort.Strings(names)
		bad("strategy", "지원 방식: "+strings.Join(names, ", "))
	} else if msg := gpuProblem(tr.GPUs, st, strategy, spec.GPUs); msg != "" {
		bad("gpus", msg)
	}
	merged, hfields := mergeHyperparameters(tr, spec.Hyperparameters)
	fields = append(fields, hfields...)
	if len(fields) > 0 {
		return "", settings.Defaults{}, invalidFields(fields)
	}
	return strategy, merged, nil
}

// gpuProblem says why this GPU count cannot run, or "" when it can.
func gpuProblem(g settings.GPURange, st settings.Strategy, strategy string, gpus int) string {
	switch {
	case !slices.Contains(g.Options, gpus):
		return fmt.Sprintf("선택 가능한 GPU 수: %v", g.Options)
	case gpus > g.Max:
		return fmt.Sprintf("현재 GPU 할당량으로는 최대 %d개까지 쓸 수 있습니다", g.Max)
	case gpus < st.MinGPUs:
		return fmt.Sprintf("%s는 GPU %d개 이상이 필요합니다", strings.ToUpper(strategy), st.MinGPUs)
	}
	return ""
}

// mergeHyperparameters overlays the request's values on the training defaults.
func mergeHyperparameters(tr settings.Training, in map[string]any) (settings.Defaults, []model.FieldError) {
	merged := tr.Defaults
	var fields []model.FieldError
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		field := "hyperparameters." + key
		i := slices.IndexFunc(tr.Hyperparameters, func(h settings.Hyperparameter) bool { return h.Key == key })
		if i < 0 {
			fields = append(fields, model.FieldError{Field: field, Message: "변경할 수 없는 값입니다"})
			continue
		}
		def := tr.Hyperparameters[i]
		if in[key] == nil {
			if !def.Nullable {
				fields = append(fields, model.FieldError{Field: field, Message: "값이 필요합니다"})
			} else if key == "max_steps" {
				merged.MaxSteps = nil
			}
			continue
		}
		v, ok := in[key].(float64)
		switch {
		case !ok:
			fields = append(fields, model.FieldError{Field: field, Message: "숫자여야 합니다"})
			continue
		case def.Type == "integer" && v != math.Trunc(v):
			fields = append(fields, model.FieldError{Field: field, Message: "정수여야 합니다"})
			continue
		case def.Min != nil && v < *def.Min, def.Max != nil && v > *def.Max, def.ExclusiveMin != nil && v <= *def.ExclusiveMin:
			fields = append(fields, model.FieldError{Field: field, Message: "범위를 벗어났습니다"})
			continue
		}
		switch key {
		case "epochs":
			merged.Epochs = int64(v)
		case "max_steps":
			n := int64(v)
			merged.MaxSteps = &n
		case "batch_size_per_gpu":
			merged.BatchSizePerGPU = int64(v)
		case "learning_rate":
			merged.LearningRate = v
		case "warmup_steps":
			merged.WarmupSteps = int64(v)
		case "seed":
			merged.Seed = int64(v)
		default:
			fields = append(fields, model.FieldError{Field: field, Message: "변경할 수 없는 값입니다"})
		}
	}
	return merged, fields
}

// report sends job.submitted to Kafka and writes the first timeline step. Best-effort: the job is
// already queued, so a failure only loses the record.
func (s *JobService) report(ctx context.Context, e model.SubmittedEvent) {
	if err := s.lifecycle.Submitted(ctx, e); err != nil {
		slog.Warn("publish job.submitted", "id", e.JobID, "err", err)
	}
	t := e.OccurredAt.Truncate(time.Millisecond)
	err := s.record.Put(ctx, model.JobEvent{
		JobID: e.JobID, Key: t.Format(model.EventKeyTimeFormat) + "#" + model.ManagerCluster + "#submitted",
		Time: t, LastTime: t, Cluster: model.ManagerCluster, Source: "backend", Reason: "Submitted",
		Message: "model and data stored in S3; JobSet " + e.JobsetName + " created in queue " + e.Queue,
		Type:    "Normal", Object: "JobSet/" + e.JobsetName, Count: 1,
	})
	if err != nil {
		slog.Warn("record submitted event", "id", e.JobID, "err", err)
	}
}

// List returns the jobs on the management cluster with their Kafka view (status · progress), newest first.
func (s *JobService) List(ctx context.Context) ([]model.Job, error) {
	statuses, err := s.sched.List(ctx)
	if err != nil {
		return nil, upstream("list jobs", err)
	}
	ids := make([]string, len(statuses))
	for i, st := range statuses {
		ids[i] = st.ID
	}
	views := s.views.Views(ids)

	jobs := make([]model.Job, len(statuses))
	for i, st := range statuses {
		v, ok := views[st.ID]
		jobs[i] = jobview.Merge(st, v, ok)
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
	images := s.cfg.Settings.Images
	job.Images = map[string]string{
		"trainer":    images.Trainer,
		"checkpoint": images.Checkpoint,
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

// Files returns the job's code files and the manifest of the files it uploaded.
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

func datasetPrefix(id string, version int) string {
	return fmt.Sprintf("datasets/%s/v%d/", id, version)
}

// safeName accepts a plain file name: no path separators, so a request cannot leave its prefix.
func safeName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.ContainsAny(name, `/\`)
}

// digestOf returns the sha256:… part of a pinned image, or "".
func digestOf(image string) string {
	if _, d, ok := strings.Cut(image, "@"); ok {
		return d
	}
	return ""
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
