// Package model holds the request/response DTOs and domain types shared by every layer.
// It has no dependencies on the other internal packages. JSON shapes follow Confluence "서비스 API 명세":
// camelCase, except config.json keys, which stay snake_case everywhere.
package model

import (
	"encoding/json"
	"time"
)

// Phase is one generation's state derived from the JobSet and its Kueue Workload.
type Phase string

const (
	PhasePending   Phase = "Pending"   // waiting in the queue for a worker to admit it
	PhaseAdmitted  Phase = "Admitted"  // a worker reserved quota; pods are starting
	PhaseRunning   Phase = "Running"   // JobSet has active pods
	PhaseSucceeded Phase = "Succeeded" // JobSet completed
	PhaseFailed    Phase = "Failed"    // JobSet failed
)

// Status is the job state the screens show.
//
// 서비스 DB가 없어 Phase에서 바로 계산한다. RECOVERING · REJECTED · CANCELLED는 아직 없다.
type Status string

const (
	StatusQueued    Status = "QUEUED"
	StatusAdmitted  Status = "ADMITTED"
	StatusRunning   Status = "RUNNING"
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
)

// StatusOf maps a generation's phase to the job status.
func StatusOf(p Phase) Status {
	switch p {
	case PhaseAdmitted:
		return StatusAdmitted
	case PhaseRunning:
		return StatusRunning
	case PhaseSucceeded:
		return StatusSucceeded
	case PhaseFailed:
		return StatusFailed
	}
	return StatusQueued
}

// DatasetRef names one version of a dataset. Format and ManifestSHA256 are filled on responses.
type DatasetRef struct {
	ID             string `json:"id"`
	Version        int    `json:"version"`
	Format         string `json:"format,omitempty"`
	ManifestSHA256 string `json:"manifestSha256,omitempty"`
}

// JobStatus is what the management cluster knows about a job.
type JobStatus struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	Model           string         `json:"model"`
	Strategy        string         `json:"strategy"`
	GPUs            int            `json:"gpus"`
	Status          Status         `json:"status"`
	Message         string         `json:"statusMessage,omitempty"` // e.g. why it is still waiting
	Generation      int            `json:"generation"`
	JobsetName      string         `json:"jobsetName"`
	Queue           string         `json:"queue"`
	Cluster         string         `json:"cluster,omitempty"` // worker MultiKueue picked
	Phase           Phase          `json:"phase"`
	Dataset         *DatasetRef    `json:"dataset,omitempty"`
	Hyperparameters map[string]any `json:"hyperparameters,omitempty"` // what the user changed
	ConfigSHA256    string         `json:"configSha256,omitempty"`
	Restarts        int64          `json:"recoveryCount"`
	CreatedAt       time.Time      `json:"createdAt"`
}

// Progress is the item train.py (rank 0) overwrites every step, copied to DynamoDB by the sidecar.
type Progress struct {
	Status             string    `json:"status"`
	Generation         int64     `json:"generation"`
	Step               int64     `json:"step"`
	TotalSteps         int64     `json:"totalSteps"`
	Loss               float64   `json:"loss"`
	LastCheckpointStep int64     `json:"lastCheckpointStep"`
	NodeName           string    `json:"nodeName,omitempty"`
	UpdatedAt          time.Time `json:"updatedAt"`
}

// Job is the API view: queue state plus training progress (null until train.py first writes).
// Config and Images are only filled on GET /api/jobs/{id}.
type Job struct {
	JobStatus
	Progress  *Progress         `json:"progress"`
	UpdatedAt time.Time         `json:"updatedAt"`
	Config    json.RawMessage   `json:"config,omitempty"` // the final config.json
	Images    map[string]string `json:"images,omitempty"`
}

// SubmitSpec is the first part ("spec") of the multipart POST /api/jobs. The model folder and the
// training data follow as file parts. Omitted strategy and name get defaults; omitted
// hyperparameters keep the training defaults.
type SubmitSpec struct {
	Name            string         `json:"name"`
	Strategy        string         `json:"strategy"`
	GPUs            int            `json:"gpus"`
	Hyperparameters map[string]any `json:"hyperparameters"`
}

// TrainConfig is config.json. train.py rejects missing and unknown keys, so every key is always written.
// The model folder and the data files are in the dataset: /data/model and /data/data/*.jsonl in the pod.
type TrainConfig struct {
	SchemaVersion    int              `json:"schema_version"`
	Model            string           `json:"model"` // the uploaded folder's name, for people
	TrainingMode     string           `json:"training_mode"`
	CheckpointFormat string           `json:"checkpoint_format"`
	Dataset          ConfigDataset    `json:"dataset"`
	Epochs           int64            `json:"epochs"`
	MaxSteps         *int64           `json:"max_steps"`
	BatchSizePerGPU  int64            `json:"batch_size_per_gpu"`
	LearningRate     float64          `json:"learning_rate"`
	WarmupSteps      int64            `json:"warmup_steps"`
	Seed             int64            `json:"seed"`
	WorldSize        int              `json:"world_size"`
	CheckpointPolicy CheckpointPolicy `json:"checkpoint_policy"`
}

type ConfigDataset struct {
	URI            string `json:"uri"`
	Format         string `json:"format"`
	ManifestSHA256 string `json:"manifest_sha256"`
}

// CheckpointPolicy is how often to save (Young/Daly, design §5). train.py turns IntervalSeconds into a
// step interval after measuring WarmupMeasureSteps steps.
type CheckpointPolicy struct {
	IntervalSeconds      float64 `json:"interval_seconds"`
	EstimatedSaveSeconds float64 `json:"estimated_save_seconds"`
	MTBFSeconds          float64 `json:"mtbf_seconds"`
	WarmupMeasureSteps   int64   `json:"warmup_measure_steps"`
}

// CreatedJob is the response to a submission.
type CreatedJob struct {
	ID           string          `json:"id"`
	Name         string          `json:"name"`
	Status       Status          `json:"status"`
	Generation   int             `json:"generation"`
	JobsetName   string          `json:"jobsetName"`
	Queue        string          `json:"queue"`
	Config       json.RawMessage `json:"config"`
	ConfigSHA256 string          `json:"configSha256"`
	CreatedAt    time.Time       `json:"createdAt"`
}

// File is one stored file of a job: code (config.json, jobset.json, train.py) or data (manifest.json).
type File struct {
	Kind string `json:"kind"` // "code" | "data"
	Name string `json:"name"`
	Size int64  `json:"size"`
}

// FileContent is the start of one stored file; datasets can be GBs, so it may be truncated.
type FileContent struct {
	Data      []byte
	Truncated bool
	TotalSize int64
}

// FieldError says why one request field was rejected.
type FieldError struct {
	Field   string `json:"field"`
	Message string `json:"message"`
}

// ErrorResponse is the body of every error response.
type ErrorResponse struct {
	Error  string       `json:"error"`
	Code   string       `json:"code,omitempty"`
	Fields []FieldError `json:"fields,omitempty"`
}
