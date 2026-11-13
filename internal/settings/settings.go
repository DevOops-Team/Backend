// Package settings holds the static deployment values behind the JobSet template, the training
// options the screen offers and the checkpoint interval inputs (Confluence "JobSet 생성" §2,
// docs/superpowers/specs/2026-11-11-file-upload-submit-design.md). BACKEND_SETTINGS points at a
// file; without it the embedded default.json is used.
package settings

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
)

//go:embed default.json
var defaultSettings []byte

type Settings struct {
	ServiceAccount  string              `json:"service_account"` // EKS training/trainer: Pod Identity → trainer role
	Queues          map[string]Queue    `json:"queues"`
	PriorityClasses PriorityClasses     `json:"priority_classes"`
	Images          Images              `json:"images"`
	Pod             Pod                 `json:"pod"`
	Strategies      map[string]Strategy `json:"strategies"`
	Training        Training            `json:"training"`
	Checkpoint      Checkpoint          `json:"checkpoint"`
	Limits          Limits              `json:"limits"`
}

type Queue struct {
	Name string `json:"name"`
	GPUs []int  `json:"gpus"`
}

// PriorityClasses empty = no label: Kueue rejects a WorkloadPriorityClass that does not exist.
type PriorityClasses struct {
	Training string `json:"training"`
}

// Images: Trainer (train.py inside, this repo's trainer/) and Checkpoint (ckpt-init · ckpt-sidecar,
// Data repo checkpoint/) normally come from the TRAINER_IMAGE · CHECKPOINT_IMAGE env (WithImages),
// so the deploy bot can pin their digests without a backend build.
type Images struct {
	Trainer    string `json:"trainer,omitempty"`
	Checkpoint string `json:"checkpoint,omitempty"` // ckpt-init · ckpt-sidecar
}

type Pod struct {
	CPURequest              string `json:"cpu_request"`
	MemoryRequest           string `json:"memory_request"`
	MemoryLimit             string `json:"memory_limit"`
	ShmSize                 string `json:"shm_size"`
	DataSize                string `json:"data_size"` // /data emptyDir: model + data the init downloads
	CkptSize                string `json:"ckpt_size"` // /ckpt emptyDir
	TerminationGraceSeconds int64  `json:"termination_grace_seconds"`
	MasterPort              int64  `json:"master_port"`
	RunAsUser               int64  `json:"run_as_user"` // ckpt-agent image runs as 1000; trainer shares /ckpt with it
}

type Strategy struct {
	CheckpointFormat string `json:"checkpoint_format"`
	MinGPUs          int    `json:"min_gpus"`
}

// Training is what a submission may choose. Any Hugging Face model folder is accepted, so there are
// no per-model profiles: one set of defaults and ranges for every model.
type Training struct {
	DatasetFormat   string           `json:"dataset_format"`
	DefaultStrategy string           `json:"default_strategy"`
	GPUs            GPURange         `json:"gpus"`
	Defaults        Defaults         `json:"defaults"`
	Hyperparameters []Hyperparameter `json:"hyperparameters"`
}

// GPURange: Options are the counts the service offers. Max is what the current GPU quota allows,
// so raising the quota only means raising Max.
type GPURange struct {
	Min     int   `json:"min"`
	Max     int   `json:"max"`
	Default int   `json:"default"`
	Options []int `json:"options"`
}

// Defaults has every config.json training key a request may change.
type Defaults struct {
	Epochs          int64   `json:"epochs"`
	MaxSteps        *int64  `json:"max_steps"`
	BatchSizePerGPU int64   `json:"batch_size_per_gpu"`
	LearningRate    float64 `json:"learning_rate"`
	WarmupSteps     int64   `json:"warmup_steps"`
	Seed            int64   `json:"seed"`
}

type Hyperparameter struct {
	Key          string   `json:"key"`
	Level        string   `json:"level"`
	Type         string   `json:"type"` // integer | number
	Nullable     bool     `json:"nullable,omitempty"`
	Min          *float64 `json:"min,omitempty"`
	ExclusiveMin *float64 `json:"exclusive_min,omitempty"`
	Max          *float64 `json:"max,omitempty"`
}

// Checkpoint holds the Young/Daly inputs (design §5). The backend turns them into a time interval;
// train.py turns the interval into a step interval once it has measured its step time.
type Checkpoint struct {
	SizeFactor          float64 `json:"size_factor"`            // checkpoint bytes ÷ model folder bytes (fp32 weights + Adam moments ≈ 3)
	WriteBytesPerSecond float64 `json:"write_bytes_per_second"` // local NVMe DCP write; the sidecar uploads in parallel
	MTBFSeconds         float64 `json:"mtbf_seconds"`           // mean time between failures; unknown, so a fixed default
	MinIntervalSeconds  float64 `json:"min_interval_seconds"`
	WarmupMeasureSteps  int64   `json:"warmup_measure_steps"`
}

// Limits cap one submission's upload.
type Limits struct {
	MaxModelBytes int64 `json:"max_model_bytes"`
	MaxDataBytes  int64 `json:"max_data_bytes"`
	MaxFiles      int   `json:"max_files"`
}

// Load reads path, or the embedded default when path is empty. Unknown keys are errors so a typo
// cannot silently fall back to a zero value.
func Load(path string) (Settings, error) {
	raw := defaultSettings
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return Settings{}, fmt.Errorf("read settings: %w", err)
		}
		raw = b
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var s Settings
	if err := dec.Decode(&s); err != nil {
		return Settings{}, fmt.Errorf("parse settings: %w", err)
	}
	if err := s.check(); err != nil {
		return Settings{}, fmt.Errorf("settings: %w", err)
	}
	return s, nil
}

func (s Settings) check() error {
	var errs []error
	if _, ok := s.Strategies[s.Training.DefaultStrategy]; !ok {
		errs = append(errs, fmt.Errorf("training.default_strategy %q is not in strategies", s.Training.DefaultStrategy))
	}
	for name, st := range s.Strategies {
		if st.CheckpointFormat == "" {
			errs = append(errs, fmt.Errorf("strategy %s has no checkpoint_format", name))
		}
	}
	g := s.Training.GPUs
	if !slices.Contains(g.Options, g.Default) || g.Default > g.Max {
		errs = append(errs, fmt.Errorf("training.gpus.default %d must be an option within max %d", g.Default, g.Max))
	}
	c := s.Checkpoint
	if c.SizeFactor <= 0 || c.WriteBytesPerSecond <= 0 || c.MTBFSeconds <= 0 || c.MinIntervalSeconds <= 0 || c.WarmupMeasureSteps < 1 {
		errs = append(errs, errors.New("checkpoint values must be positive"))
	}
	l := s.Limits
	if l.MaxModelBytes <= 0 || l.MaxDataBytes <= 0 || l.MaxFiles <= 0 {
		errs = append(errs, errors.New("limits must be positive"))
	}
	return errors.Join(errs...)
}

// WithImages returns s with the trainer and checkpoint images set from the environment. An empty
// value keeps the settings file value, so local runs without the env behave as before.
func (s Settings) WithImages(trainer, checkpoint string) Settings {
	if trainer != "" {
		s.Images.Trainer = trainer
	}
	if checkpoint != "" {
		s.Images.Checkpoint = checkpoint
	}
	return s
}

// QueueFor returns the queue that takes this many GPUs.
func (s Settings) QueueFor(gpus int) (string, error) {
	for _, q := range s.Queues {
		if slices.Contains(q.GPUs, gpus) {
			return q.Name, nil
		}
	}
	return "", fmt.Errorf("no queue takes %d GPUs", gpus)
}
