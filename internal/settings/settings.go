// Package settings holds the static deployment values behind the JobSet template and the model
// profiles (Confluence "JobSet 생성" §2, backend-settings.json). BACKEND_SETTINGS points at a file;
// without it the embedded default.json is used.
//
// 임시 구성: default.json은 FSDP 연동 테스트용이다. 합의안의 큐 · 우선순위 · ServiceAccount · 이미지가
// 준비되면 이 파일의 값만 바꾼다.
package settings

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"slices"
)

//go:embed default.json
var defaultSettings []byte

type Settings struct {
	ServiceAccount  string              `json:"service_account"` // empty = namespace default; the infra has no trainer SA yet
	Queues          map[string]Queue    `json:"queues"`
	PriorityClasses PriorityClasses     `json:"priority_classes"`
	Images          Images              `json:"images"`
	Pod             Pod                 `json:"pod"`
	Strategies      map[string]Strategy `json:"strategies"`
	ModelProfiles   map[string]Profile  `json:"model_profiles"`
	TrainScriptKey  string              `json:"train_script_key"` // S3 key copied to code/<id>/train.py
}

type Queue struct {
	Name string `json:"name"`
	GPUs []int  `json:"gpus"`
}

// PriorityClasses empty = no label: Kueue rejects a WorkloadPriorityClass that does not exist.
type PriorityClasses struct {
	Training string `json:"training"`
}

type Images struct {
	Trainer          string `json:"trainer"`
	ProgressReporter string `json:"progress_reporter"`
}

type Pod struct {
	CPURequest              string `json:"cpu_request"`
	MemoryRequest           string `json:"memory_request"`
	MemoryLimit             string `json:"memory_limit"`
	ShmSize                 string `json:"shm_size"`
	CkptLocalSize           string `json:"ckpt_local_size"`
	TerminationGraceSeconds int64  `json:"termination_grace_seconds"`
	MasterPort              int64  `json:"master_port"`
}

type Strategy struct {
	CheckpointFormat string `json:"checkpoint_format"`
	MinGPUs          int    `json:"min_gpus"`
}

// Profile is one model the service offers. Defaults has every config.json training key; only the
// keys in Hyperparameters can be changed by a request.
type Profile struct {
	DisplayName     string           `json:"display_name"`
	ModelSubPath    string           `json:"model_subpath"` // under the datasets/ prefix, where the infra role can read
	DatasetFormats  []string         `json:"dataset_formats"`
	Strategies      []string         `json:"strategies"`
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

type Defaults struct {
	Epochs               int64   `json:"epochs"`
	MaxSteps             *int64  `json:"max_steps"`
	BatchSizePerGPU      int64   `json:"batch_size_per_gpu"`
	LearningRate         float64 `json:"learning_rate"`
	WarmupSteps          int64   `json:"warmup_steps"`
	CheckpointEverySteps *int64  `json:"checkpoint_every_steps"`
	Seed                 int64   `json:"seed"`
}

type Hyperparameter struct {
	Key          string   `json:"key"`
	Level        string   `json:"level"`
	Type         string   `json:"type"` // integer | number
	Min          *float64 `json:"min,omitempty"`
	ExclusiveMin *float64 `json:"exclusive_min,omitempty"`
	Max          *float64 `json:"max,omitempty"`
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
	for name, p := range s.ModelProfiles {
		for _, st := range p.Strategies {
			if s.Strategies[st].CheckpointFormat == "" {
				return Settings{}, fmt.Errorf("model %s: strategy %q has no checkpoint_format", name, st)
			}
		}
		if !slices.Contains(p.Strategies, p.DefaultStrategy) {
			return Settings{}, fmt.Errorf("model %s: default_strategy %q is not in strategies", name, p.DefaultStrategy)
		}
	}
	return s, nil
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
