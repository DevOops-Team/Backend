package model

// TrainingOptions is GET /api/training/options: what the submit screen may choose and its limits.
// Any Hugging Face model folder is accepted, so there is one set of defaults and ranges.
type TrainingOptions struct {
	Strategies      []StrategyOption    `json:"strategies"`
	DefaultStrategy string              `json:"defaultStrategy"`
	GPUs            GPURange            `json:"gpus"`
	DatasetFormat   string              `json:"datasetFormat"`
	Hyperparameters []HyperparameterDef `json:"hyperparameters"`
	Limits          UploadLimits        `json:"limits"`
	Checkpoint      CheckpointDefaults  `json:"checkpoint"`
}

type StrategyOption struct {
	Name             string `json:"name"`
	MinGPUs          int    `json:"minGpus"`
	CheckpointFormat string `json:"checkpointFormat"`
}

type GPURange struct {
	Min     int   `json:"min"`
	Max     int   `json:"max"` // what the current GPU quota allows; options above it are shown as unavailable
	Default int   `json:"default"`
	Options []int `json:"options"`
}

type HyperparameterDef struct {
	Key          string   `json:"key"`
	Level        string   `json:"level"`
	Type         string   `json:"type"`
	Nullable     bool     `json:"nullable,omitempty"`
	Default      any      `json:"default"`
	Min          *float64 `json:"min,omitempty"`
	ExclusiveMin *float64 `json:"exclusiveMin,omitempty"`
	Max          *float64 `json:"max,omitempty"`
}

type UploadLimits struct {
	MaxModelBytes int64 `json:"maxModelBytes"`
	MaxDataBytes  int64 `json:"maxDataBytes"`
	MaxFiles      int   `json:"maxFiles"`
}

// CheckpointDefaults lets the screen preview the interval: τ = √(2 · C · M), C = model bytes ·
// sizeFactor / writeBytesPerSecond, at least minIntervalSeconds.
type CheckpointDefaults struct {
	SizeFactor          float64 `json:"sizeFactor"`
	WriteBytesPerSecond float64 `json:"writeBytesPerSecond"`
	MTBFSeconds         float64 `json:"mtbfSeconds"`
	MinIntervalSeconds  float64 `json:"minIntervalSeconds"`
}

// DatasetManifest is datasets/<id>/v1/manifest.json. ckpt-init downloads every file to /data/<name>
// and checks it (Data repo checkpoint/ckpt/init_main.py), so the shape is the checkpoint manifest's.
type DatasetManifest struct {
	SchemaVersion int             `json:"schema_version"`
	JobID         string          `json:"job_id"`
	Model         string          `json:"model"`
	Format        string          `json:"format"`
	Files         []ManifestEntry `json:"files"`
	ModelBytes    int64           `json:"model_bytes"`
	DataBytes     int64           `json:"data_bytes"`
	CreatedAt     string          `json:"created_at"`
}

type ManifestEntry struct {
	Name      string `json:"name"` // model/<path> or data/<file>
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
}

// ItemList is the body of the list endpoints.
type ItemList[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"nextCursor"`
}
