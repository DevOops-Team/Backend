package model

// Model is one entry of GET /api/models. Files and URI are additions to the API 명세 so the screen
// can show the model actually stored in S3.
type Model struct {
	Name            string              `json:"name"`
	DisplayName     string              `json:"displayName"`
	Strategies      []string            `json:"strategies"`
	DefaultStrategy string              `json:"defaultStrategy"`
	StrategyMinGPUs map[string]int      `json:"strategyMinGpus"`
	GPUs            GPURange            `json:"gpus"`
	DatasetFormats  []string            `json:"datasetFormats"`
	Hyperparameters []HyperparameterDef `json:"hyperparameters"`
	URI             string              `json:"uri"`
	Files           []StoredFile        `json:"files"`
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
	Default      any      `json:"default"`
	Min          *float64 `json:"min,omitempty"`
	ExclusiveMin *float64 `json:"exclusiveMin,omitempty"`
	Max          *float64 `json:"max,omitempty"`
}

// StoredFile is one file of a model or a dataset version.
type StoredFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256,omitempty"`
}

// Dataset is GET /api/datasets/{id}; the list leaves Versions out.
//
// 임시 구성: 서비스 DB 없이 S3의 manifest.json에서 읽는다.
type Dataset struct {
	ID            string           `json:"id"`
	Name          string           `json:"name"`
	Format        string           `json:"format"`
	LatestVersion int              `json:"latestVersion"`
	Versions      []DatasetVersion `json:"versions,omitempty"`
}

type DatasetVersion struct {
	Version        int          `json:"version"`
	Source         string       `json:"source"`
	URI            string       `json:"uri"`
	FileCount      int          `json:"fileCount"`
	TotalBytes     int64        `json:"totalBytes"`
	ManifestSHA256 string       `json:"manifestSha256"`
	Files          []StoredFile `json:"files"`
}

// ItemList is the body of the list endpoints.
type ItemList[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"nextCursor"`
}
