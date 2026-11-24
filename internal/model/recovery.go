package model

import "errors"

// RecoverySchemaVersion is the schema_version of RecoveryMetadata (K8s recovery-controller
// docs/backend-metadata-api.md, contract v1).
const RecoverySchemaVersion = 1

// ErrPlacementUnavailable is returned by a placement source that has no trusted placement of the job's
// generation (not connected yet, or the workers have not reported it). The API answers 503.
var ErrPlacementUnavailable = errors.New("placement not available")

// RecoveryMetadata is what the recovery controller reads about one failed generation of a job before it
// registers the job for recovery (GPU-119): GET /internal/v1/recovery/jobs/{id}?generation=N.
// Every number is an integer and every time is Unix seconds.
type RecoveryMetadata struct {
	SchemaVersion int    `json:"schema_version"`
	JobID         string `json:"job_id"`
	Generation    int    `json:"generation"` // the requested (failed) generation
	// RecoveryEligible is false for a job the user stopped or that finished; a failed job stays eligible.
	RecoveryEligible bool `json:"recovery_eligible"`
	// JobsetURI is the submission original s3://<bucket>/code/<id>/jobset.json, JobsetSHA256 the hash of its
	// bytes recorded at submission (not a hash of the object as it is now).
	JobsetURI           string   `json:"jobset_uri"`
	JobsetSHA256        string   `json:"jobset_sha256"`
	GPUCount            int      `json:"gpu_count"`
	Cluster             string   `json:"cluster"`
	ActiveNodes         []string `json:"active_nodes"`
	PlacementGeneration int      `json:"placement_generation"`
	JobsetName          string   `json:"jobset_name"` // management-cluster JobSet of the generation
	JobsetUID           string   `json:"jobset_uid"`  // its UID on the management cluster, not a worker copy
	ObservedAt          int64    `json:"observed_at"` // when the placement was observed, not the response time
	// AllowRestartFromScratch is optional in the contract (absent = true); the backend always sends it.
	AllowRestartFromScratch bool `json:"allow_restart_from_scratch"`
}

// Placement is where a generation of a job actually runs: the worker cluster and every node of it, as a
// trusted placement record observed them.
type Placement struct {
	Cluster    string
	Nodes      []string
	Generation int   // the generation the placement belongs to
	ObservedAt int64 // Unix seconds
}
