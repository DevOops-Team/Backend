// Package config loads the server settings from environment variables.
// Defaults match the prod infra (IaC environments/prod/data) and the training namespace.
package config

import "errors"

type Config struct {
	ListenAddr    string // LISTEN_ADDR
	AllowedOrigin string // ALLOWED_ORIGIN: the frontend, the only CORS origin
	Region        string // AWS_REGION
	Bucket        string // S3_BUCKET (required): the checkpoint bucket (ADR-14: one bucket for code, datasets, jobs)
	FencingTable  string // FENCING_TABLE: generation · owner · approved checkpoint (ADR-6)
	ProgressTable string // PROGRESS_TABLE: per-step progress the progress-reporter writes
	// KAFKA_BOOTSTRAP: brokers for job.lifecycle ("host:port,..."). Empty = no events (local runs).
	// Also passed to the ckpt-sidecar in the JobSet.
	KafkaBootstrap string
	KafkaTopic     string // KAFKA_TOPIC_LIFECYCLE
	Kubeconfig     string // KUBECONFIG: empty = default loading rules
	KubeContext    string // KUBE_CONTEXT: empty = current context
	Namespace      string // TRAINING_NAMESPACE: must match 50-management
	SettingsPath   string // BACKEND_SETTINGS: empty = embedded internal/settings/default.json
	// TRAINER_IMAGE (train.py, this repo's trainer/) and CHECKPOINT_IMAGE (ckpt-init · ckpt-sidecar, Data
	// repo checkpoint/) for the training JobSet. Gitops sets them and the deploy bot pins the digests,
	// so a new image needs no backend build. Empty = the settings file value.
	TrainerImage    string
	CheckpointImage string
}

// Load reads the configuration through getenv (os.Getenv in main, a map in tests).
func Load(getenv func(string) string) (Config, error) {
	get := func(key, fallback string) string {
		if v := getenv(key); v != "" {
			return v
		}
		return fallback
	}
	cfg := Config{
		ListenAddr:     get("LISTEN_ADDR", ":8080"),
		AllowedOrigin:  get("ALLOWED_ORIGIN", "http://localhost:5173"),
		Region:         get("AWS_REGION", "us-east-2"),
		Bucket:         getenv("S3_BUCKET"),
		FencingTable:   get("FENCING_TABLE", "devoops-prod-fencing"),
		ProgressTable:  get("PROGRESS_TABLE", "devoops-prod-job-progress"),
		KafkaBootstrap: getenv("KAFKA_BOOTSTRAP"),
		KafkaTopic:     get("KAFKA_TOPIC_LIFECYCLE", "job.lifecycle"),
		Kubeconfig:     getenv("KUBECONFIG"),
		KubeContext:    getenv("KUBE_CONTEXT"),
		Namespace:      get("TRAINING_NAMESPACE", "training"),
		SettingsPath:   getenv("BACKEND_SETTINGS"),

		TrainerImage:    getenv("TRAINER_IMAGE"),
		CheckpointImage: getenv("CHECKPOINT_IMAGE"),
	}
	if cfg.Bucket == "" {
		return Config{}, errors.New("S3_BUCKET is required")
	}
	return cfg, nil
}
