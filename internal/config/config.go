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
	// KAFKA_BOOTSTRAP: brokers for job.lifecycle ("host:port,..."): the backend publishes job.submitted.
	// Empty = no events (local runs). Also passed to the ckpt-sidecar in the JobSet.
	KafkaBootstrap string
	KafkaTopic     string // KAFKA_TOPIC_LIFECYCLE
	// PGHOST: the data database (PostgreSQL gpuf_data) the screens read status · progress · timeline from,
	// as backend_ro. The other libpq variables (PGPORT · PGDATABASE · PGUSER · PGPASSWORD · PGSSLMODE)
	// are read by the driver. Empty = cluster state only (local runs).
	DatabaseHost string
	Kubeconfig   string // KUBECONFIG: empty = default loading rules
	KubeContext  string // KUBE_CONTEXT: empty = current context
	Namespace    string // TRAINING_NAMESPACE: must match 50-management
	SettingsPath string // BACKEND_SETTINGS: empty = embedded internal/settings/default.json
	// TRAINER_IMAGE (training environment, Data repo trainer/; train.py is uploaded per job) and
	// CHECKPOINT_IMAGE (ckpt-init · ckpt-sidecar, Data repo checkpoint/) for the training JobSet. Gitops sets them and the deploy bot pins the digests,
	// so a new image needs no backend build. Empty = the settings file value.
	TrainerImage    string
	CheckpointImage string
	// RECOVERY_API_TOKEN: bearer token of the internal recovery API (GPU-119), from a Secret. Empty = that
	// API answers 503 NOT_CONFIGURED; the rest of the server works as before.
	RecoveryAPIToken string
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
		KafkaBootstrap: getenv("KAFKA_BOOTSTRAP"),
		KafkaTopic:     get("KAFKA_TOPIC_LIFECYCLE", "job.lifecycle"),
		DatabaseHost:   getenv("PGHOST"),
		Kubeconfig:     getenv("KUBECONFIG"),
		KubeContext:    getenv("KUBE_CONTEXT"),
		Namespace:      get("TRAINING_NAMESPACE", "training"),
		SettingsPath:   getenv("BACKEND_SETTINGS"),

		TrainerImage:    getenv("TRAINER_IMAGE"),
		CheckpointImage: getenv("CHECKPOINT_IMAGE"),

		RecoveryAPIToken: getenv("RECOVERY_API_TOKEN"),
	}
	if cfg.Bucket == "" {
		return Config{}, errors.New("S3_BUCKET is required")
	}
	return cfg, nil
}
