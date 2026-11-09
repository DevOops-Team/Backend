// Package config loads the server settings from environment variables.
// Defaults match the infra outputs (20-data tables, 50-management namespace).
package config

import "errors"

type Config struct {
	ListenAddr    string // LISTEN_ADDR
	AllowedOrigin string // ALLOWED_ORIGIN: the frontend, the only CORS origin
	Region        string // AWS_REGION
	Bucket        string // S3_BUCKET (required): 20-data bucket_name
	ProgressTable string // PROGRESS_TABLE: 20-data progress_table_name
	EventsTable   string // EVENTS_TABLE: 20-data events_table_name
	Kubeconfig    string // KUBECONFIG: empty = default loading rules
	KubeContext   string // KUBE_CONTEXT: empty = current context
	Namespace     string // TRAINING_NAMESPACE: must match 50-management
	SettingsPath  string // BACKEND_SETTINGS: empty = embedded internal/settings/default.json
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
		ListenAddr:    get("LISTEN_ADDR", ":8080"),
		AllowedOrigin: get("ALLOWED_ORIGIN", "http://localhost:5173"),
		Region:        get("AWS_REGION", "us-east-2"),
		Bucket:        getenv("S3_BUCKET"),
		ProgressTable: get("PROGRESS_TABLE", "devoops-job-progress"),
		EventsTable:   get("EVENTS_TABLE", "devoops-job-events"),
		Kubeconfig:    getenv("KUBECONFIG"),
		KubeContext:   getenv("KUBE_CONTEXT"),
		Namespace:     get("TRAINING_NAMESPACE", "training"),
		SettingsPath:  getenv("BACKEND_SETTINGS"),
	}
	if cfg.Bucket == "" {
		return Config{}, errors.New("S3_BUCKET is required")
	}
	return cfg, nil
}
