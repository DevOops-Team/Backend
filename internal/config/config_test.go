package config

import "testing"

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadUsesDefaultsMatchingInfra(t *testing.T) {
	cfg, err := Load(env(map[string]string{"S3_BUCKET": "devoops-bucket"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		ListenAddr:    ":8080",
		AllowedOrigin: "http://localhost:5173",
		Region:        "us-east-2",
		Bucket:        "devoops-bucket",
		ProgressTable: "devoops-job-progress",
		EventsTable:   "devoops-job-events",
		Namespace:     "training",
	}
	if cfg != want {
		t.Errorf("cfg = %+v\nwant  %+v", cfg, want)
	}
}

func TestLoadReadsEveryVariable(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"LISTEN_ADDR":        ":9090",
		"ALLOWED_ORIGIN":     "https://app.example",
		"AWS_REGION":         "ap-northeast-2",
		"S3_BUCKET":          "b",
		"PROGRESS_TABLE":     "p",
		"EVENTS_TABLE":       "e",
		"KUBECONFIG":         "/kube/config",
		"KUBE_CONTEXT":       "k3d-devoops-mgmt",
		"TRAINING_NAMESPACE": "ns",
		"BACKEND_SETTINGS":   "/etc/settings.json",
		"CHECKPOINT_IMAGE":   "ghcr.io/devoops-team/checkpoint@sha256:abc",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		ListenAddr: ":9090", AllowedOrigin: "https://app.example", Region: "ap-northeast-2",
		Bucket: "b", ProgressTable: "p", EventsTable: "e",
		Kubeconfig: "/kube/config", KubeContext: "k3d-devoops-mgmt", Namespace: "ns", SettingsPath: "/etc/settings.json",
		CheckpointImage: "ghcr.io/devoops-team/checkpoint@sha256:abc",
	}
	if cfg != want {
		t.Errorf("cfg = %+v\nwant  %+v", cfg, want)
	}
}

func TestLoadRequiresBucket(t *testing.T) {
	if _, err := Load(env(nil)); err == nil || err.Error() != "S3_BUCKET is required" {
		t.Errorf("err = %v, want S3_BUCKET is required", err)
	}
}
