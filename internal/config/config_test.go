package config

import "testing"

func env(vars map[string]string) func(string) string {
	return func(k string) string { return vars[k] }
}

func TestLoadUsesDefaultsMatchingProdInfra(t *testing.T) {
	cfg, err := Load(env(map[string]string{"S3_BUCKET": "devoops-bucket"}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		ListenAddr:    ":8080",
		AllowedOrigin: "http://localhost:5173",
		Region:        "us-east-2",
		Bucket:        "devoops-bucket",
		FencingTable:  "devoops-prod-fencing",
		ProgressTable: "devoops-prod-job-progress",
		KafkaTopic:    "job.lifecycle",
		Namespace:     "training",
	}
	if cfg != want {
		t.Errorf("cfg = %+v\nwant  %+v", cfg, want)
	}
}

func TestLoadReadsEveryVariable(t *testing.T) {
	cfg, err := Load(env(map[string]string{
		"LISTEN_ADDR":           ":9090",
		"ALLOWED_ORIGIN":        "https://app.example",
		"AWS_REGION":            "ap-northeast-2",
		"S3_BUCKET":             "b",
		"FENCING_TABLE":         "f",
		"PROGRESS_TABLE":        "p",
		"KAFKA_BOOTSTRAP":       "172.30.0.40:9092",
		"KAFKA_TOPIC_LIFECYCLE": "t",
		"KUBECONFIG":            "/kube/config",
		"KUBE_CONTEXT":          "ctx",
		"TRAINING_NAMESPACE":    "ns",
		"BACKEND_SETTINGS":      "/etc/settings.json",
		"TRAINER_IMAGE":         "ghcr.io/devoops-team/trainer@sha256:t",
		"CHECKPOINT_IMAGE":      "ghcr.io/devoops-team/checkpoint@sha256:c",
	}))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		ListenAddr: ":9090", AllowedOrigin: "https://app.example", Region: "ap-northeast-2", Bucket: "b",
		FencingTable: "f", ProgressTable: "p", KafkaBootstrap: "172.30.0.40:9092", KafkaTopic: "t",
		Kubeconfig: "/kube/config", KubeContext: "ctx", Namespace: "ns", SettingsPath: "/etc/settings.json",
		TrainerImage: "ghcr.io/devoops-team/trainer@sha256:t", CheckpointImage: "ghcr.io/devoops-team/checkpoint@sha256:c",
	}
	if cfg != want {
		t.Errorf("cfg = %+v\nwant  %+v", cfg, want)
	}
}

func TestLoadRequiresBucket(t *testing.T) {
	if _, err := Load(env(nil)); err == nil {
		t.Error("loaded without S3_BUCKET")
	}
}
