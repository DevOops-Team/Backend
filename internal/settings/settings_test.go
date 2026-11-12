package settings

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultSettingsDescribeTheEKSATestCluster(t *testing.T) {
	s, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	tr := s.Training
	if tr.DefaultStrategy != "fsdp" || tr.GPUs.Default != 2 || tr.GPUs.Max != 2 {
		t.Errorf("training = %+v, want fsdp on the two GPU nodes eks-a has", tr)
	}
	if got := tr.GPUs.Options; len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 4 {
		t.Errorf("gpu options = %v, want 1, 2, 4", got)
	}
	if tr.DatasetFormat != "text-jsonl" {
		t.Errorf("dataset format = %q", tr.DatasetFormat)
	}
	if s.Strategies["fsdp"].MinGPUs != 2 || s.Strategies["ddp"].MinGPUs != 1 {
		t.Errorf("min gpus = %+v", s.Strategies)
	}
	if s.Strategies["ddp"].CheckpointFormat != "ddp-pt" || s.Strategies["fsdp"].CheckpointFormat != "fsdp-dcp" {
		t.Errorf("formats = %+v", s.Strategies)
	}
	if q, err := s.QueueFor(2); err != nil || q != "queue-light" {
		t.Errorf("QueueFor(2) = %q, %v; 2 GPUs go to the light queue", q, err)
	}
	if s.ServiceAccount != "trainer" || s.PriorityClasses.Training != "training" {
		t.Errorf("sa = %q, priority = %q; the agreed template uses trainer and training", s.ServiceAccount, s.PriorityClasses.Training)
	}
	if s.Checkpoint.MTBFSeconds != 6*3600 {
		t.Errorf("mtbf = %v, want the 6h default", s.Checkpoint.MTBFSeconds)
	}
	if s.Limits.MaxModelBytes != 2<<30 || s.Limits.MaxDataBytes != 1<<30 {
		t.Errorf("limits = %+v", s.Limits)
	}
	if s.Images.Trainer != "" || s.Images.Checkpoint != "" {
		t.Errorf("images = %+v; trainer and checkpoint come from env (Gitops), not the embedded file", s.Images)
	}
}

func TestQueueForPicksByGPUCount(t *testing.T) {
	s := Settings{Queues: map[string]Queue{
		"light": {Name: "queue-light", GPUs: []int{1, 2}},
		"heavy": {Name: "queue-heavy", GPUs: []int{3, 4}},
	}}
	if q, _ := s.QueueFor(4); q != "queue-heavy" {
		t.Errorf("QueueFor(4) = %q", q)
	}
	if _, err := s.QueueFor(9); err == nil {
		t.Error("QueueFor(9) should fail: no queue takes 9 GPUs")
	}
}

func TestLoadReadsFileAndRejectsUnknownKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	if err := os.WriteFile(path, []byte(`{"typo_key": 1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "typo_key") {
		t.Errorf("err = %v, want unknown key error", err)
	}
}

func TestLoadRejectsInconsistentValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	body := `{"strategies":{"fsdp":{}},"training":{"default_strategy":"ddp","gpus":{"max":1,"default":2,"options":[2]}}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	for _, want := range []string{"default_strategy", "checkpoint_format", "gpus.default", "checkpoint values", "limits"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v, want it to mention %s", err, want)
		}
	}
}

func TestWithImagesOverridesOnlyWhenSet(t *testing.T) {
	base := Settings{Images: Images{Trainer: "t-file", Checkpoint: "c-file", ProgressReporter: "p"}}

	if got := base.WithImages("", ""); got.Images != base.Images {
		t.Errorf("empty env: images = %+v, want the settings file values", got.Images)
	}
	got := base.WithImages("ghcr.io/devoops-team/trainer@sha256:t", "ghcr.io/devoops-team/checkpoint@sha256:c")
	if got.Images.Trainer != "ghcr.io/devoops-team/trainer@sha256:t" || got.Images.Checkpoint != "ghcr.io/devoops-team/checkpoint@sha256:c" || got.Images.ProgressReporter != "p" {
		t.Errorf("images = %+v", got.Images)
	}
	if base.Images.Trainer != "t-file" {
		t.Error("WithImages changed the receiver")
	}
}
