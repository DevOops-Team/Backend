package settings

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultSettingsDescribeTheFSDPTest(t *testing.T) {
	s, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	gpt2, ok := s.ModelProfiles["gpt2"]
	if !ok {
		t.Fatal("gpt2 profile missing")
	}
	if gpt2.DefaultStrategy != "fsdp" || gpt2.GPUs.Default != 2 || gpt2.GPUs.Max != 2 {
		t.Errorf("gpt2 = %+v, want fsdp on the two GPU nodes the infra has", gpt2)
	}
	if got := strings.Join(gpt2.Strategies, ","); got != "fsdp,ddp" {
		t.Errorf("strategies = %s", got)
	}
	if got := gpt2.GPUs.Options; len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 4 {
		t.Errorf("gpu options = %v, want 1, 2, 4", got)
	}
	if s.Strategies["fsdp"].MinGPUs != 2 || s.Strategies["ddp"].MinGPUs != 1 {
		t.Errorf("min gpus = %+v", s.Strategies)
	}
	if s.Strategies["ddp"].CheckpointFormat != "ddp-pt" {
		t.Errorf("ddp format = %q", s.Strategies["ddp"].CheckpointFormat)
	}
	if s.Strategies["fsdp"].CheckpointFormat != "fsdp-dcp" {
		t.Errorf("fsdp format = %q", s.Strategies["fsdp"].CheckpointFormat)
	}
	if q, err := s.QueueFor(2); err != nil || q != "queue-light" {
		t.Errorf("QueueFor(2) = %q, %v; 2 GPUs go to the light queue", q, err)
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

func TestLoadRejectsProfileWhoseStrategyHasNoFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.json")
	body := `{"model_profiles":{"m":{"strategies":["fsdp"],"default_strategy":"fsdp"}},"strategies":{}}`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "fsdp") {
		t.Errorf("err = %v, want missing strategy error", err)
	}
}

func TestWithCheckpointImageOverridesOnlyWhenSet(t *testing.T) {
	base := Settings{Images: Images{Trainer: "trainer", Checkpoint: "from-file"}}

	if got := base.WithCheckpointImage(""); got.Images.Checkpoint != "from-file" {
		t.Errorf("empty env: checkpoint = %q, want the settings file value", got.Images.Checkpoint)
	}
	got := base.WithCheckpointImage("ghcr.io/devoops-team/checkpoint@sha256:abc")
	if got.Images.Checkpoint != "ghcr.io/devoops-team/checkpoint@sha256:abc" || got.Images.Trainer != "trainer" {
		t.Errorf("images = %+v, want env checkpoint and the trainer untouched", got.Images)
	}
	if base.Images.Checkpoint != "from-file" {
		t.Error("WithCheckpointImage changed the receiver")
	}
}

func TestDefaultSettingsHaveNoCheckpointImage(t *testing.T) {
	s, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if s.Images.Checkpoint != "" {
		t.Errorf("default checkpoint = %q; it comes from CHECKPOINT_IMAGE (Gitops), not the embedded file", s.Images.Checkpoint)
	}
}
