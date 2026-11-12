package repository

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

func TestSubmittedEnvelopeMatchesTheCollector(t *testing.T) {
	key, body, err := submittedRecord(model.SubmittedEvent{
		JobID: "3f9a1c2e", Generation: 1, JobsetName: "job-3f9a1c2e-g0001", GPUs: 2, Strategy: "fsdp",
		Queue: "queue-light", ConfigSHA256: "abc", ImageDigest: "sha256:def",
		OccurredAt: time.Date(2026, 11, 12, 1, 2, 3, 456789000, time.FixedZone("KST", 9*3600)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if key != "3f9a1c2e" {
		t.Errorf("key = %q, want the job id (one partition per job keeps order)", key)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	// Data collector/envelope.py: event_id · schema_version · type · occurred_at 필수, 시각은 UTC Z · 밀리초
	want := map[string]any{
		"event_id": "submitted-3f9a1c2e-g0001", "schema_version": 1.0, "type": "job.submitted",
		"occurred_at": "2026-11-11T16:02:03.456Z", "source": "backend",
		"job_id": "3f9a1c2e", "generation": 1.0,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	p, _ := got["payload"].(map[string]any)
	wantP := map[string]any{"jobset_name": "job-3f9a1c2e-g0001", "gpus": 2.0, "strategy": "fsdp", "queue": "queue-light",
		"config_sha256": "abc", "image_digest": "sha256:def"}
	for k, v := range wantP {
		if p[k] != v {
			t.Errorf("payload.%s = %v, want %v", k, p[k], v)
		}
	}
}

func TestLifecycleWithoutBrokersIsANoOp(t *testing.T) {
	l, err := NewLifecycle("", "job.lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Submitted(t.Context(), model.SubmittedEvent{JobID: "x", Generation: 1}); err != nil {
		t.Errorf("err = %v, want nil when Kafka is not configured", err)
	}
}

func TestSubmittedEnvelopeLeavesClusterOut(t *testing.T) {
	_, body, _ := submittedRecord(model.SubmittedEvent{JobID: "x", Generation: 1})
	var got map[string]any
	_ = json.Unmarshal(body, &got)
	if _, ok := got["cluster"]; ok {
		t.Error("cluster is the worker that runs the job (collector job_runs.cluster); the backend does not know it yet")
	}
}
