package repository

import (
	"encoding/json"
	"testing"
	"time"
)

// Records as the three producers wrote them to job.lifecycle on 11/13 (job 0cb3b0c3).
func TestDecodeLifecycleProducers(t *testing.T) {
	cases := map[string]string{
		"backend": `{"event_id":"submitted-0cb3b0c3-g0001","schema_version":1,"type":"job.submitted",
			"occurred_at":"2026-11-13T00:14:03.855Z","source":"backend","job_id":"0cb3b0c3","generation":1,
			"payload":{"queue":"queue-light","gpus":2}}`,
		"job-watcher": `{"event_id":"job-0cb3b0c3-g0001-started","schema_version":1,"type":"job.started",
			"occurred_at":"2026-11-13T00:14:28.129Z","source":"job-watcher","cluster":"eks-a","node":null,
			"job_id":"0cb3b0c3","generation":1,"payload":{"time_source":"observed"}}`,
		"ckpt-sidecar": `{"event_id":"progress-0cb3b0c3-g0001-s00000019","schema_version":1,"type":"job.progress",
			"occurred_at":"2026-11-13T00:14:58.080Z","source":"ckpt-sidecar","cluster":"eks-a",
			"job_id":"0cb3b0c3","generation":1,"payload":{"step":19,"total_steps":208,"loss":3.25}}`,
	}
	for src, raw := range cases {
		e, err := DecodeLifecycle([]byte(raw))
		if err != nil {
			t.Fatalf("%s: %v", src, err)
		}
		if e.JobID != "0cb3b0c3" || e.Generation != 1 || e.Source != src || e.OccurredAt.Location() != time.UTC {
			t.Errorf("%s: %+v", src, e)
		}
	}
	e, _ := DecodeLifecycle([]byte(cases["ckpt-sidecar"]))
	if e.Cluster != "eks-a" || e.Payload["step"] != 19.0 {
		t.Errorf("payload %+v cluster %q", e.Payload, e.Cluster)
	}
}

func TestDecodeLifecycleRejects(t *testing.T) {
	bad := map[string]any{
		"not json":     "{",
		"no event_id":  map[string]any{"type": "job.started", "occurred_at": "2026-11-13T00:00:00Z"},
		"no time zone": map[string]any{"event_id": "x", "type": "job.started", "occurred_at": "2026-11-13T00:00:00"},
	}
	for name, v := range bad {
		raw, ok := v.(string)
		if !ok {
			b, _ := json.Marshal(v)
			raw = string(b)
		}
		if _, err := DecodeLifecycle([]byte(raw)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
