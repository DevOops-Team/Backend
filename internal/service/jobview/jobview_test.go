package jobview

import (
	"context"
	"testing"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

type timeline struct{ events []model.JobEvent }

func (t *timeline) Put(_ context.Context, e model.JobEvent) error {
	t.events = append(t.events, e)
	return nil
}

var t0 = time.Date(2026, 11, 13, 0, 14, 3, 855_000_000, time.UTC)

func ev(id, typ string, gen int, after time.Duration, payload map[string]any) model.LifecycleEvent {
	src := map[string]string{
		"job.submitted": "backend", "job.progress": "ckpt-sidecar", "checkpoint.approved": "ckpt-sidecar",
	}[typ]
	if src == "" {
		src = "job-watcher"
	}
	cluster := "eks-a"
	if typ == "job.submitted" {
		cluster = ""
	}
	return model.LifecycleEvent{
		EventID: id, Type: typ, OccurredAt: t0.Add(after), Source: src, Cluster: cluster,
		JobID: "0cb3b0c3", Generation: gen, Payload: payload,
	}
}

func view(t *testing.T, s *Store, id string) model.JobView {
	t.Helper()
	v, ok := s.Views([]string{id})[id]
	if !ok {
		t.Fatalf("no view for %s", id)
	}
	return v
}

// The real 11/13 sequence of job 0cb3b0c3, including placed arriving before admitted.
func TestStatusFollowsTheLifecycle(t *testing.T) {
	s := New(nil)
	steps := []struct {
		e    model.LifecycleEvent
		want model.Status
	}{
		{ev("submitted-0cb3b0c3-g0001", "job.submitted", 1, 0, nil), model.StatusQueued},
		{ev("job-0cb3b0c3-g0001-placed", "job.placed", 1, 7*time.Second, nil), model.StatusAdmitted},
		{ev("job-0cb3b0c3-g0001-admitted", "job.admitted", 1, 7*time.Second, nil), model.StatusAdmitted},
		{ev("job-0cb3b0c3-g0001-started", "job.started", 1, 25*time.Second, nil), model.StatusRunning},
		{ev("job-0cb3b0c3-g0001-end", "job.completed", 1, 5*time.Minute, nil), model.StatusSucceeded},
	}
	for _, st := range steps {
		s.Apply(context.Background(), st.e)
		if got := view(t, s, "0cb3b0c3").Status; got != st.want {
			t.Fatalf("after %s: status %s, want %s", st.e.Type, got, st.want)
		}
	}
	v := view(t, s, "0cb3b0c3")
	if v.Cluster != "eks-a" || v.Generation != 1 {
		t.Errorf("cluster %q generation %d", v.Cluster, v.Generation)
	}
	if !v.UpdatedAt.Equal(t0.Add(5 * time.Minute)) {
		t.Errorf("updatedAt %v", v.UpdatedAt)
	}
}

func TestStatusNeverMovesBackwardsWithinAGeneration(t *testing.T) {
	s := New(nil)
	s.Apply(context.Background(), ev("job-x-g0001-end", "job.completed", 1, time.Minute, nil))
	s.Apply(context.Background(), ev("job-x-g0001-started", "job.started", 1, 0, nil))
	if got := view(t, s, "0cb3b0c3").Status; got != model.StatusSucceeded {
		t.Errorf("status %s, want SUCCEEDED", got)
	}
}

func TestEndEvents(t *testing.T) {
	cases := []struct {
		typ     string
		payload map[string]any
		want    model.Status
		message string
	}{
		{"job.failed", map[string]any{"message": "BackoffLimitExceeded"}, model.StatusFailed, "BackoffLimitExceeded"},
		{"job.stopped", nil, model.StatusCancelled, ""},
		{"job.fenced", map[string]any{"request_id": "rr-1", "action": "MIGRATE"}, model.StatusRecovering, "relocation rr-1 (MIGRATE)"},
	}
	for _, c := range cases {
		s := New(nil)
		s.Apply(context.Background(), ev("job-x-g0001-started", "job.started", 1, 0, nil))
		s.Apply(context.Background(), ev("job-x-g0001-end", c.typ, 1, time.Minute, c.payload))
		v := view(t, s, "0cb3b0c3")
		if v.Status != c.want || v.Message != c.message {
			t.Errorf("%s: %s %q, want %s %q", c.typ, v.Status, v.Message, c.want, c.message)
		}
	}
}

// A relocation resubmits the job as generation 2: the view moves on and starts over.
func TestNewGenerationReplacesTheOldOne(t *testing.T) {
	s := New(nil)
	s.Apply(context.Background(), ev("job-x-g0001-end", "job.fenced", 1, 0, nil))
	s.Apply(context.Background(), ev("p1", "job.progress", 1, 0, map[string]any{"step": 500.0, "total_steps": 2080.0}))
	s.Apply(context.Background(), ev("job-x-g0002-submitted", "job.submitted", 2, time.Minute, nil))
	v := view(t, s, "0cb3b0c3")
	if v.Generation != 2 || v.Status != model.StatusQueued || v.Progress != nil {
		t.Fatalf("got gen %d status %s progress %v", v.Generation, v.Status, v.Progress)
	}
	// late events of generation 1 no longer change the view
	s.Apply(context.Background(), ev("job-x-g0001-late", "job.started", 1, 2*time.Minute, nil))
	if v := view(t, s, "0cb3b0c3"); v.Status != model.StatusQueued {
		t.Errorf("old generation moved status to %s", v.Status)
	}
}

func TestProgressAndCheckpoint(t *testing.T) {
	s := New(nil)
	s.Apply(context.Background(), ev("p19", "job.progress", 1, 55*time.Second, map[string]any{
		"status": "RUNNING", "step": 19.0, "total_steps": 208.0, "loss": 3.25, "last_checkpoint_step": 0.0,
	}))
	// an older value that arrives late is ignored
	s.Apply(context.Background(), ev("p0", "job.progress", 1, 24*time.Second, map[string]any{"step": 0.0, "total_steps": 208.0}))
	s.Apply(context.Background(), ev("ck69", "checkpoint.approved", 1, 2*time.Minute, map[string]any{"step": 69.0}))

	p := view(t, s, "0cb3b0c3").Progress
	if p == nil {
		t.Fatal("no progress")
	}
	if p.Step != 19 || p.TotalSteps != 208 || p.Loss != 3.25 || p.Status != "RUNNING" || p.Generation != 1 {
		t.Errorf("progress %+v", *p)
	}
	if p.LastCheckpointStep != 69 {
		t.Errorf("lastCheckpointStep %d, want 69 from checkpoint.approved", p.LastCheckpointStep)
	}
	if !p.UpdatedAt.Equal(t0.Add(55 * time.Second)) {
		t.Errorf("updatedAt %v", p.UpdatedAt)
	}
	if v := view(t, s, "0cb3b0c3"); v.Status != "" {
		t.Errorf("progress alone set status %s", v.Status)
	}
}

func TestDuplicateEventIDsAreIgnored(t *testing.T) {
	tl := &timeline{}
	s := New(tl)
	e := ev("job-0cb3b0c3-g0001-started", "job.started", 1, 0, nil)
	if !s.Apply(context.Background(), e) || s.Apply(context.Background(), e) {
		t.Fatal("first apply should count, the replay should not")
	}
	if len(tl.events) != 1 {
		t.Errorf("timeline got %d entries", len(tl.events))
	}
}

func TestTimelineEntries(t *testing.T) {
	tl := &timeline{}
	s := New(tl)
	s.Apply(context.Background(), ev("submitted-0cb3b0c3-g0001", "job.submitted", 1, 0, nil))
	s.Apply(context.Background(), ev("job-0cb3b0c3-g0001-placed", "job.placed", 1, 7*time.Second, nil))
	s.Apply(context.Background(), ev("p1", "job.progress", 1, 30*time.Second, map[string]any{"step": 1.0}))
	s.Apply(context.Background(), ev("ck", "checkpoint.approved", 1, time.Minute, map[string]any{"step": 69.0}))
	s.Apply(context.Background(), ev("job-0cb3b0c3-g0001-end", "job.failed", 1, 2*time.Minute, map[string]any{"message": "oom"}))

	if len(tl.events) != 4 {
		t.Fatalf("got %d timeline entries, want 4 (job.progress stays out)", len(tl.events))
	}
	sub := tl.events[0]
	// same key as the backend's own submitted record, so the two merge into one entry
	if sub.Key != "2026-11-13T00:14:03.855Z#onprem#submitted" || sub.Cluster != model.ManagerCluster {
		t.Errorf("submitted entry %+v", sub)
	}
	placed := tl.events[1]
	if placed.Reason != "Placed" || placed.Cluster != "eks-a" || placed.Source != "job-watcher" ||
		placed.Object != "JobSet/job-0cb3b0c3-g0001" || placed.Type != "Normal" {
		t.Errorf("placed entry %+v", placed)
	}
	if ck := tl.events[2]; ck.Reason != "CheckpointApproved" || ck.Message != "checkpoint step 69 approved" {
		t.Errorf("checkpoint entry %+v", ck)
	}
	if f := tl.events[3]; f.Reason != "Failed" || f.Type != "Warning" || f.Message != "oom" {
		t.Errorf("failed entry %+v", f)
	}
}

func TestUnknownJobHasNoView(t *testing.T) {
	if _, ok := New(nil).Views([]string{"nope"})["nope"]; ok {
		t.Error("view for a job never seen")
	}
}

func TestMerge(t *testing.T) {
	st := model.JobStatus{ID: "0cb3b0c3", Name: "gpt2", Generation: 1, Status: model.StatusAdmitted,
		Message: "waiting for pods", CreatedAt: t0}
	running := model.JobView{Generation: 1, Status: model.StatusRunning, Cluster: "eks-a",
		Progress: &model.Progress{Step: 19, TotalSteps: 208}, UpdatedAt: t0.Add(time.Minute)}

	j := Merge(st, running, true)
	if j.Status != model.StatusRunning || j.Cluster != "eks-a" || j.Progress.Step != 19 || j.Name != "gpt2" {
		t.Errorf("view not applied: %+v", j)
	}
	if !j.UpdatedAt.Equal(t0.Add(time.Minute)) {
		t.Errorf("updatedAt %v", j.UpdatedAt)
	}

	// no view: the cluster's state as before
	if j := Merge(st, model.JobView{}, false); j.Status != model.StatusAdmitted || j.Progress != nil {
		t.Errorf("without view: %+v", j)
	}

	// the cluster is already further than the view: keep the cluster's status
	done := st
	done.Status = model.StatusSucceeded
	if j := Merge(done, running, true); j.Status != model.StatusSucceeded {
		t.Errorf("status went back to %s", j.Status)
	}

	// only progress so far: status from the cluster
	if j := Merge(st, model.JobView{Generation: 1, Progress: running.Progress}, true); j.Status != model.StatusAdmitted {
		t.Errorf("status %s", j.Status)
	}

	// a stale view of an older generation is ignored
	gen2 := st
	gen2.Generation = 2
	if j := Merge(gen2, running, true); j.Status != model.StatusAdmitted || j.Progress != nil {
		t.Errorf("old generation applied: %+v", j)
	}

	// fenced: the view's reason replaces the cluster's message
	fenced := model.JobView{Generation: 1, Status: model.StatusRecovering, Message: "relocation rr-1 (MIGRATE)"}
	if j := Merge(st, fenced, true); j.Status != model.StatusRecovering || j.Message != "relocation rr-1 (MIGRATE)" {
		t.Errorf("fenced: %+v", j)
	}
}
