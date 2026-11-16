package jobview

import (
	"testing"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

var t0 = time.Date(2026, 11, 13, 0, 14, 3, 855_000_000, time.UTC)

func ptr[T any](v T) *T { return &v }

// v_job_status.state (Data db/003) → the API status.
func TestFromStatusMapsTheDatabaseState(t *testing.T) {
	for state, want := range map[string]model.Status{
		"PENDING":   model.StatusQueued,
		"RUNNING":   model.StatusRunning,
		"COMPLETED": model.StatusSucceeded,
		"FAILED":    model.StatusFailed,
		"FENCED":    model.StatusRecovering,
		"STOPPED":   model.StatusCancelled,
		"CANCELLED": model.StatusCancelled,
	} {
		v := FromStatus(model.StatusRow{JobID: "0cb3b0c3", Generation: 1, State: state, SubmittedAt: &t0})
		if v.Status != want {
			t.Errorf("state %s → %s, want %s", state, v.Status, want)
		}
	}
}

func TestFromStatusLeavesAnUnknownStateEmpty(t *testing.T) {
	// Merge then keeps the management cluster's status instead of guessing.
	if v := FromStatus(model.StatusRow{JobID: "x", Generation: 1, State: "SOMETHING_NEW"}); v.Status != "" {
		t.Errorf("status = %s, want empty", v.Status)
	}
}

func TestFromStatusCarriesWorkerProgressAndApprovedCheckpoint(t *testing.T) {
	v := FromStatus(model.StatusRow{
		JobID: "0cb3b0c3", Generation: 2, State: "RUNNING", Cluster: ptr("eks-b"), RecoveryCount: 1,
		SubmittedAt: &t0, StartedAt: ptr(t0.Add(time.Minute)),
		ProgressStatus: ptr("RUNNING"), Step: ptr(int64(120)), TotalSteps: ptr(int64(208)), Loss: ptr(2.5),
		ProgressUpdatedAt: ptr(t0.Add(3 * time.Minute)),
		LastApprovedStep:  ptr(int64(100)), LastApprovedAt: ptr(t0.Add(2 * time.Minute)),
	})
	if v.Generation != 2 || v.Cluster != "eks-b" {
		t.Errorf("generation %d cluster %q", v.Generation, v.Cluster)
	}
	p := v.Progress
	if p == nil {
		t.Fatal("progress = nil")
	}
	if p.Status != "RUNNING" || p.Generation != 2 || p.Step != 120 || p.TotalSteps != 208 || p.Loss != 2.5 ||
		p.LastCheckpointStep != 100 || !p.UpdatedAt.Equal(t0.Add(3*time.Minute)) {
		t.Errorf("progress = %+v", *p)
	}
	if !v.UpdatedAt.Equal(t0.Add(3 * time.Minute)) {
		t.Errorf("updatedAt = %v, want the newest time in the row", v.UpdatedAt)
	}
}

func TestFromStatusShowsAnApprovedCheckpointBeforeAnyProgress(t *testing.T) {
	v := FromStatus(model.StatusRow{JobID: "x", Generation: 2, State: "PENDING", SubmittedAt: &t0,
		LastApprovedStep: ptr(int64(694)), LastApprovedAt: ptr(t0.Add(time.Minute))})
	if v.Progress == nil || v.Progress.LastCheckpointStep != 694 || v.Progress.Step != 0 {
		t.Errorf("progress = %+v, want only the last approved checkpoint (where recovery resumes)", v.Progress)
	}
}

func TestFromStatusHasNoProgressWithoutProgressOrCheckpoint(t *testing.T) {
	if v := FromStatus(model.StatusRow{JobID: "x", Generation: 1, State: "PENDING", SubmittedAt: &t0}); v.Progress != nil {
		t.Errorf("progress = %+v, want nil until train.py reports", v.Progress)
	}
}

// History turns job_runs and checkpoints into timeline rows, oldest first.
func TestHistoryListsEachGenerationAndCheckpoint(t *testing.T) {
	runs := []model.RunRecord{
		{Generation: 1, Cluster: ptr("eks-a"), Queue: ptr("queue-light"), SubmittedAt: &t0,
			StartedAt: ptr(t0.Add(time.Minute)), EndedAt: ptr(t0.Add(10 * time.Minute)), EndReason: ptr("fenced")},
		{Generation: 2, Cluster: ptr("eks-b"), StartedAt: ptr(t0.Add(12 * time.Minute)),
			EndedAt: ptr(t0.Add(30 * time.Minute)), EndReason: ptr("completed")},
	}
	ckpts := []model.CheckpointRecord{{Generation: 1, Step: 694, ApprovedAt: t0.Add(5 * time.Minute)}}

	got := History("0cb3b0c3", runs, ckpts)
	want := []struct {
		reason, cluster, typ, object string
	}{
		{"Submitted", "onprem", "Normal", "JobSet/job-0cb3b0c3-g0001"},
		{"Started", "eks-a", "Normal", "JobSet/job-0cb3b0c3-g0001"},
		{"CheckpointApproved", "eks-a", "Normal", "JobSet/job-0cb3b0c3-g0001"},
		{"Fenced", "eks-a", "Warning", "JobSet/job-0cb3b0c3-g0001"},
		{"Resumed", "eks-b", "Normal", "JobSet/job-0cb3b0c3-g0002"},
		{"Completed", "eks-b", "Normal", "JobSet/job-0cb3b0c3-g0002"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d entries %+v, want %d", len(got), got, len(want))
	}
	for i, w := range want {
		g := got[i]
		if g.Reason != w.reason || g.Cluster != w.cluster || g.Type != w.typ || g.Object != w.object || g.JobID != "0cb3b0c3" {
			t.Errorf("[%d] = %s %s %s %s, want %+v", i, g.Reason, g.Cluster, g.Type, g.Object, w)
		}
	}
	if got[2].Message != "checkpoint step 694 approved" {
		t.Errorf("checkpoint message = %q", got[2].Message)
	}
}

// The backend writes its own submission entry when it submits (service.report) with this key, so the
// database row and that entry must collapse into one.
func TestHistorySubmissionSharesTheBackendKey(t *testing.T) {
	got := History("0cb3b0c3", []model.RunRecord{{Generation: 1, SubmittedAt: &t0}}, nil)
	if len(got) != 1 || got[0].Key != "2026-11-13T00:14:03.855Z#onprem#submitted" {
		t.Errorf("entries = %+v", got)
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
