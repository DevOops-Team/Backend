package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
)

var t0 = time.Date(2026, 11, 16, 1, 0, 0, 0, time.UTC)

type fakeScheduler struct{ statuses []model.JobStatus }

func (f fakeScheduler) Submit(context.Context, *unstructured.Unstructured) error { return nil }
func (f fakeScheduler) List(context.Context) ([]model.JobStatus, error)          { return f.statuses, nil }

type fakeRecords struct {
	rows  []model.StatusRow
	runs  []model.RunRecord
	ckpts []model.CheckpointRecord
	final *model.FinalCheckpoint
	err   error
}

func (f fakeRecords) Statuses(context.Context, []string) ([]model.StatusRow, error) {
	return f.rows, f.err
}
func (f fakeRecords) History(context.Context, string) ([]model.RunRecord, []model.CheckpointRecord, error) {
	return f.runs, f.ckpts, f.err
}
func (f fakeRecords) FinalCheckpoint(context.Context, string) (*model.FinalCheckpoint, error) {
	return f.final, f.err
}

type fakeEvents struct{ events []model.JobEvent }

func (f fakeEvents) List(context.Context, string) ([]model.JobEvent, error) { return f.events, nil }

func service(sched Scheduler, rec JobRecords, ev EventReader) *JobService {
	return NewJobService(Config{}, Deps{Scheduler: sched, Records: rec, Events: ev})
}

func step(v int64) *int64 { return &v }

func TestListShowsTheDatabaseStatusAndProgress(t *testing.T) {
	sched := fakeScheduler{[]model.JobStatus{{ID: "0cb3b0c3", Generation: 1, Status: model.StatusAdmitted, CreatedAt: t0}}}
	started := t0.Add(time.Minute)
	cluster := "eks-a"
	rec := fakeRecords{rows: []model.StatusRow{{JobID: "0cb3b0c3", Generation: 1, State: "RUNNING", Cluster: &cluster,
		SubmittedAt: &t0, StartedAt: &started, Step: step(19), TotalSteps: step(208)}}}

	jobs, err := service(sched, rec, fakeEvents{}).List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	j := jobs[0]
	if j.Status != model.StatusRunning || j.Cluster != "eks-a" || j.Progress == nil || j.Progress.Step != 19 {
		t.Errorf("job = %+v progress %+v", j, j.Progress)
	}
}

// The database is only the progress source: when it is down the jobs still come from the cluster.
func TestListKeepsTheClusterStatusWhenTheDatabaseFails(t *testing.T) {
	sched := fakeScheduler{[]model.JobStatus{{ID: "0cb3b0c3", Generation: 1, Status: model.StatusAdmitted, CreatedAt: t0}}}
	jobs, err := service(sched, fakeRecords{err: errors.New("connection refused")}, fakeEvents{}).List(t.Context())
	if err != nil {
		t.Fatalf("err = %v, want the cluster's jobs", err)
	}
	if len(jobs) != 1 || jobs[0].Status != model.StatusAdmitted || jobs[0].Progress != nil {
		t.Errorf("jobs = %+v", jobs)
	}
}

func TestEventsMergesDatabaseHistoryWithClusterStepsWithoutDuplicates(t *testing.T) {
	ckpt := t0.Add(5 * time.Minute)
	cluster := "eks-a"
	rec := fakeRecords{
		runs:  []model.RunRecord{{Generation: 1, Cluster: &cluster, SubmittedAt: &t0}},
		ckpts: []model.CheckpointRecord{{Generation: 1, Step: 694, ApprovedAt: ckpt}},
	}
	// The backend's own submission entry has the same key as the database row; a Kueue step sits between.
	ev := fakeEvents{[]model.JobEvent{
		{JobID: "0cb3b0c3", Key: "2026-11-16T01:00:00.000Z#onprem#submitted", Time: t0, Reason: "Submitted"},
		{JobID: "0cb3b0c3", Key: "2026-11-16T01:00:02.000Z#onprem#k1", Time: t0.Add(2 * time.Second), Reason: "QuotaReserved"},
	}}

	got, err := service(fakeScheduler{}, rec, ev).Events(t.Context(), "0cb3b0c3")
	if err != nil {
		t.Fatal(err)
	}
	var reasons []string
	for _, e := range got {
		reasons = append(reasons, e.Reason)
	}
	want := []string{"Submitted", "QuotaReserved", "CheckpointApproved"}
	if len(reasons) != len(want) {
		t.Fatalf("reasons = %v, want %v", reasons, want)
	}
	for i := range want {
		if reasons[i] != want[i] {
			t.Fatalf("reasons = %v, want %v (oldest first, one submission)", reasons, want)
		}
	}
}

func TestEventsStillShowsClusterStepsWhenTheDatabaseFails(t *testing.T) {
	ev := fakeEvents{[]model.JobEvent{{JobID: "0cb3b0c3", Key: "2026-11-16T01:00:02.000Z#onprem#k1", Reason: "QuotaReserved"}}}
	got, err := service(fakeScheduler{}, fakeRecords{err: errors.New("down")}, ev).Events(t.Context(), "0cb3b0c3")
	if err != nil || len(got) != 1 {
		t.Errorf("events = %+v err %v", got, err)
	}
}
