package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

// stoppingScheduler lists fixed jobs and records stop requests; stopped is what Stop reports back.
type stoppingScheduler struct {
	fakeScheduler
	calls   *[]string
	at      *time.Time
	stopped []string
	err     error
}

func (f stoppingScheduler) Stop(_ context.Context, id string, at time.Time) ([]string, error) {
	*f.calls = append(*f.calls, id)
	*f.at = at
	return f.stopped, f.err
}

type recordedEvents struct{ got *[]model.JobEvent }

func (f recordedEvents) Put(_ context.Context, e model.JobEvent) error {
	*f.got = append(*f.got, e)
	return nil
}

// listedObjects is a bucket that also answers List by prefix.
type listedObjects struct {
	fakeObjects
	lists *int
}

func (f listedObjects) List(_ context.Context, prefix string) ([]model.ObjectInfo, error) {
	*f.lists++
	var out []model.ObjectInfo
	for k := range f.fakeObjects {
		if strings.HasPrefix(k, prefix) {
			out = append(out, model.ObjectInfo{Key: k})
		}
	}
	return out, nil
}

var stopAt = time.Date(2026, 11, 18, 2, 0, 0, 0, time.UTC)

func stopService(status model.Status, stopped []string) (*JobService, *[]string, *time.Time, *[]model.JobEvent) {
	var calls []string
	var at time.Time
	var events []model.JobEvent
	sched := stoppingScheduler{fakeScheduler{[]model.JobStatus{{ID: modelJob, Generation: 2, Status: status, CreatedAt: t0}}},
		&calls, &at, stopped, nil}
	s := NewJobService(Config{Now: func() time.Time { return stopAt }},
		Deps{Scheduler: sched, Records: fakeRecords{}, Events: fakeEvents{}, Objects: fakeObjects{}, Record: recordedEvents{&events}})
	return s, &calls, &at, &events
}

func TestStopEndsAQueuedOrRunningJobAndRecordsIt(t *testing.T) {
	for _, st := range []model.Status{model.StatusQueued, model.StatusAdmitted, model.StatusRunning} {
		s, calls, at, events := stopService(st, []string{"job-" + modelJob + "-g0002"})
		if err := s.Stop(t.Context(), modelJob); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		if len(*calls) != 1 || (*calls)[0] != modelJob || !at.Equal(stopAt) {
			t.Errorf("%s: stop calls %v at %s", st, *calls, at)
		}
		if len(*events) != 1 || (*events)[0].Reason != "StopRequested" || (*events)[0].Object != "JobSet/job-"+modelJob+"-g0002" {
			t.Errorf("%s: timeline %+v", st, *events)
		}
	}
}

func TestStopRefusesFinishedAndRecoveringJobs(t *testing.T) {
	for st, code := range map[model.Status]string{
		model.StatusSucceeded: "FINISHED", model.StatusFailed: "FINISHED", model.StatusCancelled: "FINISHED",
		model.StatusRecovering: "RECOVERING",
	} {
		s, calls, _, _ := stopService(st, nil)
		err := s.Stop(t.Context(), modelJob)
		var e *Error
		if !errors.As(err, &e) || e.Kind != KindConflict || e.ErrorCode() != code {
			t.Errorf("%s: err = %v, want conflict %s", st, err, code)
		}
		if len(*calls) != 0 {
			t.Errorf("%s: stopped anyway", st)
		}
	}
}

func TestStopAgainIsAccepted(t *testing.T) {
	s, calls, _, _ := stopService(model.StatusStopping, nil)
	if err := s.Stop(t.Context(), modelJob); err != nil || len(*calls) != 0 {
		t.Errorf("err = %v calls %v; want nil and no new stop", err, *calls)
	}
}

func TestStopOfAJobThatJustEndedIsAConflict(t *testing.T) {
	s, _, _, events := stopService(model.StatusRunning, nil)
	if err := s.Stop(t.Context(), modelJob); kindOf(err) != KindConflict || len(*events) != 0 {
		t.Errorf("err = %v events %v; want conflict and no timeline entry", err, *events)
	}
}

func TestStopChecksTheIDAndReportsClusterErrors(t *testing.T) {
	s, _, _, _ := stopService(model.StatusRunning, nil)
	if kindOf(s.Stop(t.Context(), "../x")) != KindInvalid {
		t.Error("want invalid for a bad id")
	}
	if kindOf(s.Stop(t.Context(), "ffffffff")) != KindNotFound {
		t.Error("want not found for an unknown job")
	}
	var calls []string
	var at time.Time
	sched := stoppingScheduler{fakeScheduler{[]model.JobStatus{{ID: modelJob, Status: model.StatusRunning}}}, &calls, &at, nil,
		errors.New("forbidden")}
	s = NewJobService(Config{}, Deps{Scheduler: sched, Records: fakeRecords{}})
	if kindOf(s.Stop(t.Context(), modelJob)) != KindUpstream {
		t.Error("want upstream when the cluster refuses")
	}
}

const stoppedJobSet = `{"apiVersion":"jobset.x-k8s.io/v1alpha2","kind":"JobSet","metadata":{"name":"job-aa11-g0001",
 "labels":{"devoops/job-id":"aa11","devoops/generation":"1","kueue.x-k8s.io/queue-name":"queue-light"},
 "annotations":{"devoops/name":"my-run","devoops/model":"Qwen2.5-0.5B","devoops/strategy":"fsdp","devoops/world-size":"2",
 "devoops/dataset":"aa11/v1"}}}`

func goneService(rows []model.StatusRow, objs fakeObjects, now *time.Time) (*JobService, *int) {
	lists := 0
	running := fakeScheduler{[]model.JobStatus{{ID: "bb22", Generation: 1, Status: model.StatusRunning, CreatedAt: t0}}}
	return NewJobService(Config{Now: func() time.Time { return *now }}, Deps{Scheduler: running,
		Records: fakeRecords{gone: rows}, Objects: listedObjects{objs, &lists}}), &lists
}

// A stopped job has no JobSet; it stays listed from the data database and its jobset.json.
func TestListKeepsStoppedJobsFromTheirSubmission(t *testing.T) {
	first, later := t0.Add(-time.Hour), t0.Add(-time.Minute)
	rows := []model.StatusRow{
		{JobID: "aa11", Generation: 3, State: "CANCELLED", SubmittedAt: &later, FirstSubmittedAt: &first},
		{JobID: "cc33", Generation: 1, State: "RUNNING", SubmittedAt: &first, FirstSubmittedAt: &first},
		{JobID: "dd44", Generation: 1, State: "STOPPED", SubmittedAt: &first}, // deleted: no jobset.json
	}
	objs := fakeObjects{"code/aa11/jobset.json": stoppedJobSet, "code/aa11/config.json": "{}",
		"code/cc33/jobset.json": strings.ReplaceAll(stoppedJobSet, "aa11", "cc33")}
	now := stopAt
	s, lists := goneService(rows, objs, &now)

	jobs, err := s.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]model.Job{}
	for _, j := range jobs {
		got[j.ID] = j
	}
	if len(jobs) != 3 || jobs[0].ID != "bb22" {
		t.Fatalf("jobs = %+v, want bb22, aa11 and cc33, newest first", jobs)
	}
	a := got["aa11"]
	if a.Status != model.StatusCancelled || a.Name != "my-run" || a.Model != "Qwen2.5-0.5B" || a.GPUs != 2 ||
		a.Generation != 3 || a.JobsetName != "job-aa11-g0003" || !a.CreatedAt.Equal(first) || a.Dataset == nil {
		t.Errorf("stopped job = %+v", a)
	}
	if c := got["cc33"]; c.Status != model.StatusStopping {
		t.Errorf("job whose end has not arrived = %s, want STOPPING", c.Status)
	}

	// The listing is reused for a while and the parsed jobset.json for good.
	if _, err := s.List(t.Context()); err != nil || *lists != 1 {
		t.Errorf("lists = %d, want the S3 listing reused", *lists)
	}
	now = now.Add(submissionsTTL + time.Second)
	delete(objs, "code/aa11/jobset.json") // deleted by another replica
	jobs, _ = s.List(t.Context())
	if *lists != 2 || len(jobs) != 2 {
		t.Errorf("after the TTL: lists = %d jobs = %d, want the deleted job gone", *lists, len(jobs))
	}
}

// Deleting a stopped job (GPU-196) drops it from this replica's list at once.
func TestDeleteOfAStoppedJobForgetsIt(t *testing.T) {
	rows := []model.StatusRow{{JobID: "aa11", Generation: 1, State: "CANCELLED", SubmittedAt: &t0}}
	objs := fakeObjects{"code/aa11/jobset.json": stoppedJobSet}
	now := stopAt
	s, _ := goneService(rows, objs, &now)
	if err := s.Delete(t.Context(), "aa11"); err != nil {
		t.Fatal(err)
	}
	if jobs, _ := s.List(t.Context()); len(jobs) != 1 {
		t.Errorf("jobs = %+v, want the deleted job gone", jobs)
	}
}
