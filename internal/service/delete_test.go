package service

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/DevOops-Team/Backend/internal/model"
)

// recordingScheduler lists fixed jobs and records which jobs' JobSets were deleted.
type recordingScheduler struct {
	fakeScheduler
	deleted *[]string
}

func (f recordingScheduler) Delete(_ context.Context, jobID string) error {
	*f.deleted = append(*f.deleted, jobID)
	return nil
}

// purgingObjects records the purged prefixes in order and can fail on one of them.
type purgingObjects struct {
	fakeObjects
	purged *[]string
	failOn string
}

func (f purgingObjects) DeleteAll(_ context.Context, prefix string) error {
	if prefix == f.failOn {
		return errors.New("AccessDenied")
	}
	*f.purged = append(*f.purged, prefix)
	return nil
}

func deleteService(status model.Status, failOn string) (*JobService, *[]string, *[]string) {
	var deleted, purged []string
	sched := recordingScheduler{fakeScheduler{[]model.JobStatus{{ID: modelJob, Generation: 2, Status: status, CreatedAt: t0}}}, &deleted}
	objs := purgingObjects{fakeObjects{}, &purged, failOn}
	return NewJobService(Config{}, Deps{Scheduler: sched, Records: fakeRecords{}, Events: fakeEvents{}, Objects: objs}), &deleted, &purged
}

func TestDeletePurgesCheckpointsModelAndInputsThenTheJobSets(t *testing.T) {
	for _, st := range []model.Status{model.StatusSucceeded, model.StatusFailed, model.StatusCancelled} {
		s, deleted, purged := deleteService(st, "")
		if err := s.Delete(t.Context(), modelJob); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
		want := []string{"jobs/" + modelJob + "/", "code/" + modelJob + "/", "datasets/" + modelJob + "/"}
		if !slices.Equal(*purged, want) {
			t.Errorf("%s: purged %v, want %v", st, *purged, want)
		}
		if !slices.Equal(*deleted, []string{modelJob}) {
			t.Errorf("%s: deleted JobSets of %v", st, *deleted)
		}
	}
}

func TestDeleteRefusesAJobThatHasNotFinished(t *testing.T) {
	for _, st := range []model.Status{model.StatusQueued, model.StatusAdmitted, model.StatusRunning, model.StatusRecovering} {
		s, deleted, purged := deleteService(st, "")
		err := s.Delete(t.Context(), modelJob)
		var e *Error
		if !errors.As(err, &e) || e.Kind != KindConflict || e.ErrorCode() != "NOT_FINISHED" {
			t.Errorf("%s: err = %v, want conflict NOT_FINISHED", st, err)
		}
		if len(*deleted) != 0 || len(*purged) != 0 {
			t.Errorf("%s: deleted %v purged %v, want nothing", st, *deleted, *purged)
		}
	}
}

// When S3 fails the JobSets stay, so the job is still listed and the delete can be retried.
func TestDeleteKeepsTheJobWhenS3Fails(t *testing.T) {
	s, deleted, _ := deleteService(model.StatusSucceeded, "code/"+modelJob+"/")
	err := s.Delete(t.Context(), modelJob)
	if kindOf(err) != KindUpstream {
		t.Fatalf("err = %v, want upstream", err)
	}
	if len(*deleted) != 0 {
		t.Errorf("JobSets deleted %v after an S3 failure", *deleted)
	}
}

func TestDeleteChecksTheID(t *testing.T) {
	s, _, _ := deleteService(model.StatusSucceeded, "")
	if kindOf(s.Delete(t.Context(), "../x")) != KindInvalid {
		t.Error("want invalid for a bad id")
	}
	if kindOf(s.Delete(t.Context(), "ffffffff")) != KindNotFound {
		t.Error("want not found for an unknown job")
	}
}
