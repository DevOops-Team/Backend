package service

import (
	"context"
	"log/slog"
	"slices"

	"github.com/DevOops-Team/Backend/internal/model"
)

// Delete removes a finished job for good (GPU-196): its checkpoints and final model (jobs/<id>/), the
// submission (code/<id>/) and the uploaded model · data (datasets/<id>/) with every S3 version, then its
// JobSets, which takes it off the list. S3 goes first so that a failure leaves the job listed and the
// delete can be retried. The data database rows stay as history for the metrics.
func (s *JobService) Delete(ctx context.Context, id string) error {
	if !validJobID.MatchString(id) {
		return invalid("invalid job id")
	}
	jobs, err := s.List(ctx)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(jobs, func(j model.Job) bool { return j.ID == id })
	if i < 0 {
		return notFound("job not found")
	}
	switch jobs[i].Status {
	case model.StatusSucceeded, model.StatusFailed, model.StatusCancelled:
	default:
		// A running job still writes checkpoints and may be relocated; stop it first.
		return &Error{Kind: KindConflict, Code: "NOT_FINISHED",
			Msg: "only finished jobs can be deleted (status " + string(jobs[i].Status) + ")"}
	}

	for _, prefix := range []string{"jobs/" + id + "/", "code/" + id + "/", "datasets/" + id + "/"} {
		if err := s.objects.DeleteAll(ctx, prefix); err != nil {
			return upstream("delete s3://"+prefix, err)
		}
	}
	if err := s.sched.Delete(ctx, id); err != nil {
		return upstream("delete JobSets", err)
	}
	slog.Info("job deleted", "id", id, "status", jobs[i].Status)
	return nil
}
