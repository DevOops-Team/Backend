package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

// Stop ends a queued or running job at the user's request (GPU-204). The JobSets get the stop annotations
// and are deleted; the job watcher (Data) then reports job.cancelled and the collector records the end, so
// the job shows STOPPING until the data database says CANCELLED. The job stays listed (gone below).
//
// A recovering job is refused: the relocator is about to create its next generation from jobset.json and
// would bring it back. A job that already stops is accepted again (nothing to do), a finished one is not.
func (s *JobService) Stop(ctx context.Context, id string) error {
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
	switch st := jobs[i].Status; st {
	case model.StatusQueued, model.StatusAdmitted, model.StatusRunning:
	case model.StatusStopping:
		return nil
	case model.StatusRecovering:
		return &Error{Kind: KindConflict, Code: "RECOVERING",
			Msg: "the job is being moved to another worker; stop it once it runs again"}
	default:
		return alreadyEnded(st)
	}

	at := s.cfg.Now()
	stopped, err := s.sched.Stop(ctx, id, at)
	if err != nil {
		return upstream("stop JobSets", err)
	}
	if len(stopped) == 0 {
		return alreadyEnded(jobs[i].Status) // it ended between the list and the stop
	}
	slog.Info("job stopped", "id", id, "jobsets", stopped)
	t := at.UTC().Truncate(time.Millisecond)
	if err := s.record.Put(ctx, model.JobEvent{
		JobID: id, Key: t.Format(model.EventKeyTimeFormat) + "#" + model.ManagerCluster + "#stop-requested",
		Time: t, LastTime: t, Cluster: model.ManagerCluster, Source: "backend", Reason: "StopRequested",
		Message: "stopped by the user; deleted JobSet " + strings.Join(stopped, ", "),
		Type:    "Normal", Object: "JobSet/" + stopped[len(stopped)-1], Count: 1,
	}); err != nil {
		slog.Warn("record stop", "id", id, "err", err)
	}
	return nil
}

func alreadyEnded(st model.Status) error {
	return &Error{Kind: KindConflict, Code: "FINISHED", Msg: "the job has already ended (status " + string(st) + ")"}
}

// submissionsTTL bounds how long a deleted job (GPU-196) can still be listed by another replica.
const submissionsTTL = 30 * time.Second

// submissions knows which jobs S3 still holds a jobset.json for, so a job whose JobSets were deleted by a
// stop can still be listed from what Submit wrote. The listing of code/ is refreshed every submissionsTTL;
// a jobset.json never changes, so its parsed form is kept.
type submissions struct {
	mu       sync.Mutex
	listedAt time.Time
	present  map[string]bool
	meta     map[string]model.JobStatus
}

func (s *JobService) jobsetKey(id string) string { return "code/" + id + "/jobset.json" }

// present reports whether S3 still has the job's jobset.json.
func (s *JobService) present(ctx context.Context, id string) (bool, error) {
	c := &s.subs
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.present == nil || s.cfg.Now().Sub(c.listedAt) > submissionsTTL {
		objs, err := s.objects.List(ctx, "code/")
		if err != nil {
			return false, err
		}
		c.present = map[string]bool{}
		for _, o := range objs {
			if jobID, ok := strings.CutSuffix(strings.TrimPrefix(o.Key, "code/"), "/jobset.json"); ok {
				c.present[jobID] = true
			}
		}
		c.listedAt = s.cfg.Now()
	}
	return c.present[id], nil
}

// forget drops a deleted job at once on this replica.
func (s *JobService) forget(id string) {
	c := &s.subs
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.present, id)
	delete(c.meta, id)
}

// submitted reads the job's name, model, strategy and the rest from its jobset.json.
func (s *JobService) submitted(ctx context.Context, id string) (model.JobStatus, error) {
	c := &s.subs
	c.mu.Lock()
	st, ok := c.meta[id]
	c.mu.Unlock()
	if ok {
		return st, nil
	}
	data, _, err := s.objects.Get(ctx, s.jobsetKey(id), maxMetadataBytes)
	if err != nil {
		return model.JobStatus{}, err
	}
	var js struct {
		Metadata struct {
			Name        string            `json:"name"`
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(data, &js); err != nil {
		return model.JobStatus{}, err
	}
	st = model.JobStatusFromMeta(id, js.Metadata.Name, js.Metadata.Labels, js.Metadata.Annotations)
	c.mu.Lock()
	if c.meta == nil {
		c.meta = map[string]model.JobStatus{}
	}
	c.meta[id] = st
	c.mu.Unlock()
	return st, nil
}

// gone turns data-database rows of jobs without a JobSet (JobRecords.Gone) into list entries. The status
// starts as STOPPING, the database's end (CANCELLED, or RECOVERING while the relocator works) replaces it
// in jobview.Merge. A job S3 no longer has was deleted and is left out.
func (s *JobService) gone(ctx context.Context, rows []model.StatusRow) []model.JobStatus {
	var out []model.JobStatus
	for _, r := range rows {
		ok, err := s.present(ctx, r.JobID)
		if err != nil {
			slog.Warn("list submissions in S3", "err", err)
			return out
		}
		if !ok {
			continue
		}
		st, err := s.submitted(ctx, r.JobID)
		if err != nil {
			if !errors.Is(err, model.ErrNotFound) {
				slog.Warn("read jobset.json", "id", r.JobID, "err", err)
			}
			continue
		}
		st.Generation = r.Generation
		st.JobsetName = model.JobSetName(r.JobID, r.Generation)
		st.Status = model.StatusStopping
		for _, t := range []*time.Time{r.SubmittedAt, r.FirstSubmittedAt} {
			if t != nil {
				st.CreatedAt = *t
			}
		}
		out = append(out, st)
	}
	return out
}
