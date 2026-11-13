// Package jobview folds Kafka job.lifecycle events into what the screens show per job (GPU-146):
// status · worker · progress · last approved checkpoint of the latest generation, plus timeline entries.
//
// Every backend replica reads the whole topic from the start, so all replicas hold the same views and
// a restart rebuilds them (retention 7 days). Producers:
//
//	backend        job.submitted
//	job-watcher    job.admitted · job.placed · job.started/resumed · job.completed/failed/fenced/stopped
//	ckpt-sidecar   job.progress (about every 30 s) · checkpoint.approved
//
// Events are deduplicated by event_id; within a generation the status only moves forward, because the
// producers are independent and records can arrive slightly out of order (placed before admitted).
package jobview

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

// EventWriter stores timeline entries (repository.Timeline).
type EventWriter interface {
	Put(ctx context.Context, e model.JobEvent) error
}

// statusOf maps a lifecycle event type to the job status it means.
var statusOf = map[string]model.Status{
	"job.submitted": model.StatusQueued,
	"job.admitted":  model.StatusAdmitted,
	"job.placed":    model.StatusAdmitted,
	"job.started":   model.StatusRunning,
	"job.resumed":   model.StatusRunning,
	"job.completed": model.StatusSucceeded,
	"job.failed":    model.StatusFailed,
	"job.fenced":    model.StatusRecovering,
	"job.stopped":   model.StatusCancelled,
}

type job struct {
	view     model.JobView
	progress *model.Progress // latest job.progress of view.Generation
	ckpt     int64           // highest checkpoint.approved step of view.Generation
}

// Store holds the views; safe for one writer (the Kafka consumer) and many readers (requests).
type Store struct {
	timeline EventWriter // nil = no timeline entries

	mu   sync.RWMutex
	seen map[string]struct{}
	jobs map[string]*job
}

func New(timeline EventWriter) *Store {
	return &Store{timeline: timeline, seen: map[string]struct{}{}, jobs: map[string]*job{}}
}

// Apply folds one event in. It returns false for an event_id it has already applied.
func (s *Store) Apply(ctx context.Context, e model.LifecycleEvent) bool {
	if e.JobID == "" {
		return false
	}
	s.mu.Lock()
	if _, dup := s.seen[e.EventID]; dup {
		s.mu.Unlock()
		return false
	}
	s.seen[e.EventID] = struct{}{}
	s.fold(e)
	s.mu.Unlock()

	if entry, ok := timelineEntry(e); ok && s.timeline != nil {
		if err := s.timeline.Put(ctx, entry); err != nil {
			slog.Warn("record lifecycle event", "id", e.JobID, "type", e.Type, "err", err)
		}
	}
	return true
}

func (s *Store) fold(e model.LifecycleEvent) {
	j := s.jobs[e.JobID]
	if j == nil {
		j = &job{}
		s.jobs[e.JobID] = j
	}
	switch {
	case e.Generation < j.view.Generation:
		return // a late event of a generation that was already replaced
	case e.Generation > j.view.Generation:
		*j = job{view: model.JobView{Generation: e.Generation}}
	}
	v := &j.view

	switch e.Type {
	case "job.progress":
		if j.progress != nil && e.OccurredAt.Before(j.progress.UpdatedAt) {
			return
		}
		j.progress = &model.Progress{
			Status:             str(e.Payload, "status"),
			Generation:         int64(e.Generation),
			Step:               num(e.Payload, "step"),
			TotalSteps:         num(e.Payload, "total_steps"),
			Loss:               float(e.Payload, "loss"),
			LastCheckpointStep: num(e.Payload, "last_checkpoint_step"),
			UpdatedAt:          e.OccurredAt,
		}
	case "checkpoint.approved":
		j.ckpt = max(j.ckpt, num(e.Payload, "step"))
	default:
		st, ok := statusOf[e.Type]
		if !ok {
			return
		}
		if v.Status == "" || model.StatusRank(st) > model.StatusRank(v.Status) {
			v.Status = st
			v.Message = message(e)
		}
	}
	if e.Cluster != "" {
		v.Cluster = e.Cluster
	}
	if e.OccurredAt.After(v.UpdatedAt) {
		v.UpdatedAt = e.OccurredAt
	}
}

// Views returns the views of the given jobs that have any event.
func (s *Store) Views(ids []string) map[string]model.JobView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]model.JobView, len(ids))
	for _, id := range ids {
		j, ok := s.jobs[id]
		if !ok {
			continue
		}
		v := j.view
		if j.progress != nil || j.ckpt > 0 {
			p := model.Progress{Generation: int64(v.Generation), UpdatedAt: v.UpdatedAt}
			if j.progress != nil {
				p = *j.progress
			}
			p.LastCheckpointStep = max(p.LastCheckpointStep, j.ckpt)
			v.Progress = &p
		}
		out[id] = v
	}
	return out
}

// message is the human reason of an end event; "" for the others.
func message(e model.LifecycleEvent) string {
	switch e.Type {
	case "job.failed":
		return str(e.Payload, "message")
	case "job.fenced":
		if id := str(e.Payload, "request_id"); id != "" {
			return fmt.Sprintf("relocation %s (%s)", id, str(e.Payload, "action"))
		}
	}
	return ""
}

// timelineEntry turns a status or checkpoint event into a timeline row. job.progress stays out: it
// arrives every 30 s and is already shown as the progress bar.
func timelineEntry(e model.LifecycleEvent) (model.JobEvent, bool) {
	if e.Type == "job.progress" {
		return model.JobEvent{}, false
	}
	if _, ok := statusOf[e.Type]; !ok && e.Type != "checkpoint.approved" {
		return model.JobEvent{}, false
	}
	cluster, suffix := e.Cluster, e.EventID
	if cluster == "" {
		cluster = model.ManagerCluster
	}
	if e.Type == "job.submitted" {
		// The backend records its own submission with this key (service.report); share it so the
		// entry is written once.
		cluster, suffix = model.ManagerCluster, "submitted"
	}
	t := e.OccurredAt.UTC().Truncate(time.Millisecond)
	entry := model.JobEvent{
		JobID:    e.JobID,
		Key:      t.Format(model.EventKeyTimeFormat) + "#" + cluster + "#" + suffix,
		Time:     t,
		LastTime: t,
		Cluster:  cluster,
		Source:   e.Source,
		Reason:   reason(e.Type),
		Message:  describe(e),
		Type:     "Normal",
		Object:   fmt.Sprintf("JobSet/job-%s-g%04d", e.JobID, e.Generation),
		Count:    1,
	}
	switch e.Type {
	case "job.failed", "job.fenced", "job.stopped":
		entry.Type = "Warning"
	}
	return entry, true
}

// reason turns "job.started" into "Started" and "checkpoint.approved" into "CheckpointApproved".
func reason(typ string) string {
	if typ == "checkpoint.approved" {
		return "CheckpointApproved"
	}
	name := strings.TrimPrefix(typ, "job.")
	return strings.ToUpper(name[:1]) + name[1:]
}

func describe(e model.LifecycleEvent) string {
	switch e.Type {
	case "job.submitted":
		return "submitted to queue " + str(e.Payload, "queue")
	case "job.admitted":
		return "Kueue admitted the workload"
	case "job.placed":
		return "MultiKueue placed it on " + e.Cluster
	case "job.started":
		return "all pods ready, training started"
	case "job.resumed":
		return "all pods ready, resumed from the last checkpoint"
	case "job.completed":
		return "JobSet completed"
	case "job.stopped":
		return "stopped"
	case "checkpoint.approved":
		return fmt.Sprintf("checkpoint step %d approved", num(e.Payload, "step"))
	}
	if m := message(e); m != "" {
		return m
	}
	return strings.TrimPrefix(e.Type, "job.")
}

func str(p map[string]any, k string) string {
	s, _ := p[k].(string)
	return s
}

// JSON numbers decode as float64.
func float(p map[string]any, k string) float64 {
	f, _ := p[k].(float64)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

func num(p map[string]any, k string) int64 { return int64(float(p, k)) }

// Merge combines what the management cluster says about a job (JobSet · Workload: names, settings, the
// reason it still waits) with its Kafka view. The view wins for its generation, except that the
// status never goes back: if the cluster is already further (the watcher lags by a moment, or its
// events are not in the topic any more), the cluster's status stays.
func Merge(st model.JobStatus, v model.JobView, ok bool) model.Job {
	j := model.Job{JobStatus: st, UpdatedAt: st.CreatedAt}
	if !ok || v.Generation < st.Generation {
		return j
	}
	if v.Generation > st.Generation {
		j.Generation = v.Generation // the next generation is announced before its JobSet is listed
		j.Status, j.Message, j.Cluster = v.Status, v.Message, v.Cluster
	} else if v.Status != "" && model.StatusRank(v.Status) >= model.StatusRank(st.Status) {
		j.Status = v.Status
		if v.Message != "" {
			j.Message = v.Message
		}
	}
	if j.Status == "" {
		j.Status = st.Status
	}
	if j.Cluster == "" {
		j.Cluster = v.Cluster
	}
	j.Progress = v.Progress
	if v.UpdatedAt.After(j.UpdatedAt) {
		j.UpdatedAt = v.UpdatedAt
	}
	return j
}
