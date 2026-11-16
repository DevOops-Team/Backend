// Package jobview turns what the data database records about a job into what the screens show:
// status · worker · progress · last approved checkpoint of the latest generation, plus timeline rows.
//
// The backend no longer folds Kafka job.lifecycle itself (GPU-146). The collector (Data repo) stores
// every event in PostgreSQL gpuf_data, and the backend reads it as backend_ro (Data db/004):
//
//	data.v_job_status   latest generation per job + progress + last approved checkpoint → FromStatus
//	data.job_runs       one row per generation (submitted · started · ended)            → History
//	data.checkpoints    approved checkpoints                                            → History
//
// So every replica shows the same thing and a restart loses nothing. The backend still publishes
// job.submitted to Kafka; the collector turns it into the job_runs row.
package jobview

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

// statusOf maps v_job_status.state to the API status.
var statusOf = map[string]model.Status{
	"PENDING":   model.StatusQueued,
	"RUNNING":   model.StatusRunning,
	"COMPLETED": model.StatusSucceeded,
	"FAILED":    model.StatusFailed,
	"FENCED":    model.StatusRecovering, // the relocator took the generation down; the next one follows
	"STOPPED":   model.StatusCancelled,
	"CANCELLED": model.StatusCancelled,
}

// FromStatus turns a v_job_status row into the job's view. An unknown state leaves Status empty, so
// Merge keeps the management cluster's status.
func FromStatus(r model.StatusRow) model.JobView {
	v := model.JobView{Generation: r.Generation, Status: statusOf[r.State], Cluster: deref(r.Cluster)}
	for _, t := range []*time.Time{r.SubmittedAt, r.StartedAt, r.EndedAt, r.ProgressUpdatedAt, r.LastApprovedAt} {
		if t != nil && t.After(v.UpdatedAt) {
			v.UpdatedAt = *t
		}
	}
	if r.Step == nil && r.LastApprovedStep == nil {
		return v
	}
	p := model.Progress{Generation: int64(r.Generation), UpdatedAt: v.UpdatedAt}
	if r.Step != nil {
		p.Status = deref(r.ProgressStatus)
		p.Step = *r.Step
		p.TotalSteps = deref(r.TotalSteps)
		p.Loss = deref(r.Loss)
		if r.ProgressUpdatedAt != nil {
			p.UpdatedAt = *r.ProgressUpdatedAt
		}
	}
	// The approved checkpoint (any generation) is where recovery resumes from, so it is what the
	// screen calls the last checkpoint.
	p.LastCheckpointStep = deref(r.LastApprovedStep)
	v.Progress = &p
	return v
}

// History returns the job's timeline rows from its generations and approved checkpoints, oldest first.
// Keys follow the timeline format "<time>#<cluster>#<id>" so they merge with the management-cluster rows.
func History(jobID string, runs []model.RunRecord, ckpts []model.CheckpointRecord) []model.JobEvent {
	var out []model.JobEvent
	clusterOf := map[int]string{}
	for _, r := range runs {
		cluster := deref(r.Cluster)
		clusterOf[r.Generation] = cluster
		object := fmt.Sprintf("JobSet/%s", model.JobSetName(jobID, r.Generation))
		if r.SubmittedAt != nil {
			// The backend records its own submission with this key (service.report); share it.
			suffix := "submitted"
			if r.Generation > 1 {
				suffix = fmt.Sprintf("submitted-g%04d", r.Generation)
			}
			msg := "submitted"
			if q := deref(r.Queue); q != "" {
				msg = "submitted to queue " + q
			}
			out = append(out, row(jobID, *r.SubmittedAt, model.ManagerCluster, suffix, "backend", "Submitted", msg, "Normal", object))
		}
		if r.StartedAt != nil {
			reason, msg := "Started", "training started"
			if r.Generation > 1 {
				reason, msg = "Resumed", "resumed from the last approved checkpoint"
			}
			out = append(out, row(jobID, *r.StartedAt, orManager(cluster), fmt.Sprintf("started-g%04d", r.Generation),
				"job-watcher", reason, msg, "Normal", object))
		}
		if r.EndedAt != nil {
			end := deref(r.EndReason)
			reason, typ := endReason(end)
			out = append(out, row(jobID, *r.EndedAt, orManager(cluster), fmt.Sprintf("ended-g%04d", r.Generation),
				"job-watcher", reason, end, typ, object))
		}
	}
	for _, c := range ckpts {
		out = append(out, row(jobID, c.ApprovedAt, orManager(clusterOf[c.Generation]),
			fmt.Sprintf("checkpoint-g%04d-s%d", c.Generation, c.Step), "ckpt-sidecar", "CheckpointApproved",
			fmt.Sprintf("checkpoint step %d approved", c.Step), "Normal",
			"JobSet/"+model.JobSetName(jobID, c.Generation)))
	}
	slices.SortStableFunc(out, func(a, b model.JobEvent) int { return strings.Compare(a.Key, b.Key) })
	return out
}

func endReason(end string) (reason, typ string) {
	switch end {
	case "completed":
		return "Completed", "Normal"
	case "failed":
		return "Failed", "Warning"
	case "fenced":
		return "Fenced", "Warning"
	case "stopped", "cancelled":
		return "Stopped", "Warning"
	}
	return "Ended", "Normal"
}

func row(jobID string, at time.Time, cluster, suffix, source, reason, msg, typ, object string) model.JobEvent {
	t := at.UTC().Truncate(time.Millisecond)
	return model.JobEvent{
		JobID: jobID, Key: t.Format(model.EventKeyTimeFormat) + "#" + cluster + "#" + suffix,
		Time: t, LastTime: t, Cluster: cluster, Source: source, Reason: reason, Message: msg,
		Type: typ, Object: object, Count: 1,
	}
}

func orManager(cluster string) string {
	if cluster == "" {
		return model.ManagerCluster
	}
	return cluster
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

// Merge combines what the management cluster says about a job (JobSet · Workload: names, settings, the
// reason it still waits) with its database view. The view wins for its generation, except that the
// status never goes back: if the cluster is already further (the collector lags by a moment), the
// cluster's status stays.
func Merge(st model.JobStatus, v model.JobView, ok bool) model.Job {
	j := model.Job{JobStatus: st, UpdatedAt: st.CreatedAt}
	if !ok || v.Generation < st.Generation {
		return j
	}
	if v.Generation > st.Generation {
		j.Generation = v.Generation // the next generation is recorded before its JobSet is listed
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
