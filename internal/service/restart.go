package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
)

// Restart runs a job the user stopped again from its last approved checkpoint (GPU-240). It moves the
// fencing item from the stopped generation N to N+1 and submits generation N+1, made from the stored
// jobset.json the way the relocator makes a recovery generation, with the restart annotations. ckpt-init of
// the new generation downloads the approved checkpoint and training continues after its step (nothing
// approved yet: from the start). The job watcher reports job.submitted (new generation) + job.resumed.
//
// Only a CANCELLED job restarts. A STOPPING one is still being stopped (its end event has not arrived),
// a queued, running or recovering one is not stopped, and a finished one has nothing to continue.
func (s *JobService) Restart(ctx context.Context, id string) error {
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
	job := jobs[i]
	switch job.Status {
	case model.StatusCancelled:
	case model.StatusStopping:
		return &Error{Kind: KindConflict, Code: "STOPPING", Msg: "the job is still stopping; restart it once it shows as stopped"}
	case model.StatusQueued, model.StatusAdmitted, model.StatusRunning, model.StatusRecovering:
		return &Error{Kind: KindConflict, Code: "NOT_STOPPED", Msg: "the job has not been stopped (status " + string(job.Status) + ")"}
	default:
		return alreadyEnded(job.Status)
	}
	if job.Generation < 1 {
		return &Error{Kind: KindConflict, Code: "NOT_STOPPED", Msg: "the job's generation is not known yet"}
	}

	data, _, err := s.objects.Get(ctx, s.jobsetKey(id), maxMetadataBytes)
	if errors.Is(err, model.ErrNotFound) {
		return notFound("the job's jobset.json is gone")
	}
	if err != nil {
		return upstream("read jobset.json", err)
	}
	var orig map[string]any
	if err := json.Unmarshal(data, &orig); err != nil {
		return fmt.Errorf("parse jobset.json of %s: %w", id, err)
	}
	at := s.cfg.Now().UTC().Truncate(time.Second)
	next := job.Generation + 1
	js := restartJobSet(orig, id, next, at)

	if err := s.fencing.Restart(ctx, id, job.Generation, at); err != nil {
		if errors.Is(err, model.ErrGenerationMoved) || errors.Is(err, model.ErrNotFound) {
			return &Error{Kind: KindConflict, Code: "GENERATION_MOVED",
				Msg: "the job's fencing record has moved on; reload and try again", Err: err}
		}
		return upstream("advance the fencing generation", err)
	}
	if err := s.sched.Submit(ctx, js); err != nil && !errors.Is(err, model.ErrAlreadyExists) {
		// The fencing item is already at next: a retry of the restart skips that step and submits again.
		return &Error{Kind: KindUpstream, Code: "SUBMIT_FAILED", Msg: "submit JobSet failed: " + err.Error(), Err: err}
	}
	slog.Info("job restarted", "id", id, "from", job.Generation, "jobset", js.GetName())
	t := s.cfg.Now().UTC().Truncate(time.Millisecond)
	if err := s.record.Put(ctx, model.JobEvent{
		JobID: id, Key: t.Format(model.EventKeyTimeFormat) + "#" + model.ManagerCluster + "#restart-requested",
		Time: t, LastTime: t, Cluster: model.ManagerCluster, Source: "backend", Reason: "RestartRequested",
		Message: fmt.Sprintf("restarted by the user from the last approved checkpoint; submitted JobSet %s", js.GetName()),
		Type:    "Normal", Object: "JobSet/" + js.GetName(), Count: 1,
	}); err != nil {
		slog.Warn("record restart", "id", id, "err", err)
	}
	return nil
}

// restartJobSet makes generation gen of a job from its stored jobset.json, changing only what names the
// generation (the relocator's recovery_render.py does the same for a recovery, JobSet 템플릿 합의안 6절):
// the name and DNS subdomain, the devoops/generation labels, every container's GENERATION env and the
// trainer's master address. It keeps the job's own queue and priority (not the recovery ones), drops a
// recovery-id or stop annotation a copy could carry and adds the restart annotations. orig is not changed.
func restartJobSet(orig map[string]any, jobID string, gen int, at time.Time) *unstructured.Unstructured {
	js := &unstructured.Unstructured{Object: orig}
	js = js.DeepCopy()
	obj := js.Object
	delete(obj, "status")
	for _, k := range []string{"uid", "resourceVersion", "creationTimestamp", "generation", "managedFields", "selfLink"} {
		unstructured.RemoveNestedField(obj, "metadata", k)
	}
	name := model.JobSetName(jobID, gen)
	generation := strconv.Itoa(gen)
	js.SetName(name)

	labels := js.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[model.GenerationLabel] = generation
	js.SetLabels(labels)
	ann := js.GetAnnotations()
	if ann == nil {
		ann = map[string]string{}
	}
	for _, k := range []string{model.RecoveryIDAnnotation, model.StopReasonAnnotation, model.StopRequestedAtAnnotation} {
		delete(ann, k)
	}
	ann[model.RestartReasonAnnotation] = model.RestartReasonUser
	ann[model.RestartRequestedAtAnnotation] = at.UTC().Format(time.RFC3339)
	js.SetAnnotations(ann)

	if _, ok, _ := unstructured.NestedFieldNoCopy(obj, "spec", "network", "subdomain"); ok {
		_ = unstructured.SetNestedField(obj, name, "spec", "network", "subdomain")
	}
	oldNames := regexp.MustCompile(`job-` + regexp.QuoteMeta(jobID) + `-g\d{4}`)
	rjs, _, _ := unstructured.NestedSlice(obj, "spec", "replicatedJobs")
	for _, rj := range rjs {
		tmpl, ok := nestedMap(rj, "template", "spec", "template")
		if !ok {
			continue
		}
		meta, _ := tmpl["metadata"].(map[string]any)
		if meta == nil {
			meta = map[string]any{}
			tmpl["metadata"] = meta
		}
		podLabels, _ := meta["labels"].(map[string]any)
		if podLabels == nil {
			podLabels = map[string]any{}
			meta["labels"] = podLabels
		}
		podLabels[model.GenerationLabel] = generation
		spec, _ := tmpl["spec"].(map[string]any)
		for _, field := range []string{"initContainers", "containers"} {
			cs, _ := spec[field].([]any)
			for _, c := range cs {
				ct, _ := c.(map[string]any)
				if ct == nil {
					continue
				}
				envs, _ := ct["env"].([]any)
				for _, e := range envs {
					if ev, _ := e.(map[string]any); ev != nil && ev["name"] == "GENERATION" {
						if _, has := ev["value"]; has {
							ev["value"] = generation
						}
					}
				}
				for _, f := range []string{"command", "args"} {
					vals, _ := ct[f].([]any)
					for i, v := range vals {
						if str, ok := v.(string); ok {
							vals[i] = oldNames.ReplaceAllString(str, name) // master address (trainer script)
						}
					}
				}
			}
		}
	}
	_ = unstructured.SetNestedSlice(obj, rjs, "spec", "replicatedJobs")
	return js
}

func nestedMap(v any, path ...string) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	for _, p := range path {
		if !ok {
			return nil, false
		}
		m, ok = m[p].(map[string]any)
	}
	return m, ok
}
