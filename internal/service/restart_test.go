package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service/jobspec"
	"github.com/DevOops-Team/Backend/internal/settings"
)

var restartAt = time.Date(2026, 11, 20, 4, 0, 0, 0, time.UTC)

// submittingScheduler lists fixed jobs and keeps what is submitted.
type submittingScheduler struct {
	fakeScheduler
	got *[]*unstructured.Unstructured
	err error
}

func (f submittingScheduler) Submit(_ context.Context, js *unstructured.Unstructured) error {
	*f.got = append(*f.got, js)
	return f.err
}

type restartFencing struct {
	noFencing
	calls *[]int
	err   error
}

func (f restartFencing) Restart(_ context.Context, _ string, from int, at time.Time) error {
	*f.calls = append(*f.calls, from)
	if !at.Equal(restartAt) {
		return errors.New("wrong time " + at.String())
	}
	return f.err
}

// storedJobSet is what Submit wrote to S3 for generation 1 of job modelJob.
func storedJobSet(t *testing.T) string {
	t.Helper()
	st, err := settings.Load("")
	if err != nil {
		t.Fatal(err)
	}
	js, err := jobspec.Render(jobspec.Input{
		JobID: modelJob, Generation: 1, Name: "demo", Model: "gpt2", Strategy: "fsdp", GPUs: 2,
		DatasetID: modelJob, DatasetVersion: 1, ConfigSHA256: "f7be", CheckpointFormat: "fsdp-dcp", Queue: "queue-light",
		Namespace: "training", Bucket: "b", FencingTable: "f", KafkaBootstrap: "k:9092", KafkaTopic: "job.lifecycle",
		Settings: st.WithImages("ghcr.io/devoops-team/trainer@sha256:t1", "ghcr.io/devoops-team/checkpoint@sha256:c1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(js.Object)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func restartService(t *testing.T, status model.Status, fencingErr, submitErr error) (*JobService, *[]int, *[]*unstructured.Unstructured, *[]model.JobEvent) {
	var calls []int
	var submitted []*unstructured.Unstructured
	var events []model.JobEvent
	sched := submittingScheduler{fakeScheduler{[]model.JobStatus{{ID: modelJob, Generation: 2, Status: status, CreatedAt: t0}}},
		&submitted, submitErr}
	s := NewJobService(Config{Now: func() time.Time { return restartAt }}, Deps{
		Scheduler: sched, Records: fakeRecords{}, Events: fakeEvents{}, Record: recordedEvents{&events},
		Objects: fakeObjects{"code/" + modelJob + "/jobset.json": storedJobSet(t)},
		Fencing: restartFencing{calls: &calls, err: fencingErr},
	})
	return s, &calls, &submitted, &events
}

func TestRestartSubmitsTheNextGenerationOfAStoppedJob(t *testing.T) {
	s, calls, submitted, events := restartService(t, model.StatusCancelled, nil, nil)
	if err := s.Restart(t.Context(), modelJob); err != nil {
		t.Fatal(err)
	}
	if len(*calls) != 1 || (*calls)[0] != 2 {
		t.Errorf("fencing restart calls = %v, want one from generation 2", *calls)
	}
	if len(*submitted) != 1 || (*submitted)[0].GetName() != "job-"+modelJob+"-g0003" {
		t.Fatalf("submitted %v, want job-%s-g0003", *submitted, modelJob)
	}
	if len(*events) != 1 || (*events)[0].Reason != "RestartRequested" || (*events)[0].Object != "JobSet/job-"+modelJob+"-g0003" {
		t.Errorf("timeline %+v", *events)
	}
}

func TestRestartRefusesJobsThatAreNotStopped(t *testing.T) {
	for st, code := range map[model.Status]string{
		model.StatusStopping: "STOPPING", model.StatusQueued: "NOT_STOPPED", model.StatusRunning: "NOT_STOPPED",
		model.StatusRecovering: "NOT_STOPPED", model.StatusSucceeded: "FINISHED", model.StatusFailed: "FINISHED",
	} {
		s, calls, submitted, _ := restartService(t, st, nil, nil)
		var e *Error
		if err := s.Restart(t.Context(), modelJob); !errors.As(err, &e) || e.Kind != KindConflict || e.ErrorCode() != code {
			t.Errorf("%s: err = %v, want conflict %s", st, err, code)
		}
		if len(*calls)+len(*submitted) != 0 {
			t.Errorf("%s: touched fencing %v or submitted %d", st, *calls, len(*submitted))
		}
	}
}

func TestRestartDoesNotSubmitWhenTheGenerationMoved(t *testing.T) {
	s, _, submitted, _ := restartService(t, model.StatusCancelled, model.ErrGenerationMoved, nil)
	var e *Error
	if err := s.Restart(t.Context(), modelJob); !errors.As(err, &e) || e.ErrorCode() != "GENERATION_MOVED" {
		t.Errorf("err = %v, want GENERATION_MOVED", err)
	}
	if len(*submitted) != 0 {
		t.Error("submitted although the fencing step failed")
	}
}

func TestRestartAcceptsAJobSetThatARetryAlreadySubmitted(t *testing.T) {
	s, _, _, _ := restartService(t, model.StatusCancelled, nil, model.ErrAlreadyExists)
	if err := s.Restart(t.Context(), modelJob); err != nil {
		t.Errorf("err = %v; the same generation's JobSet exists, so the restart is done", err)
	}
	s, _, _, _ = restartService(t, model.StatusCancelled, nil, errors.New("forbidden"))
	if kindOf(s.Restart(t.Context(), modelJob)) != KindUpstream {
		t.Error("want upstream when the cluster refuses")
	}
}

func TestRestartChecksTheID(t *testing.T) {
	s, _, _, _ := restartService(t, model.StatusCancelled, nil, nil)
	if kindOf(s.Restart(t.Context(), "../x")) != KindInvalid || kindOf(s.Restart(t.Context(), "ffffffff")) != KindNotFound {
		t.Error("want invalid for a bad id and not found for an unknown job")
	}
}

func TestRestartJobSetChangesOnlyTheGeneration(t *testing.T) {
	var orig map[string]any
	if err := json.Unmarshal([]byte(storedJobSet(t)), &orig); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(orig)
	js := restartJobSet(orig, modelJob, 3, restartAt)
	if after, _ := json.Marshal(orig); string(after) != string(before) {
		t.Error("restartJobSet changed the stored original")
	}
	name := "job-" + modelJob + "-g0003"
	if js.GetName() != name {
		t.Errorf("name = %s", js.GetName())
	}
	if sub, _, _ := unstructured.NestedString(js.Object, "spec", "network", "subdomain"); sub != name {
		t.Errorf("subdomain = %s", sub)
	}
	l := js.GetLabels()
	if l[model.GenerationLabel] != "3" || l[model.QueueLabel] != "queue-light" || l[model.JobIDLabel] != modelJob {
		t.Errorf("labels = %v; generation 3, the job's own queue", l)
	}
	a := js.GetAnnotations()
	if a[model.RestartReasonAnnotation] != "user" || a[model.RestartRequestedAtAnnotation] != "2026-11-20T04:00:00Z" {
		t.Errorf("restart annotations = %v", a)
	}
	if _, ok := a[model.RecoveryIDAnnotation]; ok {
		t.Error("a user restart is not a recovery")
	}

	raw, _ := json.Marshal(js.Object)
	text := string(raw)
	if strings.Contains(text, "-g0001") {
		t.Errorf("generation 1 name left in the JobSet: %s", text)
	}
	rjs, _, _ := unstructured.NestedSlice(js.Object, "spec", "replicatedJobs")
	tmpl, _ := nestedMap(rjs[0], "template", "spec", "template")
	if tmpl["metadata"].(map[string]any)["labels"].(map[string]any)[model.GenerationLabel] != "3" {
		t.Error("pod template generation label not moved")
	}
	spec := tmpl["spec"].(map[string]any)
	for _, field := range []string{"initContainers", "containers"} {
		for _, c := range spec[field].([]any) {
			ct := c.(map[string]any)
			for _, e := range ct["env"].([]any) {
				if ev := e.(map[string]any); ev["name"] == "GENERATION" && ev["value"] != "3" {
					t.Errorf("%s GENERATION = %v", ct["name"], ev["value"])
				}
			}
		}
	}
	trainer := spec["containers"].([]any)[0].(map[string]any)
	if !strings.Contains(trainer["args"].([]any)[0].(string), "--master-addr="+name+"-workers-0-0."+name) {
		t.Errorf("trainer master address not moved: %v", trainer["args"])
	}
}
