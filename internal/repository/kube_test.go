package repository

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/DevOops-Team/Backend/internal/model"
)

func jobSet(name, jobID string, status map[string]any) *unstructured.Unstructured {
	obj := map[string]any{
		"apiVersion": "jobset.x-k8s.io/v1alpha2",
		"kind":       "JobSet",
		"metadata": map[string]any{
			"name":              name,
			"namespace":         "training",
			"uid":               "uid-" + name,
			"creationTimestamp": "2026-10-27T10:00:00Z",
			"labels":            map[string]any{"devoops/job-id": jobID},
		},
		"spec": map[string]any{},
	}
	if status != nil {
		obj["status"] = status
	}
	return &unstructured.Unstructured{Object: obj}
}

func workload(owner string, status map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kueue.x-k8s.io/v1beta2",
		"kind":       "Workload",
		"metadata": map[string]any{
			"name":      "jobset-" + owner + "-x",
			"namespace": "training",
			"ownerReferences": []any{map[string]any{
				"apiVersion": "jobset.x-k8s.io/v1alpha2", "kind": "JobSet", "name": owner, "uid": "uid-" + owner,
			}},
		},
		"status": status,
	}}
}

func condition(typ, status string) map[string]any {
	return map[string]any{"type": typ, "status": status, "message": typ + " message"}
}

func newKube(objs ...runtime.Object) *Kube {
	scheme := runtime.NewScheme()
	gvrs := map[schema.GroupVersionResource]string{
		JobSetGVR:   "JobSetList",
		WorkloadGVR: "WorkloadList",
		EventGVR:    "EventList",
	}
	return &Kube{dyn: dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, gvrs, objs...), namespace: "training"}
}

func listOne(t *testing.T, c *Kube) model.JobStatus {
	t.Helper()
	jobs, err := c.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("got %d jobs, want 1", len(jobs))
	}
	return jobs[0]
}

func TestListReportsPendingJobWithoutWorkloadAdmission(t *testing.T) {
	c := newKube(jobSet("job-a1", "a1", nil), workload("job-a1", map[string]any{}))

	got := listOne(t, c)

	if got.ID != "a1" || got.Name != "job-a1" || got.Phase != model.PhasePending {
		t.Errorf("got %+v, want a1/job-a1 Pending", got)
	}
}

func TestListReportsClusterMultiKueuePicked(t *testing.T) {
	c := newKube(
		jobSet("job-a1", "a1", nil),
		workload("job-a1", map[string]any{
			"clusterName": "devoops-b",
			"conditions":  []any{condition("QuotaReserved", "True"), condition("Admitted", "True")},
		}),
	)

	got := listOne(t, c)

	if got.Cluster != "devoops-b" || got.Phase != model.PhaseAdmitted {
		t.Errorf("got cluster %q phase %q, want devoops-b Admitted", got.Cluster, got.Phase)
	}
}

func TestListReportsRunningWhenJobSetHasActivePods(t *testing.T) {
	c := newKube(
		jobSet("job-a1", "a1", map[string]any{
			"restarts":             int64(1),
			"replicatedJobsStatus": []any{map[string]any{"name": "workers", "active": int64(1)}},
		}),
		workload("job-a1", map[string]any{"conditions": []any{condition("Admitted", "True")}}),
	)

	got := listOne(t, c)

	if got.Phase != model.PhaseRunning || got.Restarts != 1 {
		t.Errorf("got phase %q restarts %d, want Running 1", got.Phase, got.Restarts)
	}
}

func TestListReportsSucceededFromJobSetCompletedCondition(t *testing.T) {
	c := newKube(jobSet("job-a1", "a1", map[string]any{"conditions": []any{condition("Completed", "True")}}))

	if got := listOne(t, c); got.Phase != model.PhaseSucceeded {
		t.Errorf("phase = %q, want Succeeded", got.Phase)
	}
}

func TestListReportsFailedWithReason(t *testing.T) {
	c := newKube(jobSet("job-a1", "a1", map[string]any{"conditions": []any{condition("Failed", "True")}}))

	got := listOne(t, c)

	if got.Phase != model.PhaseFailed || got.Message != "Failed message" {
		t.Errorf("got phase %q message %q, want Failed with condition message", got.Phase, got.Message)
	}
}

func TestListIgnoresJobSetsNotCreatedByBackend(t *testing.T) {
	foreign := jobSet("someone-else", "", nil)
	foreign.SetLabels(nil)
	c := newKube(foreign, jobSet("job-a1", "a1", nil))

	jobs, err := c.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(jobs) != 1 || jobs[0].ID != "a1" {
		t.Errorf("jobs = %+v, want only a1", jobs)
	}
}

func TestSubmitCreatesJobSetInNamespace(t *testing.T) {
	c := newKube()
	js := jobSet("job-new", "new", nil)

	if err := c.Submit(context.Background(), js); err != nil {
		t.Fatal(err)
	}

	got, err := c.dyn.Resource(JobSetGVR).Namespace("training").Get(context.Background(), "job-new", metav1GetOptions)
	if err != nil {
		t.Fatalf("JobSet not created: %v", err)
	}
	if got.GetLabels()["devoops/job-id"] != "new" {
		t.Errorf("labels = %v", got.GetLabels())
	}
}

// The timeline tracker reads every event and workload of the training namespace.
func TestEventsAndWorkloadsListTrainingNamespace(t *testing.T) {
	ev := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event",
		"metadata":       map[string]any{"name": "e1", "namespace": "training"},
		"involvedObject": map[string]any{"kind": "Workload", "name": "jobset-job-abc12345-x"},
	}}
	other := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Event", "metadata": map[string]any{"name": "e2", "namespace": "kube-system"},
	}}
	c := newKube(ev, other, workload("job-abc12345", nil))

	events, err := c.Events(context.Background())
	if err != nil || len(events) != 1 || events[0].GetName() != "e1" {
		t.Errorf("events = %v, err = %v", events, err)
	}
	wls, err := c.Workloads(context.Background())
	if err != nil || len(wls) != 1 {
		t.Errorf("workloads = %v, err = %v", wls, err)
	}
}

func TestListReadsJobDetailsWrittenAtSubmit(t *testing.T) {
	js := jobSet("job-3f9a1c2e-g0001", "3f9a1c2e", nil)
	js.SetLabels(map[string]string{
		model.JobIDLabel: "3f9a1c2e", model.GenerationLabel: "1", model.QueueLabel: "queue-auto",
	})
	js.SetAnnotations(map[string]string{
		model.NameAnnotation: "gpt2-demo", model.ModelAnnotation: "gpt2", model.StrategyAnnotation: "fsdp",
		model.WorldSizeAnnotation: "2", model.DatasetAnnotation: "ds-test/v3", model.ConfigSHA256Annotation: "abc",
		model.HyperparametersAnnotation: `{"epochs":3}`,
	})

	got := listOne(t, newKube(js))

	if got.Name != "gpt2-demo" || got.JobsetName != "job-3f9a1c2e-g0001" || got.Model != "gpt2" || got.Strategy != "fsdp" ||
		got.GPUs != 2 || got.Generation != 1 || got.Queue != "queue-auto" || got.ConfigSHA256 != "abc" {
		t.Errorf("status = %+v", got)
	}
	if got.Dataset == nil || got.Dataset.ID != "ds-test" || got.Dataset.Version != 3 {
		t.Errorf("dataset = %+v", got.Dataset)
	}
	if got.Hyperparameters["epochs"] != 3.0 {
		t.Errorf("hyperparameters = %v", got.Hyperparameters)
	}
	if got.Status != model.StatusQueued {
		t.Errorf("status = %s, want QUEUED", got.Status)
	}
}

func TestSubmitReportsDuplicateNameAsAlreadyExists(t *testing.T) {
	c := newKube(jobSet("job-a-g0001", "a", nil))

	err := c.Submit(context.Background(), jobSet("job-a-g0001", "a", nil))

	if !errors.Is(err, model.ErrAlreadyExists) {
		t.Errorf("err = %v, want ErrAlreadyExists", err)
	}
}
