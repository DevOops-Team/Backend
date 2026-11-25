package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/DevOops-Team/Backend/internal/model"
)

// GroupVersionResources the backend reads and writes on the on-prem management cluster.
var (
	JobSetGVR   = schema.GroupVersionResource{Group: "jobset.x-k8s.io", Version: "v1alpha2", Resource: "jobsets"}
	WorkloadGVR = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "workloads"}
	EventGVR    = schema.GroupVersionResource{Version: "v1", Resource: "events"}
)

// Kube submits JobSets to the on-prem management cluster and reads their state (JobSet + Workload → status, Events for the timeline).
type Kube struct {
	dyn       dynamic.Interface
	namespace string
}

// NewKube connects using a kubeconfig file and optional context (empty = current context).
func NewKube(kubeconfig, kubeContext, namespace string) (*Kube, error) {
	rules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: kubeconfig}
	if kubeconfig == "" {
		rules = clientcmd.NewDefaultClientConfigLoadingRules()
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{CurrentContext: kubeContext},
	).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Kube{dyn: dyn, namespace: namespace}, nil
}

func (c *Kube) Submit(ctx context.Context, js *unstructured.Unstructured) error {
	_, err := c.dyn.Resource(JobSetGVR).Namespace(c.namespace).Create(ctx, js, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("%w: %s", model.ErrAlreadyExists, err.Error())
	}
	return err
}

func (c *Kube) List(ctx context.Context) ([]model.JobStatus, error) {
	jobsets, err := c.dyn.Resource(JobSetGVR).Namespace(c.namespace).List(ctx, metav1.ListOptions{LabelSelector: model.JobIDLabel})
	if err != nil {
		return nil, fmt.Errorf("list jobsets: %w", err)
	}
	workloads, err := c.dyn.Resource(WorkloadGVR).Namespace(c.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("list workloads: %w", err)
	}

	byOwner := map[string]*unstructured.Unstructured{}
	for i := range workloads.Items {
		wl := &workloads.Items[i]
		for _, ref := range wl.GetOwnerReferences() {
			if ref.Kind == model.JobSetKind {
				byOwner[ref.Name] = wl
			}
		}
	}

	out := make([]model.JobStatus, 0, len(jobsets.Items))
	for i := range jobsets.Items {
		js := &jobsets.Items[i]
		id := js.GetLabels()[model.JobIDLabel]
		if id == "" {
			continue
		}
		out = append(out, status(id, js, byOwner[js.GetName()]))
	}
	return out, nil
}

// Delete removes every generation's JobSet of the job. Background propagation lets the garbage collector
// remove the Kueue Workload and the Jobs; MultiKueue then removes the copies on the worker cluster.
// The backend's Role has delete but not deletecollection, so the JobSets are listed and deleted one by one.
func (c *Kube) Delete(ctx context.Context, jobID string) error {
	res := c.dyn.Resource(JobSetGVR).Namespace(c.namespace)
	l, err := res.List(ctx, metav1.ListOptions{LabelSelector: model.JobIDLabel + "=" + jobID})
	if err != nil {
		return fmt.Errorf("list jobsets: %w", err)
	}
	background := metav1.DeletePropagationBackground
	for _, js := range l.Items {
		err := res.Delete(ctx, js.GetName(), metav1.DeleteOptions{PropagationPolicy: &background})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete jobset %s: %w", js.GetName(), err)
		}
	}
	return nil
}

// Stop ends a user's job (GPU-204). Every JobSet of the job that has not finished first gets the stop
// annotations, so the job watcher reports job.cancelled instead of job.stopped, and is then deleted the same
// way as Delete. The delete is pinned to the annotated object's UID: a JobSet recreated under the same name in
// between is left alone. The stop happens at the delete; the workers' pods go with the Workload.
// It returns the names of the JobSets it stopped (none when the job has nothing left to stop).
func (c *Kube) Stop(ctx context.Context, jobID string, requestedAt time.Time) ([]string, error) {
	res := c.dyn.Resource(JobSetGVR).Namespace(c.namespace)
	l, err := res.List(ctx, metav1.ListOptions{LabelSelector: model.JobIDLabel + "=" + jobID})
	if err != nil {
		return nil, fmt.Errorf("list jobsets: %w", err)
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": map[string]string{
		model.StopReasonAnnotation:      model.StopReasonUser,
		model.StopRequestedAtAnnotation: requestedAt.UTC().Format(time.RFC3339),
	}}})
	if err != nil {
		return nil, err
	}
	background := metav1.DeletePropagationBackground
	var stopped []string
	for i := range l.Items {
		js := &l.Items[i]
		if done, _ := conditionOf(js, "Completed"); done {
			continue
		}
		if done, _ := conditionOf(js, "Failed"); done {
			continue
		}
		name := js.GetName()
		if !stopRequested(js) { // a repeated request keeps the first time
			_, err := res.Patch(ctx, name, types.MergePatchType, patch, metav1.PatchOptions{})
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return stopped, fmt.Errorf("annotate jobset %s: %w", name, err)
			}
		}
		uid := js.GetUID()
		err := res.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &background,
			Preconditions: &metav1.Preconditions{UID: &uid}})
		if err != nil && !apierrors.IsNotFound(err) {
			return stopped, fmt.Errorf("delete jobset %s: %w", name, err)
		}
		stopped = append(stopped, name)
	}
	return stopped, nil
}

// Events lists core/v1 Events of the training namespace (timeline.Source).
func (c *Kube) Events(ctx context.Context) ([]unstructured.Unstructured, error) {
	l, err := c.dyn.Resource(EventGVR).Namespace(c.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return l.Items, nil
}

// Workloads lists Kueue Workloads of the training namespace (timeline.Source).
func (c *Kube) Workloads(ctx context.Context) ([]unstructured.Unstructured, error) {
	l, err := c.dyn.Resource(WorkloadGVR).Namespace(c.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return l.Items, nil
}

func status(id string, js, wl *unstructured.Unstructured) model.JobStatus {
	st := model.JobStatusFromMeta(id, js.GetName(), js.GetLabels(), js.GetAnnotations())
	st.CreatedAt = js.GetCreationTimestamp().Time
	st.Restarts, _, _ = unstructured.NestedInt64(js.Object, "status", "restarts")

	if wl != nil {
		st.Cluster, _, _ = unstructured.NestedString(wl.Object, "status", "clusterName")
		if ok, _ := conditionOf(wl, "Admitted"); ok {
			st.Phase = model.PhaseAdmitted
		} else if _, msg := conditionOf(wl, "QuotaReserved"); msg != "" {
			st.Message = msg // e.g. why it is still waiting
		}
	}

	if active(js) > 0 {
		st.Phase = model.PhaseRunning
	}
	completed, msg := conditionOf(js, "Completed")
	if completed {
		st.Phase, st.Message = model.PhaseSucceeded, msg
	}
	failed, msg := conditionOf(js, "Failed")
	if failed {
		st.Phase, st.Message = model.PhaseFailed, msg
	}
	st.Status = model.StatusOf(st.Phase)
	if stopRequested(js) && !completed && !failed {
		st.Status = model.StatusStopping // being deleted after a stop request (GPU-204)
	}
	return st
}

func stopRequested(js *unstructured.Unstructured) bool {
	return js.GetAnnotations()[model.StopReasonAnnotation] != ""
}

// conditionOf reports whether condition typ is True, and its message either way.
func conditionOf(obj *unstructured.Unstructured, typ string) (bool, string) {
	conds, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conds {
		cm, _ := c.(map[string]any)
		if cm["type"] == typ {
			msg, _ := cm["message"].(string)
			return cm["status"] == "True", msg
		}
	}
	return false, ""
}

func active(js *unstructured.Unstructured) int64 {
	var n int64
	jobs, _, _ := unstructured.NestedSlice(js.Object, "status", "replicatedJobsStatus")
	for _, j := range jobs {
		jm, _ := j.(map[string]any)
		if v, ok := jm["active"].(int64); ok {
			n += v
		}
	}
	return n
}
