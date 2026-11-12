package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/DevOops-Team/Backend/internal/model"
)

// GroupVersionResources the backend reads and writes on k3s.
var (
	JobSetGVR   = schema.GroupVersionResource{Group: "jobset.x-k8s.io", Version: "v1alpha2", Resource: "jobsets"}
	WorkloadGVR = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "workloads"}
	EventGVR    = schema.GroupVersionResource{Version: "v1", Resource: "events"}
)

// Kube submits JobSets to k3s and reads their state (JobSet + Workload → status, Events for the timeline).
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
	labels, ann := js.GetLabels(), js.GetAnnotations()
	st := model.JobStatus{
		ID:           id,
		Name:         js.GetName(),
		Model:        ann[model.ModelAnnotation],
		Strategy:     ann[model.StrategyAnnotation],
		Generation:   atoi(labels[model.GenerationLabel]),
		GPUs:         atoi(ann[model.WorldSizeAnnotation]),
		JobsetName:   js.GetName(),
		Queue:        labels[model.QueueLabel],
		Dataset:      parseDataset(ann[model.DatasetAnnotation]),
		ConfigSHA256: ann[model.ConfigSHA256Annotation],
		Phase:        model.PhasePending,
		CreatedAt:    js.GetCreationTimestamp().Time,
	}
	if name := ann[model.NameAnnotation]; name != "" {
		st.Name = name
	}
	if raw := ann[model.HyperparametersAnnotation]; raw != "" {
		_ = json.Unmarshal([]byte(raw), &st.Hyperparameters) // only informational
	}
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
	if ok, msg := conditionOf(js, "Completed"); ok {
		st.Phase, st.Message = model.PhaseSucceeded, msg
	}
	if ok, msg := conditionOf(js, "Failed"); ok {
		st.Phase, st.Message = model.PhaseFailed, msg
	}
	st.Status = model.StatusOf(st.Phase)
	return st
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s) // 0 for JobSets that lack the value
	return n
}

// parseDataset reads the devoops/dataset annotation, "<id>/v<version>".
func parseDataset(s string) *model.DatasetRef {
	id, v, ok := strings.Cut(s, "/v")
	version, err := strconv.Atoi(v)
	if !ok || err != nil {
		return nil
	}
	return &model.DatasetRef{ID: id, Version: version}
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
