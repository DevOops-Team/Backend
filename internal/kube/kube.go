// Package kube submits JobSets to the management cluster (k3s) and reads their state.
//
// On the management cluster MultiKueue owns the JobSet (spec.managedBy) and mirrors
// the worker's JobSet status back, so JobSet status reflects the real training run.
// The Kueue Workload tells whether a worker admitted it and which one (status.clusterName).
package kube

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/DevOops-Team/Backend/internal/api"
	"github.com/DevOops-Team/Backend/internal/jobspec"
)

var (
	JobSetGVR   = schema.GroupVersionResource{Group: "jobset.x-k8s.io", Version: "v1alpha2", Resource: "jobsets"}
	WorkloadGVR = schema.GroupVersionResource{Group: "kueue.x-k8s.io", Version: "v1beta2", Resource: "workloads"}

	metav1GetOptions = metav1.GetOptions{}
)

type Client struct {
	dyn       dynamic.Interface
	namespace string
}

// New connects using a kubeconfig file and optional context (empty = current context).
func New(kubeconfig, kubeContext, namespace string) (*Client, error) {
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
	return &Client{dyn: dyn, namespace: namespace}, nil
}

func (c *Client) Submit(ctx context.Context, js *unstructured.Unstructured) error {
	_, err := c.dyn.Resource(JobSetGVR).Namespace(c.namespace).Create(ctx, js, metav1.CreateOptions{})
	return err
}

func (c *Client) List(ctx context.Context) ([]api.JobStatus, error) {
	jobsets, err := c.dyn.Resource(JobSetGVR).Namespace(c.namespace).List(ctx, metav1.ListOptions{LabelSelector: jobspec.JobIDLabel})
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
			if ref.Kind == jobspec.JobSetKind {
				byOwner[ref.Name] = wl
			}
		}
	}

	out := make([]api.JobStatus, 0, len(jobsets.Items))
	for i := range jobsets.Items {
		js := &jobsets.Items[i]
		id := js.GetLabels()[jobspec.JobIDLabel]
		if id == "" {
			continue
		}
		out = append(out, status(id, js, byOwner[js.GetName()]))
	}
	return out, nil
}

func status(id string, js, wl *unstructured.Unstructured) api.JobStatus {
	st := api.JobStatus{
		ID:        id,
		Name:      js.GetName(),
		Phase:     api.PhasePending,
		CreatedAt: js.GetCreationTimestamp().Time,
	}
	st.Restarts, _, _ = unstructured.NestedInt64(js.Object, "status", "restarts")

	if wl != nil {
		st.Cluster, _, _ = unstructured.NestedString(wl.Object, "status", "clusterName")
		if ok, _ := conditionOf(wl, "Admitted"); ok {
			st.Phase = api.PhaseAdmitted
		} else if _, msg := conditionOf(wl, "QuotaReserved"); msg != "" {
			st.Message = msg // e.g. why it is still waiting
		}
	}

	if active(js) > 0 {
		st.Phase = api.PhaseRunning
	}
	if ok, msg := conditionOf(js, "Completed"); ok {
		st.Phase, st.Message = api.PhaseSucceeded, msg
	}
	if ok, msg := conditionOf(js, "Failed"); ok {
		st.Phase, st.Message = api.PhaseFailed, msg
	}
	return st
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
