package repository

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/DevOops-Team/Backend/internal/model"
)

func finished(js *unstructured.Unstructured, cond string) *unstructured.Unstructured {
	_ = unstructured.SetNestedSlice(js.Object, []any{map[string]any{"type": cond, "status": "True"}}, "status", "conditions")
	return js
}

func stopClient(objs ...runtime.Object) (*dynamicfake.FakeDynamicClient, *Kube) {
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{JobSetGVR: "JobSetList", WorkloadGVR: "WorkloadList"}, objs...)
	return dyn, &Kube{dyn: dyn, namespace: "training"}
}

// The watcher must see the annotations on the deleted object, so each JobSet is patched before it is deleted.
func TestKubeStopAnnotatesThenDeletesTheUnfinishedJobSets(t *testing.T) {
	dyn, k := stopClient(jobset("job-aa-g0001", "aa"), finished(jobset("job-aa-g0000", "aa"), "Failed"),
		jobset("job-bb-g0001", "bb"))
	at := time.Date(2026, 11, 18, 3, 4, 5, 0, time.FixedZone("KST", 9*3600))

	var deletedAnn map[string]string
	dyn.PrependReactor("delete", "jobsets", func(a k8stesting.Action) (bool, runtime.Object, error) {
		name := a.(k8stesting.DeleteAction).GetName()
		obj, err := dyn.Tracker().Get(JobSetGVR, "training", name)
		if err == nil {
			deletedAnn = obj.(*unstructured.Unstructured).GetAnnotations()
		}
		return false, nil, nil
	})

	stopped, err := k.Stop(t.Context(), "aa", at)
	if err != nil {
		t.Fatal(err)
	}
	if len(stopped) != 1 || stopped[0] != "job-aa-g0001" {
		t.Errorf("stopped %v, want the running generation only", stopped)
	}
	if deletedAnn[model.StopReasonAnnotation] != "user" || deletedAnn[model.StopRequestedAtAnnotation] != "2026-11-17T18:04:05Z" {
		t.Errorf("annotations at delete = %v", deletedAnn)
	}
	l, _ := dyn.Resource(JobSetGVR).Namespace("training").List(t.Context(), metav1.ListOptions{})
	var left []string
	for _, js := range l.Items {
		left = append(left, js.GetName())
	}
	if len(left) != 2 {
		t.Errorf("left %v, want the failed generation and the other job", left)
	}

	stopped, err = k.Stop(t.Context(), "aa", at)
	if err != nil || len(stopped) != 0 {
		t.Errorf("second stop = %v, %v; want nothing to stop", stopped, err)
	}
}

func TestStatusShowsStoppingWhileTheJobSetIsBeingDeleted(t *testing.T) {
	js := jobset("job-aa-g0001", "aa")
	js.SetAnnotations(map[string]string{model.StopReasonAnnotation: "user"})
	if st := status("aa", js, nil); st.Status != model.StatusStopping {
		t.Errorf("status = %s, want STOPPING", st.Status)
	}
	if st := status("aa", finished(js, "Completed"), nil); st.Status != model.StatusSucceeded {
		t.Errorf("finished = %s, want SUCCEEDED", st.Status)
	}
}
