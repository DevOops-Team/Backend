package timeline

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/yaml"
)

func obj(t *testing.T, y string) *unstructured.Unstructured {
	t.Helper()
	// Decode like the dynamic client does (integers stay int64).
	raw, err := yaml.YAMLToJSON([]byte(y))
	if err != nil {
		t.Fatal(err)
	}
	m := map[string]any{}
	if err := utiljson.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return &unstructured.Unstructured{Object: m}
}

const quotaReserved = `
apiVersion: v1
kind: Event
metadata: {name: x.1, namespace: training, uid: ev-1, resourceVersion: "10"}
involvedObject: {kind: Workload, name: jobset-job-c89e2220-0703d}
reason: QuotaReserved
message: Quota reserved in ClusterQueue queue-auto, wait time since queued was 0s
type: Normal
source: {component: kueue-admission}
firstTimestamp: "2026-10-29T02:12:40Z"
lastTimestamp: "2026-10-29T02:12:40Z"
count: 1
`

func TestFromEventMapsKueueEventToJob(t *testing.T) {
	e, ok := FromEvent(obj(t, quotaReserved), "onprem")
	if !ok {
		t.Fatal("expected the event to belong to job c89e2220")
	}
	if e.JobID != "c89e2220" || e.Cluster != "onprem" || e.Source != "kueue-admission" || e.Reason != "QuotaReserved" {
		t.Errorf("got %+v", e)
	}
	if e.Object != "Workload/jobset-job-c89e2220-0703d" || e.Count != 1 || e.Type != "Normal" {
		t.Errorf("got %+v", e)
	}
	if e.Key != "2026-10-29T02:12:40.000Z#onprem#ev-1" {
		t.Errorf("key = %q; must sort by time and be stable for the same event", e.Key)
	}
}

// Newer events (events.k8s.io via core/v1) carry eventTime, reportingController and series instead.
func TestFromEventReadsEventTimeAndSeries(t *testing.T) {
	ev := obj(t, `
apiVersion: v1
kind: Event
metadata: {name: x.2, namespace: training, uid: ev-2}
involvedObject: {kind: Pod, name: job-c89e2220-trainer-0-0-h7ccm}
reason: FailedScheduling
message: 0/1 nodes are available
type: Warning
reportingComponent: default-scheduler
eventTime: "2026-10-29T02:13:01.123456Z"
series: {count: 4, lastObservedTime: "2026-10-29T02:14:01.000000Z"}
`)
	e, ok := FromEvent(ev, "devoops-a")
	if !ok {
		t.Fatal("pod event should map to its job")
	}
	if e.Source != "default-scheduler" || e.Count != 4 || e.Type != "Warning" {
		t.Errorf("got %+v", e)
	}
	if !e.Time.Equal(time.Date(2026, 10, 29, 2, 13, 1, 123000000, time.UTC)) || e.LastTime.Before(e.Time) {
		t.Errorf("time = %v, last = %v", e.Time, e.LastTime)
	}
}

func TestFromEventIgnoresObjectsOfNoJob(t *testing.T) {
	ev := obj(t, `
apiVersion: v1
kind: Event
metadata: {name: x.3, uid: ev-3}
involvedObject: {kind: Pod, name: gpu-placeholder-abc}
reason: Scheduled
firstTimestamp: "2026-10-29T02:13:01Z"
`)
	if _, ok := FromEvent(ev, "devoops-a"); ok {
		t.Error("events of non-job objects must be skipped")
	}
}

const multikueueWorkload = `
apiVersion: kueue.x-k8s.io/v1beta2
kind: Workload
metadata: {name: jobset-job-c89e2220-0703d, namespace: training}
status:
  admissionChecks:
  - name: multikueue-auto
    state: Ready
    message: The workload got reservation on "devoops-a"
    lastTransitionTime: "2026-10-29T02:12:42Z"
  conditions:
  - type: QuotaReserved
    status: "True"
    lastTransitionTime: "2026-10-29T02:12:40Z"
  - type: Finished
    status: "True"
    reason: Succeeded
    message: jobset completed successfully
    lastTransitionTime: "2026-10-29T02:19:20Z"
`

// MultiKueue reports which worker took the job only in the Workload status, not in events.
func TestFromWorkloadRecordsMultiKueueDispatchAndFinish(t *testing.T) {
	evs := FromWorkload(obj(t, multikueueWorkload), "onprem")
	if len(evs) != 2 {
		t.Fatalf("want admission check + finished, got %+v", evs)
	}
	ac := evs[0]
	if ac.JobID != "c89e2220" || ac.Source != "multikueue" || ac.Reason != "Ready" || ac.Message != `The workload got reservation on "devoops-a"` {
		t.Errorf("admission check event = %+v", ac)
	}
	if ac.Key != "2026-10-29T02:12:42.000Z#onprem#wl-jobset-job-c89e2220-0703d-ac-multikueue-auto-Ready" {
		t.Errorf("key = %q", ac.Key)
	}
	if fin := evs[1]; fin.Source != "kueue" || fin.Reason != "Finished" || fin.Message != "Succeeded: jobset completed successfully" {
		t.Errorf("finished event = %+v", fin)
	}
}

// Each admission check state is its own entry, so Pending → Ready shows as two steps.
func TestFromWorkloadKeysDependOnCheckState(t *testing.T) {
	pending := obj(t, `
apiVersion: kueue.x-k8s.io/v1beta2
kind: Workload
metadata: {name: jobset-job-c89e2220-0703d}
status:
  admissionChecks:
  - {name: multikueue-auto, state: Pending, lastTransitionTime: "2026-10-29T02:12:40Z"}
`)
	a := FromWorkload(pending, "onprem")[0].Key
	b := FromWorkload(obj(t, multikueueWorkload), "onprem")[0].Key
	if a == b {
		t.Error("Pending and Ready must be separate timeline entries")
	}
}
