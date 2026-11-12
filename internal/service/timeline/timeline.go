// Package timeline turns Kubernetes Events and Kueue Workload status into per-job timeline entries.
//
// The same item shape is written by the backend (on-prem management cluster, "onprem") and by the job-watcher
// on every EKS worker (infra cluster-addons/job_watcher.py), so keep the two in sync:
//   - job ID: the first "job-<8 hex>" in the involved object's name (pods, jobs, jobsets, workloads)
//   - sort key: "<time, RFC3339 with ms, UTC>#<cluster>#<id>"; rewriting the same event overwrites it
package timeline

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
)

// KeyTimeFormat is fixed-width so keys sort in time order.
const KeyTimeFormat = model.EventKeyTimeFormat

var jobIDPattern = regexp.MustCompile(`job-([0-9a-f]{8})(?:[^0-9a-f]|$)`)

// JobID extracts the job ID from an object name, e.g. jobset-job-c89e2220-0703d → c89e2220.
func JobID(name string) (string, bool) {
	m := jobIDPattern.FindStringSubmatch(name)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// Key builds the sort key.
func Key(t time.Time, cluster, id string) string {
	return t.UTC().Format(KeyTimeFormat) + "#" + cluster + "#" + id
}

// FromEvent maps a core/v1 Event to the job its involved object belongs to.
func FromEvent(ev *unstructured.Unstructured, cluster string) (model.JobEvent, bool) {
	kind, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "kind")
	name, _, _ := unstructured.NestedString(ev.Object, "involvedObject", "name")
	id, ok := JobID(name)
	if !ok {
		return model.JobEvent{}, false
	}

	first := firstTime(ev, "eventTime", "firstTimestamp")
	if first.IsZero() {
		first = ev.GetCreationTimestamp().Time
	}
	first = first.UTC().Truncate(time.Millisecond) // same precision as the key
	last := firstTime(ev, "series.lastObservedTime", "lastTimestamp")
	if last.Before(first) {
		last = first
	}
	count, _, _ := unstructured.NestedInt64(ev.Object, "series", "count")
	if count == 0 {
		count, _, _ = unstructured.NestedInt64(ev.Object, "count")
	}
	if count == 0 {
		count = 1
	}

	return model.JobEvent{
		JobID:    id,
		Key:      Key(first, cluster, string(ev.GetUID())),
		Time:     first.UTC(),
		LastTime: last.UTC(),
		Cluster:  cluster,
		Source:   firstString(ev, "source.component", "reportingComponent", "reportingController"),
		Reason:   str(ev, "reason"),
		Message:  str(ev, "message"),
		Type:     str(ev, "type"),
		Object:   kind + "/" + name,
		Count:    count,
	}, true
}

// FromWorkload records what Kueue keeps only in Workload status: each admission check state
// (MultiKueue: which worker got the reservation) and the final Finished condition.
func FromWorkload(wl *unstructured.Unstructured, cluster string) []model.JobEvent {
	id, ok := JobID(wl.GetName())
	if !ok {
		return nil
	}
	object := "Workload/" + wl.GetName()
	var out []model.JobEvent

	checks, _, _ := unstructured.NestedSlice(wl.Object, "status", "admissionChecks")
	for _, c := range checks {
		cm, _ := c.(map[string]any)
		name, state := fmt.Sprint(cm["name"]), fmt.Sprint(cm["state"])
		t := parseTime(cm["lastTransitionTime"])
		msg, _ := cm["message"].(string)
		out = append(out, model.JobEvent{
			JobID: id, Key: Key(t, cluster, "wl-"+wl.GetName()+"-ac-"+name+"-"+state),
			Time: t, LastTime: t, Cluster: cluster, Source: "multikueue",
			Reason: state, Message: msg, Type: checkType(state), Object: object, Count: 1,
		})
	}

	conds, _, _ := unstructured.NestedSlice(wl.Object, "status", "conditions")
	for _, c := range conds {
		cm, _ := c.(map[string]any)
		if cm["type"] != "Finished" || cm["status"] != "True" {
			continue
		}
		t := parseTime(cm["lastTransitionTime"])
		reason, _ := cm["reason"].(string)
		msg, _ := cm["message"].(string)
		typ := "Normal"
		if reason != "Succeeded" {
			typ = "Warning"
		}
		out = append(out, model.JobEvent{
			JobID: id, Key: Key(t, cluster, "wl-"+wl.GetName()+"-finished"),
			Time: t, LastTime: t, Cluster: cluster, Source: "kueue",
			Reason: "Finished", Message: reason + ": " + msg, Type: typ, Object: object, Count: 1,
		})
	}
	return out
}

func checkType(state string) string {
	if state == "Rejected" || state == "Retry" {
		return "Warning"
	}
	return "Normal"
}

func str(u *unstructured.Unstructured, field string) string {
	s, _, _ := unstructured.NestedString(u.Object, field)
	return s
}

func firstString(u *unstructured.Unstructured, paths ...string) string {
	for _, p := range paths {
		if s, _, _ := unstructured.NestedString(u.Object, strings.Split(p, ".")...); s != "" {
			return s
		}
	}
	return ""
}

func firstTime(u *unstructured.Unstructured, paths ...string) time.Time {
	for _, p := range paths {
		v, found, _ := unstructured.NestedFieldNoCopy(u.Object, strings.Split(p, ".")...)
		if t := parseTime(v); found && !t.IsZero() {
			return t
		}
	}
	return time.Time{}
}

func parseTime(v any) time.Time {
	s, _ := v.(string)
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}
