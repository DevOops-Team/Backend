package jobspec

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func initContainers(t *testing.T, js *unstructured.Unstructured) []any {
	t.Helper()
	ics, _ := podSpec(t, js)["initContainers"].([]any)
	return ics
}

func sidecarOf(t *testing.T, js *unstructured.Unstructured) map[string]any {
	t.Helper()
	for _, c := range initContainers(t, js) {
		if cm := c.(map[string]any); cm["name"] == ProgressSidecarName {
			return cm
		}
	}
	t.Fatalf("no %s init container", ProgressSidecarName)
	return nil
}

func envOf(c map[string]any) map[string]map[string]any {
	out := map[string]map[string]any{}
	for _, e := range slice(c["env"]) {
		em := e.(map[string]any)
		out[em["name"].(string)] = em
	}
	return out
}

func mountPaths(c map[string]any) map[string]string {
	out := map[string]string{}
	for _, m := range slice(c["volumeMounts"]) {
		mm := m.(map[string]any)
		out[mm["name"].(string)] = mm["mountPath"].(string)
	}
	return out
}

// A native sidecar (init container with restartPolicy Always) runs next to the trainer
// and stops by itself when the trainer exits, so the Job can still complete.
func TestPrepareAddsProgressSidecarAsNativeSidecar(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	sc := sidecarOf(t, js)
	if sc["restartPolicy"] != "Always" {
		t.Errorf("restartPolicy = %v, want Always (native sidecar)", sc["restartPolicy"])
	}
	if args := slice(sc["args"]); len(args) == 0 || !strings.Contains(args[len(args)-1].(string), "update_item") {
		t.Error("sidecar must run the progress reporter script")
	}
}

func TestProgressSidecarWritesToProgressTable(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	env := envOf(sidecarOf(t, js))
	for name, want := range map[string]string{
		"JOB_ID":             "abc123",
		"PROGRESS_TABLE":     "devoops-job-progress",
		"PROGRESS_FILE":      "/progress/progress.json",
		"AWS_REGION":         "us-east-1",
		"AWS_DEFAULT_REGION": "us-east-1",
	} {
		if got, _ := env[name]["value"].(string); got != want {
			t.Errorf("sidecar env %s = %q, want %q", name, got, want)
		}
	}
	if ref, _, _ := unstructured.NestedString(env["NODE_NAME"], "valueFrom", "fieldRef", "fieldPath"); ref != "spec.nodeName" {
		t.Errorf("sidecar NODE_NAME fieldPath = %q, want spec.nodeName", ref)
	}
}

// The trainer only writes a local JSON file; the sidecar reads the same emptyDir.
func TestPrepareSharesProgressDirBetweenTrainerAndSidecar(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, v := range slice(podSpec(t, js)["volumes"]) {
		vm := v.(map[string]any)
		if vm["name"] == "progress" {
			_, found = vm["emptyDir"]
		}
	}
	if !found {
		t.Fatal("expected emptyDir volume progress")
	}
	if p := mountPaths(container(t, js))["progress"]; p != "/progress" {
		t.Errorf("trainer mounts progress at %q, want /progress", p)
	}
	if p := mountPaths(sidecarOf(t, js))["progress"]; p != "/progress" {
		t.Errorf("sidecar mounts progress at %q, want /progress", p)
	}
}

// Kueue counts sidecar requests in the quota and the node must still fit the trainer.
func TestProgressSidecarRequestsAreSmall(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	req, _, _ := unstructured.NestedStringMap(sidecarOf(t, js), "resources", "requests")
	if req["cpu"] != "50m" || req["memory"] != "128Mi" {
		t.Errorf("sidecar requests = %v, want cpu 50m, memory 128Mi", req)
	}
}

func TestPrepareKeepsUserInitContainers(t *testing.T) {
	manifest := strings.Replace(twoWorkerJobSet, "            containers:\n",
		"            initContainers:\n            - name: fetch\n              image: busybox\n            containers:\n", 1)

	js, err := Prepare([]byte(manifest), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	ics := initContainers(t, js)
	if len(ics) != 2 || ics[0].(map[string]any)["name"] != "fetch" {
		t.Fatalf("initContainers = %v, want user fetch first then the sidecar", ics)
	}
}
