package jobspec

import (
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// The example shipped for the frontend must stay accepted by Prepare.
func TestExampleJobYAMLIsAccepted(t *testing.T) {
	manifest, err := os.ReadFile("../../examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}

	js, err := Prepare(manifest, testOptions())
	if err != nil {
		t.Fatalf("examples/job.yaml rejected: %v", err)
	}
	if js.GetName() != "job-abc123" {
		t.Errorf("name = %q", js.GetName())
	}
}

func TestExampleJobYAMLHasNoDuplicateEnvAfterPrepare(t *testing.T) {
	manifest, err := os.ReadFile("../../examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}

	js, err := Prepare(manifest, testOptions())
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	for _, e := range container(t, js)["env"].([]any) {
		name := e.(map[string]any)["name"].(string)
		if seen[name] {
			t.Errorf("duplicate env %s", name)
		}
		seen[name] = true
	}
}

// The example trains with DDP on both GPU nodes: two indexed pods in one job (one gang for Kueue),
// rank 0 found through the JobSet headless service, one pod per node.
func TestExampleJobYAMLRunsDDPOnTwoNodes(t *testing.T) {
	manifest, err := os.ReadFile("../../examples/job.yaml")
	if err != nil {
		t.Fatal(err)
	}

	js, err := Prepare(manifest, testOptions())
	if err != nil {
		t.Fatal(err)
	}

	jobs, _, _ := unstructured.NestedSlice(js.Object, "spec", "replicatedJobs")
	job := jobs[0].(map[string]any)
	for _, f := range []string{"parallelism", "completions"} {
		if v, _, _ := unstructured.NestedInt64(job, "template", "spec", f); v != 2 {
			t.Errorf("%s = %d, want 2 (one pod per GPU node)", f, v)
		}
	}
	if m, _, _ := unstructured.NestedString(job, "template", "spec", "completionMode"); m != "Indexed" {
		t.Errorf("completionMode = %q, want Indexed (JOB_COMPLETION_INDEX is the node rank)", m)
	}

	envs := map[string]map[string]any{}
	for _, e := range container(t, js)["env"].([]any) {
		em := e.(map[string]any)
		envs[em["name"].(string)] = em
	}
	ref, _, _ := unstructured.NestedString(envs["JOBSET_NAME"], "valueFrom", "fieldRef", "fieldPath")
	if ref != "metadata.labels['jobset.sigs.k8s.io/jobset-name']" {
		t.Errorf("JOBSET_NAME fieldPath = %q; MASTER_ADDR is built from it", ref)
	}

	anti, _, _ := unstructured.NestedSlice(podSpec(t, js), "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution")
	if len(anti) == 0 {
		t.Error("expected hostname anti-affinity so the two ranks land on different GPU nodes")
	}
}
