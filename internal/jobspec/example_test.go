package jobspec

import (
	"os"
	"testing"
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
