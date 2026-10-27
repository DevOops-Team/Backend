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
