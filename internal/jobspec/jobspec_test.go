package jobspec

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const twoWorkerJobSet = `
apiVersion: jobset.x-k8s.io/v1alpha2
kind: JobSet
metadata:
  name: my-training
spec:
  replicatedJobs:
  - name: workers
    replicas: 1
    template:
      spec:
        parallelism: 2
        completions: 2
        completionMode: Indexed
        template:
          spec:
            restartPolicy: Never
            containers:
            - name: trainer
              image: pytorch/pytorch:latest
              resources:
                limits:
                  nvidia.com/gpu: 1
`

func testOptions() Options {
	return Options{
		JobID:         "abc123",
		Namespace:     "training",
		QueueName:     "queue-auto",
		ProgressTable: "devoops-job-progress",
		Region:        "us-east-1",
	}
}

func podSpec(t *testing.T, js *unstructured.Unstructured) map[string]any {
	t.Helper()
	jobs, _, _ := unstructured.NestedSlice(js.Object, "spec", "replicatedJobs")
	spec, found, err := unstructured.NestedMap(jobs[0].(map[string]any), "template", "spec", "template", "spec")
	if err != nil || !found {
		t.Fatalf("pod spec not found: %v", err)
	}
	return spec
}

func podLabels(t *testing.T, js *unstructured.Unstructured) map[string]string {
	t.Helper()
	jobs, _, _ := unstructured.NestedSlice(js.Object, "spec", "replicatedJobs")
	labels, _, _ := unstructured.NestedStringMap(jobs[0].(map[string]any), "template", "spec", "template", "metadata", "labels")
	return labels
}

func container(t *testing.T, js *unstructured.Unstructured) map[string]any {
	t.Helper()
	return podSpec(t, js)["containers"].([]any)[0].(map[string]any)
}

func TestPrepareRejectsNonJobSetManifest(t *testing.T) {
	_, err := Prepare([]byte("apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: x\n"), testOptions())

	if err == nil {
		t.Fatal("expected error for kind Job, got nil")
	}
}

func TestPrepareRejectsJobSetWithoutReplicatedJobs(t *testing.T) {
	_, err := Prepare([]byte("apiVersion: jobset.x-k8s.io/v1alpha2\nkind: JobSet\nmetadata:\n  name: x\nspec: {}\n"), testOptions())

	if err == nil {
		t.Fatal("expected error for JobSet without replicatedJobs, got nil")
	}
}

func TestPrepareRejectsInvalidYAML(t *testing.T) {
	_, err := Prepare([]byte("kind: [unclosed"), testOptions())

	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestPrepareNamesJobSetAfterJobIDInTrainingNamespace(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	if js.GetName() != "job-abc123" {
		t.Errorf("name = %q, want job-abc123", js.GetName())
	}
	if js.GetNamespace() != "training" {
		t.Errorf("namespace = %q, want training", js.GetNamespace())
	}
}

func TestPrepareSubmitsToQueueAndLabelsJobID(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	labels := js.GetLabels()
	if labels["kueue.x-k8s.io/queue-name"] != "queue-auto" {
		t.Errorf("queue label = %q, want queue-auto", labels["kueue.x-k8s.io/queue-name"])
	}
	if labels["devoops/job-id"] != "abc123" {
		t.Errorf("job-id label = %q, want abc123", labels["devoops/job-id"])
	}
}

func TestPrepareDefaultsMaxRestartsToThree(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	got, _, _ := unstructured.NestedInt64(js.Object, "spec", "failurePolicy", "maxRestarts")
	if got != 3 {
		t.Errorf("maxRestarts = %d, want 3", got)
	}
}

func TestPrepareKeepsUserMaxRestarts(t *testing.T) {
	manifest := twoWorkerJobSet + "  failurePolicy:\n    maxRestarts: 7\n"

	js, err := Prepare([]byte(manifest), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	got, _, _ := unstructured.NestedInt64(js.Object, "spec", "failurePolicy", "maxRestarts")
	if got != 7 {
		t.Errorf("maxRestarts = %d, want 7", got)
	}
}

func TestPrepareLabelsPodsIntoGPUGroup(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	labels := podLabels(t, js)
	if labels["devoops/gpu-group"] != "train" {
		t.Errorf("gpu-group label = %q, want train", labels["devoops/gpu-group"])
	}
}

func TestPrepareToleratesGPUTaint(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	tolerations := podSpec(t, js)["tolerations"].([]any)
	tol := tolerations[0].(map[string]any)
	if tol["key"] != "nvidia.com/gpu" || tol["effect"] != "NoSchedule" {
		t.Errorf("toleration = %v, want nvidia.com/gpu NoSchedule", tol)
	}
}

func TestPrepareColocatesPodsInOneZoneAcrossNamespaces(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	terms, _, _ := unstructured.NestedSlice(podSpec(t, js), "affinity", "podAffinity", "requiredDuringSchedulingIgnoredDuringExecution")
	term := terms[0].(map[string]any)
	if term["topologyKey"] != "topology.kubernetes.io/zone" {
		t.Errorf("topologyKey = %v, want topology.kubernetes.io/zone", term["topologyKey"])
	}
	if _, ok := term["namespaceSelector"]; !ok {
		t.Error("namespaceSelector missing: standby placeholders live in another namespace")
	}
}

func TestPrepareSpreadsPodsOnePerNode(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	terms, _, _ := unstructured.NestedSlice(podSpec(t, js), "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution")
	if terms[0].(map[string]any)["topologyKey"] != "kubernetes.io/hostname" {
		t.Errorf("anti-affinity topologyKey = %v, want kubernetes.io/hostname", terms[0].(map[string]any)["topologyKey"])
	}
}

func TestPrepareMountsJobScopedS3Volumes(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	mounts := map[string]map[string]any{}
	for _, m := range container(t, js)["volumeMounts"].([]any) {
		mm := m.(map[string]any)
		mounts[mm["mountPath"].(string)] = mm
	}
	want := map[string]string{"/code": "s3-code", "/data": "s3-datasets", "/ckpt": "s3-checkpoints"}
	for path, volume := range want {
		m, ok := mounts[path]
		if !ok {
			t.Errorf("mount %s missing", path)
			continue
		}
		if m["name"] != volume || m["subPath"] != "abc123" {
			t.Errorf("mount %s = %v, want volume %s subPath abc123", path, m, volume)
		}
	}

	claims := map[string]string{}
	for _, v := range podSpec(t, js)["volumes"].([]any) {
		vv := v.(map[string]any)
		if pvc, ok := vv["persistentVolumeClaim"].(map[string]any); ok {
			claims[vv["name"].(string)] = pvc["claimName"].(string)
		}
	}
	for _, volume := range want {
		if claims[volume] != volume {
			t.Errorf("volume %s claim = %q, want %q", volume, claims[volume], volume)
		}
	}
}

func TestPrepareInjectsRuntimeEnv(t *testing.T) {
	js, err := Prepare([]byte(twoWorkerJobSet), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	env := map[string]string{}
	for _, e := range container(t, js)["env"].([]any) {
		ee := e.(map[string]any)
		if v, ok := ee["value"].(string); ok {
			env[ee["name"].(string)] = v
		}
	}
	want := map[string]string{
		"JOB_ID":        "abc123",
		"PROGRESS_FILE": "/progress/progress.json",
		"CODE_DIR":      "/code",
		"DATA_DIR":      "/data",
		"CKPT_DIR":      "/ckpt",
		// boto3(botocore)는 AWS_DEFAULT_REGION만 읽는다. 사용자 코드가 AWS SDK를 쓸 때를 위해 둘 다 넣는다.
		"AWS_REGION":         "us-east-1",
		"AWS_DEFAULT_REGION": "us-east-1",
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("env %s = %q, want %q", k, env[k], v)
		}
	}
	if _, ok := env["PROGRESS_TABLE"]; ok {
		t.Error("trainer must not get PROGRESS_TABLE; the progress sidecar owns DynamoDB writes")
	}
}

func TestPrepareKeepsUserEnv(t *testing.T) {
	manifest := twoWorkerJobSet + "              env:\n              - name: LR\n                value: \"0.01\"\n"

	js, err := Prepare([]byte(manifest), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	found := false
	for _, e := range container(t, js)["env"].([]any) {
		if e.(map[string]any)["name"] == "LR" {
			found = true
		}
	}
	if !found {
		t.Error("user env LR was dropped")
	}
}

func TestPrepareDoesNotDuplicateEnvTheUserAlreadySet(t *testing.T) {
	manifest := twoWorkerJobSet + "              env:\n              - name: AWS_REGION\n                value: eu-west-1\n"

	js, err := Prepare([]byte(manifest), testOptions())
	if err != nil {
		t.Fatal(err)
	}

	count := map[string]int{}
	var region string
	for _, e := range container(t, js)["env"].([]any) {
		ee := e.(map[string]any)
		count[ee["name"].(string)]++
		if ee["name"] == "AWS_REGION" {
			region, _ = ee["value"].(string)
		}
	}
	for name, n := range count {
		if n > 1 {
			t.Errorf("env %s appears %d times; Kubernetes rejects duplicate env names", name, n)
		}
	}
	if region != "us-east-1" {
		t.Errorf("AWS_REGION = %q, want backend value us-east-1 (resources live there)", region)
	}
}
