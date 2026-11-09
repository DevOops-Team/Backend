package jobspec

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/DevOops-Team/Backend/internal/settings"
)

func input(t *testing.T) Input {
	t.Helper()
	s, err := settings.Load("")
	if err != nil {
		t.Fatal(err)
	}
	return Input{
		JobID: "3f9a1c2e", Generation: 1, Name: "gpt2-demo", Model: "gpt2", Strategy: "fsdp", GPUs: 2,
		DatasetID: "ds-3f9a", DatasetVersion: 1, ConfigSHA256: "f7be89cf", CheckpointFormat: "fsdp-dcp",
		Hyperparameters: map[string]any{"epochs": 3.0}, ModelSubPath: "_shared/models/gpt2", Queue: "queue-auto",
		Namespace: "training", Region: "us-east-2", ProgressTable: "devoops-job-progress", Settings: s,
	}
}

// doc renders through JSON, the same bytes that are stored in S3 and sent to the API server.
func doc(t *testing.T, in Input) map[string]any {
	t.Helper()
	js, err := Render(in)
	if err != nil {
		t.Fatal(err)
	}
	js.DeepCopy() // panics on values a real client cannot handle
	raw, err := json.Marshal(js.Object)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func at(t *testing.T, v any, path ...any) any {
	t.Helper()
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("%v: not an object at %q", path, k)
			}
			v = m[k]
		case int:
			s, ok := v.([]any)
			if !ok || k >= len(s) {
				t.Fatalf("%v: no index %d", path, k)
			}
			v = s[k]
		}
	}
	return v
}

func podSpec(t *testing.T, d map[string]any) map[string]any {
	t.Helper()
	return at(t, d, "spec", "replicatedJobs", 0, "template", "spec", "template", "spec").(map[string]any)
}

func container(t *testing.T, list any, name string) map[string]any {
	t.Helper()
	for _, c := range list.([]any) {
		if cm := c.(map[string]any); cm["name"] == name {
			return cm
		}
	}
	t.Fatalf("container %s not found", name)
	return nil
}

func envOf(c map[string]any) map[string]string {
	out := map[string]string{}
	for _, e := range c["env"].([]any) {
		em := e.(map[string]any)
		v, _ := em["value"].(string)
		out[em["name"].(string)] = v
	}
	return out
}

func TestRenderNamesJobSetByJobAndGeneration(t *testing.T) {
	d := doc(t, input(t))

	if at(t, d, "metadata", "name") != "job-3f9a1c2e-g0001" || at(t, d, "metadata", "namespace") != "training" {
		t.Errorf("metadata = %v", d["metadata"])
	}
	if at(t, d, "spec", "network", "subdomain") != "job-3f9a1c2e-g0001" || at(t, d, "spec", "network", "enableDNSHostnames") != true {
		t.Errorf("network = %v", at(t, d, "spec", "network"))
	}
}

func TestRenderLabelsAndAnnotationsFollowTheTemplate(t *testing.T) {
	d := doc(t, input(t))

	labels := at(t, d, "metadata", "labels").(map[string]any)
	for k, want := range map[string]string{
		"kueue.x-k8s.io/queue-name": "queue-auto", "devoops/job-id": "3f9a1c2e", "devoops/generation": "1",
	} {
		if labels[k] != want {
			t.Errorf("label %s = %v, want %s", k, labels[k], want)
		}
	}
	ann := at(t, d, "metadata", "annotations").(map[string]any)
	for k, want := range map[string]string{
		"devoops/strategy": "fsdp", "devoops/world-size": "2", "devoops/model": "gpt2",
		"devoops/dataset": "ds-3f9a/v1", "devoops/config-sha256": "f7be89cf", "devoops/name": "gpt2-demo",
		"devoops/hyperparameters": `{"epochs":3}`,
	} {
		if ann[k] != want {
			t.Errorf("annotation %s = %v, want %s", k, ann[k], want)
		}
	}
}

func TestRenderOmitsPriorityLabelWhenInfraHasNoPriorityClass(t *testing.T) {
	d := doc(t, input(t))
	if _, ok := at(t, d, "metadata", "labels").(map[string]any)["kueue.x-k8s.io/priority-class"]; ok {
		t.Error("priority-class label set, but no WorkloadPriorityClass exists; Kueue would reject the job")
	}

	in := input(t)
	in.Settings.PriorityClasses.Training = "training"
	d = doc(t, in)
	if at(t, d, "metadata", "labels", "kueue.x-k8s.io/priority-class") != "training" {
		t.Error("priority-class label missing when configured")
	}
}

func TestRenderDisablesEveryAutomaticRetry(t *testing.T) {
	d := doc(t, input(t))

	if at(t, d, "spec", "failurePolicy", "maxRestarts") != 0.0 {
		t.Errorf("maxRestarts = %v", at(t, d, "spec", "failurePolicy", "maxRestarts"))
	}
	job := at(t, d, "spec", "replicatedJobs", 0, "template", "spec")
	if at(t, job, "backoffLimit") != 0.0 || at(t, job, "template", "spec", "restartPolicy") != "Never" {
		t.Errorf("job = %v", job)
	}
	args := at(t, container(t, podSpec(t, d)["containers"], "trainer"), "args", 0).(string)
	if !strings.Contains(args, "--max-restarts=0") {
		t.Errorf("args = %s", args)
	}
}

func TestRenderSizesTheJobByGPUs(t *testing.T) {
	d := doc(t, input(t))

	rj := at(t, d, "spec", "replicatedJobs", 0).(map[string]any)
	if rj["name"] != "workers" {
		t.Errorf("name = %v", rj["name"])
	}
	spec := at(t, rj, "template", "spec")
	if at(t, spec, "completionMode") != "Indexed" || at(t, spec, "completions") != 2.0 || at(t, spec, "parallelism") != 2.0 {
		t.Errorf("spec = %v", spec)
	}
}

func TestRenderRunsTorchrunOnTrainPyFromS3(t *testing.T) {
	trainer := container(t, podSpec(t, doc(t, input(t)))["containers"], "trainer")

	args := at(t, trainer, "args", 0).(string)
	for _, want := range []string{
		"exec torchrun --nnodes=2 --nproc-per-node=1", `--node-rank="$JOB_COMPLETION_INDEX"`,
		"--master-addr=job-3f9a1c2e-g0001-workers-0-0.job-3f9a1c2e-g0001", "--master-port=29500", "/code/train.py",
	} {
		if !strings.Contains(args, want) {
			t.Errorf("args missing %q:\n%s", want, args)
		}
	}
	if strings.Contains(args, "--epochs") || strings.Contains(args, "--total-steps") {
		t.Errorf("hyperparameters belong in config.json, not args:\n%s", args)
	}
}

func TestRenderPassesRuntimeValuesAsEnv(t *testing.T) {
	trainer := container(t, podSpec(t, doc(t, input(t)))["containers"], "trainer")

	got := envOf(trainer)
	for k, want := range map[string]string{
		"JOB_ID": "3f9a1c2e", "GENERATION": "1", "AWS_REGION": "us-east-2", "TRAIN_CONFIG": "/code/config.json",
		"CONFIG_SHA256": "f7be89cf", "DATA_DIR": "/data", "CKPT_DIR": "/ckpt", "PROGRESS_FILE": "/progress/progress.json",
		"MODEL_DIR": "/model", "CKPT_LOCAL_DIR": "/ckpt-local",
	} {
		if got[k] != want {
			t.Errorf("env %s = %q, want %q", k, got[k], want)
		}
	}
}

func TestRenderMountsS3ThroughInfraVolumes(t *testing.T) {
	spec := podSpec(t, doc(t, input(t)))
	trainer := container(t, spec["containers"], "trainer")

	want := map[string][3]any{ // mountPath → claim, subPath, readOnly
		"/code":  {"s3-code", "3f9a1c2e", true},
		"/data":  {"s3-datasets", "ds-3f9a/v1", true},
		"/model": {"s3-datasets", "_shared/models/gpt2", true},
		"/ckpt":  {"s3-checkpoints", "3f9a1c2e", nil},
	}
	found := 0
	for _, m := range trainer["volumeMounts"].([]any) {
		mm := m.(map[string]any)
		w, ok := want[mm["mountPath"].(string)]
		if !ok {
			continue
		}
		found++
		if mm["name"] != w[0] || mm["subPath"] != w[1] || mm["readOnly"] != w[2] {
			t.Errorf("mount %v, want %v", mm, w)
		}
	}
	if found != len(want) {
		t.Errorf("found %d S3 mounts, want %d: %v", found, len(want), trainer["volumeMounts"])
	}
}

func TestRenderPlacesPodsInOneZoneOnePerNode(t *testing.T) {
	spec := podSpec(t, doc(t, input(t)))

	zone := at(t, spec, "affinity", "podAffinity", "requiredDuringSchedulingIgnoredDuringExecution", 0)
	if at(t, zone, "topologyKey") != "topology.kubernetes.io/zone" || at(t, zone, "labelSelector", "matchLabels", "devoops/gpu-group") != "train" {
		t.Errorf("zone term = %v", zone)
	}
	if _, ok := zone.(map[string]any)["namespaceSelector"]; !ok {
		t.Error("namespaceSelector missing: placeholders live in gpu-standby and must share the zone")
	}
	node := at(t, spec, "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution", 0)
	if at(t, node, "topologyKey") != "kubernetes.io/hostname" {
		t.Errorf("node term = %v", node)
	}
	if at(t, spec, "tolerations", 0, "operator") != "Exists" {
		t.Errorf("tolerations = %v", spec["tolerations"])
	}
}

func TestRenderUsesPodSettings(t *testing.T) {
	spec := podSpec(t, doc(t, input(t)))
	trainer := container(t, spec["containers"], "trainer")

	if spec["terminationGracePeriodSeconds"] != 60.0 {
		t.Errorf("grace = %v", spec["terminationGracePeriodSeconds"])
	}
	if at(t, trainer, "resources", "requests", "cpu") != "3" || at(t, trainer, "resources", "requests", "memory") != "12Gi" ||
		at(t, trainer, "resources", "limits", "memory") != "14Gi" || at(t, trainer, "resources", "limits", "nvidia.com/gpu") != "1" {
		t.Errorf("resources = %v", trainer["resources"])
	}
	if trainer["image"] != "pytorch/pytorch:2.5.1-cuda12.4-cudnn9-runtime" {
		t.Errorf("image = %v", trainer["image"])
	}
}

func TestRenderOmitsServiceAccountUntilInfraCreatesIt(t *testing.T) {
	if _, ok := podSpec(t, doc(t, input(t)))["serviceAccountName"]; ok {
		t.Error("serviceAccountName set, but the infra has no trainer ServiceAccount")
	}
	in := input(t)
	in.Settings.ServiceAccount = "trainer"
	if podSpec(t, doc(t, in))["serviceAccountName"] != "trainer" {
		t.Error("serviceAccountName ignored when configured")
	}
}

func TestRenderAddsProgressReporterAsNativeSidecar(t *testing.T) {
	spec := podSpec(t, doc(t, input(t)))
	sc := container(t, spec["initContainers"], "progress-reporter")

	if sc["restartPolicy"] != "Always" {
		t.Errorf("restartPolicy = %v", sc["restartPolicy"])
	}
	got := envOf(sc)
	for k, want := range map[string]string{
		"JOB_ID": "3f9a1c2e", "GENERATION": "1", "AWS_REGION": "us-east-2",
		"PROGRESS_TABLE": "devoops-job-progress", "PROGRESS_FILE": "/progress/progress.json",
	} {
		if got[k] != want {
			t.Errorf("env %s = %q, want %q", k, got[k], want)
		}
	}
	if !strings.Contains(at(t, sc, "args", 0).(string), "update_item") {
		t.Error("sidecar does not run progress_reporter.py")
	}
}

func TestRenderLeavesOutComponentsNotBuiltYet(t *testing.T) {
	spec := podSpec(t, doc(t, input(t)))
	for _, c := range spec["initContainers"].([]any) {
		if name := c.(map[string]any)["name"]; name == "ckpt-init" || name == "ckpt-sidecar" {
			t.Errorf("%v rendered, but the ckpt-agent image does not exist yet", name)
		}
	}
}
