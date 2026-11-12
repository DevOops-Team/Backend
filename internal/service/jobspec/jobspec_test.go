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
	s = s.WithImages("ghcr.io/devoops-team/trainer@sha256:t1", "ghcr.io/devoops-team/checkpoint@sha256:c1")
	return Input{
		JobID: "3f9a1c2e", Generation: 1, Name: "gpt2-demo", Model: "gpt2", Strategy: "fsdp", GPUs: 2,
		DatasetID: "3f9a1c2e", DatasetVersion: 1, DatasetManifestSHA256: "6b04", ConfigSHA256: "f7be89cf",
		CheckpointFormat: "fsdp-dcp", Hyperparameters: map[string]any{"epochs": 3.0}, Queue: "queue-light",
		Namespace: "training", Region: "us-east-2", Bucket: "ckpt-bucket", FencingTable: "devoops-prod-fencing",
		ProgressTable: "devoops-prod-job-progress", KafkaBootstrap: "172.30.0.40:9092", KafkaTopic: "job.lifecycle",
		Settings: s,
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
		v, ok := em["value"].(string)
		if !ok && em["valueFrom"] != nil {
			raw, _ := json.Marshal(em["valueFrom"])
			v = string(raw)
		}
		out[em["name"].(string)] = v
	}
	return out
}

func TestRenderNamesJobSetByJobAndGeneration(t *testing.T) {
	d := doc(t, input(t))
	if at(t, d, "metadata", "name") != "job-3f9a1c2e-g0001" || at(t, d, "metadata", "namespace") != "training" {
		t.Errorf("metadata = %v", d["metadata"])
	}
	if at(t, d, "spec", "network", "subdomain") != "job-3f9a1c2e-g0001" {
		t.Errorf("network = %v", at(t, d, "spec", "network"))
	}
}

func TestRenderLabelsAndAnnotationsFollowTheTemplate(t *testing.T) {
	d := doc(t, input(t))
	labels := at(t, d, "metadata", "labels").(map[string]any)
	for k, want := range map[string]string{
		"kueue.x-k8s.io/queue-name": "queue-light", "kueue.x-k8s.io/priority-class": "training",
		"devoops/job-id": "3f9a1c2e", "devoops/generation": "1",
	} {
		if labels[k] != want {
			t.Errorf("label %s = %v, want %s", k, labels[k], want)
		}
	}
	ann := at(t, d, "metadata", "annotations").(map[string]any)
	for k, want := range map[string]string{
		"devoops/strategy": "fsdp", "devoops/world-size": "2", "devoops/model": "gpt2",
		"devoops/dataset": "3f9a1c2e/v1", "devoops/config-sha256": "f7be89cf", "devoops/name": "gpt2-demo",
	} {
		if ann[k] != want {
			t.Errorf("annotation %s = %v, want %s", k, ann[k], want)
		}
	}
}

func TestRenderOmitsPriorityLabelWhenNotConfigured(t *testing.T) {
	in := input(t)
	in.Settings.PriorityClasses.Training = ""
	if _, ok := at(t, doc(t, in), "metadata", "labels").(map[string]any)["kueue.x-k8s.io/priority-class"]; ok {
		t.Error("priority-class label set without a WorkloadPriorityClass; Kueue would reject the job")
	}
}

func TestRenderNeedsTheTrainerAndCheckpointImages(t *testing.T) {
	in := input(t)
	in.Settings.Images.Checkpoint = ""
	if _, err := Render(in); err == nil {
		t.Error("rendered a JobSet without ckpt-init: pods would start with no data and no fencing check")
	}
}

func TestRenderDisablesEveryAutomaticRetry(t *testing.T) {
	d := doc(t, input(t))
	if at(t, d, "spec", "failurePolicy", "maxRestarts") != 0.0 {
		t.Errorf("maxRestarts = %v", at(t, d, "spec", "failurePolicy", "maxRestarts"))
	}
	job := at(t, d, "spec", "replicatedJobs", 0, "template", "spec").(map[string]any)
	if job["backoffLimit"] != 0.0 || podSpec(t, d)["restartPolicy"] != "Never" {
		t.Errorf("job = %v", job)
	}
	args := container(t, podSpec(t, d)["containers"], "trainer")["args"].([]any)[0].(string)
	if !strings.Contains(args, "--max-restarts=0") {
		t.Errorf("args = %s", args)
	}
}

func TestRenderSizesTheJobByGPUs(t *testing.T) {
	d := doc(t, input(t))
	rj := at(t, d, "spec", "replicatedJobs", 0).(map[string]any)
	if rj["name"] != "workers" || rj["replicas"] != 1.0 {
		t.Errorf("replicatedJob = %v", rj)
	}
	spec := at(t, rj, "template", "spec").(map[string]any)
	if spec["completions"] != 2.0 || spec["parallelism"] != 2.0 || spec["completionMode"] != "Indexed" {
		t.Errorf("spec = %v", spec)
	}
}

func TestRenderRunsTrainPyFromTheTrainerImageAndKeepsStopExitCode(t *testing.T) {
	trainer := container(t, podSpec(t, doc(t, input(t)))["containers"], "trainer")
	if trainer["image"] != "ghcr.io/devoops-team/trainer@sha256:t1" {
		t.Errorf("image = %v", trainer["image"])
	}
	args := trainer["args"].([]any)[0].(string)
	for _, want := range []string{
		"torchrun --nnodes=2 --nproc-per-node=1 --node-rank=\"$JOB_COMPLETION_INDEX\"",
		"--master-addr=job-3f9a1c2e-g0001-workers-0-0.job-3f9a1c2e-g0001", "--master-port=29500",
		"/opt/train/train.py", `if [ -f "$CKPT_DIR/STOP" ]; then exit 75; fi`,
	} {
		if !strings.Contains(args, want) {
			t.Errorf("args missing %q:\n%s", want, args)
		}
	}
	if strings.Contains(args, "pip install") || strings.Contains(args, "epochs") {
		t.Errorf("no runtime installs, and hyperparameters belong in config.json:\n%s", args)
	}
}

func TestRenderPassesRuntimeValuesToTheTrainer(t *testing.T) {
	got := envOf(container(t, podSpec(t, doc(t, input(t)))["containers"], "trainer"))
	for k, want := range map[string]string{
		"JOB_ID": "3f9a1c2e", "GENERATION": "1", "TRAIN_CONFIG": "/code/config.json", "CONFIG_SHA256": "f7be89cf",
		"DATA_DIR": "/data", "CKPT_DIR": "/ckpt", "PROGRESS_FILE": "/progress/progress.json",
	} {
		if got[k] != want {
			t.Errorf("trainer env %s = %q, want %q", k, got[k], want)
		}
	}
}

func TestRenderAddsCkptInitWithConfigAndDataset(t *testing.T) {
	spec := podSpec(t, doc(t, input(t)))
	init := container(t, spec["initContainers"], "ckpt-init")
	if init["image"] != "ghcr.io/devoops-team/checkpoint@sha256:c1" || init["restartPolicy"] != nil {
		t.Errorf("ckpt-init = %v; it runs once before the others", init)
	}
	if strings.Join(toStrings(init["command"]), " ") != "python -m ckpt.init_main" {
		t.Errorf("command = %v", init["command"])
	}
	got := envOf(init)
	for k, want := range map[string]string{
		"JOB_ID": "3f9a1c2e", "GENERATION": "1", "CKPT_BUCKET": "ckpt-bucket", "FENCING_TABLE": "devoops-prod-fencing",
		"CONFIG_URI": "s3://ckpt-bucket/code/3f9a1c2e/config.json", "CONFIG_SHA256": "f7be89cf",
		"DATASET_URI": "s3://ckpt-bucket/datasets/3f9a1c2e/v1/", "DATASET_MANIFEST_SHA256": "6b04",
		"CHECKPOINT_FORMAT": "fsdp-dcp", "WORLD_SIZE": "2", "CODE_DIR": "/code", "DATA_DIR": "/data", "CKPT_DIR": "/ckpt",
	} {
		if got[k] != want {
			t.Errorf("ckpt-init env %s = %q, want %q", k, got[k], want)
		}
	}
	if !strings.Contains(got["CLUSTER_NAME"], "devoops-cluster-info") {
		t.Errorf("CLUSTER_NAME = %s; the JobSet is copied unchanged, so it must come from the worker ConfigMap", got["CLUSTER_NAME"])
	}
	if !strings.Contains(got["RANK"], "job-completion-index") {
		t.Errorf("RANK = %s", got["RANK"])
	}
}

func TestRenderAddsCkptSidecarThatReportsToKafka(t *testing.T) {
	sc := container(t, podSpec(t, doc(t, input(t)))["initContainers"], "ckpt-sidecar")
	if sc["restartPolicy"] != "Always" || sc["command"] != nil {
		t.Errorf("ckpt-sidecar = %v; native sidecar on the image's default command", sc)
	}
	got := envOf(sc)
	for k, want := range map[string]string{
		"KAFKA_BOOTSTRAP": "172.30.0.40:9092", "KAFKA_TOPIC_LIFECYCLE": "job.lifecycle", "IMAGE_DIGEST": "sha256:t1",
		"PROGRESS_FILE": "/progress/progress.json", "WORLD_SIZE": "2", "OUTBOX_PREFIX": "",
	} {
		if got[k] != want {
			t.Errorf("ckpt-sidecar env %s = %q, want %q", k, got[k], want)
		}
	}
}

func TestRenderSharesPodLocalVolumesAsUser1000(t *testing.T) {
	spec := podSpec(t, doc(t, input(t)))
	sc := spec["securityContext"].(map[string]any)
	if sc["runAsUser"] != 1000.0 || sc["fsGroup"] != 1000.0 {
		t.Errorf("securityContext = %v; the ckpt-agent image runs as 1000", sc)
	}
	vols := map[string]map[string]any{}
	for _, v := range spec["volumes"].([]any) {
		vm := v.(map[string]any)
		if vm["persistentVolumeClaim"] != nil {
			t.Errorf("volume %v: the agreed template uses emptyDir + init download, not S3 mounts", vm)
		}
		vols[vm["name"].(string)] = vm
	}
	for _, name := range []string{"ckpt", "data", "code", "progress", "dshm"} {
		if vols[name] == nil {
			t.Errorf("volume %s missing", name)
		}
	}
	mounts := map[string]bool{}
	for _, m := range container(t, spec["containers"], "trainer")["volumeMounts"].([]any) {
		mm := m.(map[string]any)
		mounts[mm["mountPath"].(string)] = mm["readOnly"] == true
	}
	if !mounts["/code"] || !mounts["/data"] || mounts["/ckpt"] {
		t.Errorf("trainer mounts = %v; code · data read-only, ckpt writable", mounts)
	}
}

func TestRenderPlacesPodsInOneZoneOnePerNode(t *testing.T) {
	aff := podSpec(t, doc(t, input(t)))["affinity"].(map[string]any)
	zone := at(t, aff, "podAffinity", "requiredDuringSchedulingIgnoredDuringExecution", 0).(map[string]any)
	if zone["topologyKey"] != "topology.kubernetes.io/zone" {
		t.Errorf("zone term = %v", zone)
	}
	node := at(t, aff, "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution", 0).(map[string]any)
	if node["topologyKey"] != "kubernetes.io/hostname" {
		t.Errorf("node term = %v", node)
	}
}

func TestRenderUsesPodSettingsAndServiceAccount(t *testing.T) {
	spec := podSpec(t, doc(t, input(t)))
	if spec["terminationGracePeriodSeconds"] != 60.0 || spec["serviceAccountName"] != "trainer" {
		t.Errorf("grace = %v, sa = %v", spec["terminationGracePeriodSeconds"], spec["serviceAccountName"])
	}
	res := container(t, spec["containers"], "trainer")["resources"].(map[string]any)
	if at(t, res, "requests", "cpu") != "3" || at(t, res, "limits", "nvidia.com/gpu") != "1" {
		t.Errorf("resources = %v", res)
	}
}

func TestRenderAddsProgressReporterAsNativeSidecar(t *testing.T) {
	sc := container(t, podSpec(t, doc(t, input(t)))["initContainers"], "progress-reporter")
	if sc["restartPolicy"] != "Always" {
		t.Errorf("restartPolicy = %v", sc["restartPolicy"])
	}
	got := envOf(sc)
	for k, want := range map[string]string{
		"JOB_ID": "3f9a1c2e", "GENERATION": "1", "PROGRESS_TABLE": "devoops-prod-job-progress", "HOME": "/tmp",
	} {
		if got[k] != want {
			t.Errorf("env %s = %q, want %q", k, got[k], want)
		}
	}
}

func TestRenderLeavesOutProgressReporterWithoutTable(t *testing.T) {
	in := input(t)
	in.ProgressTable = ""
	for _, c := range podSpec(t, doc(t, in))["initContainers"].([]any) {
		if c.(map[string]any)["name"] == "progress-reporter" {
			t.Error("progress-reporter added without a table to write to")
		}
	}
}

func toStrings(v any) []string {
	var out []string
	for _, s := range v.([]any) {
		out = append(out, s.(string))
	}
	return out
}
