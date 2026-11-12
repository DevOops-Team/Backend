// Package jobspec renders the JobSet a training job is submitted as, following Confluence 「JobSet
// 템플릿 (합의안)」 v6 (ADR-12). The object is stored in S3 as jobset.json and sent to the management
// cluster unchanged; recovery re-submits that stored copy with only the §6 values changed.
//
// Pod (rank r, GPU 1):
//
//	[init]    ckpt-init          generation · owner check, config.json → /code, dataset → /data, approved checkpoint → /ckpt
//	[sidecar] ckpt-sidecar       /ckpt → S3, rank 0: manifest · approval · heartbeat · Kafka checkpoint.approved
//	[sidecar] progress-reporter  /progress/progress.json → DynamoDB progress table
//	[main]    trainer            torchrun /opt/train/train.py (inside the trainer image)
//
// The uploaded model folder is part of the dataset (/data/model), so ckpt-init needs no model URI.
package jobspec

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/settings"
)

const (
	replicatedJobName = "workers"

	codeDir    = "/code"
	dataDir    = "/data"
	ckptDir    = "/ckpt"
	trainEntry = "/opt/train/train.py"

	// ClusterInfoConfigMap is the per-worker ConfigMap that tells ckpt-init · ckpt-sidecar which cluster
	// they run on (the JobSet is copied unchanged to every worker, so env values cannot say it).
	ClusterInfoConfigMap = "devoops-cluster-info"

	// stopExitCode: the sidecar wrote /ckpt/STOP (generation changed, approval refused). torchrun turns
	// the worker's 75 into 1, so the entry script restores it (체크포인트 규약 · Data checkpoint/README).
	stopExitCode = 75
)

// Input is everything that varies per job. Render has no side effects, so recovery can render
// the same way from a stored job.
type Input struct {
	JobID                 string
	Generation            int
	Name                  string
	Model                 string
	Strategy              string
	GPUs                  int
	DatasetID             string
	DatasetVersion        int
	DatasetManifestSHA256 string
	ConfigSHA256          string
	CheckpointFormat      string
	Hyperparameters       map[string]any
	Queue                 string
	Namespace             string
	Region                string
	Bucket                string
	FencingTable          string
	ProgressTable         string
	KafkaBootstrap        string
	KafkaTopic            string
	Settings              settings.Settings
}

// Render builds the JobSet. Every value is a JSON type (string, int64, bool, []any, map[string]any)
// so the object can be deep-copied by the Kubernetes client.
func Render(in Input) (*unstructured.Unstructured, error) {
	if in.Settings.Images.Trainer == "" || in.Settings.Images.Checkpoint == "" {
		return nil, fmt.Errorf("trainer and checkpoint images are required (TRAINER_IMAGE, CHECKPOINT_IMAGE)")
	}
	name := model.JobSetName(in.JobID, in.Generation)
	generation := strconv.Itoa(in.Generation)

	hyper := in.Hyperparameters
	if hyper == nil {
		hyper = map[string]any{}
	}
	hyperJSON, err := json.Marshal(hyper)
	if err != nil {
		return nil, fmt.Errorf("encode hyperparameters: %w", err)
	}

	labels := map[string]any{
		model.QueueLabel:      in.Queue,
		model.JobIDLabel:      in.JobID,
		model.GenerationLabel: generation,
	}
	if pc := in.Settings.PriorityClasses.Training; pc != "" {
		labels[model.PriorityClassLabel] = pc
	}

	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": model.JobSetAPIVersion,
		"kind":       model.JobSetKind,
		"metadata": map[string]any{
			"name":      name,
			"namespace": in.Namespace,
			"labels":    labels,
			"annotations": map[string]any{
				model.StrategyAnnotation:        in.Strategy,
				model.WorldSizeAnnotation:       strconv.Itoa(in.GPUs),
				model.ModelAnnotation:           in.Model,
				model.DatasetAnnotation:         fmt.Sprintf("%s/v%d", in.DatasetID, in.DatasetVersion),
				model.ConfigSHA256Annotation:    in.ConfigSHA256,
				model.NameAnnotation:            in.Name,
				model.HyperparametersAnnotation: string(hyperJSON),
			},
		},
		"spec": map[string]any{
			"failurePolicy": map[string]any{"maxRestarts": int64(0)}, // 재시작은 복구 컨트롤러만
			"successPolicy": map[string]any{"operator": "All"},
			"network":       map[string]any{"enableDNSHostnames": true, "subdomain": name},
			"replicatedJobs": []any{map[string]any{
				"name":     replicatedJobName,
				"replicas": int64(1),
				"template": map[string]any{"spec": map[string]any{
					"completionMode": "Indexed", // Pod 번호 0..N-1 = node rank
					"completions":    int64(in.GPUs),
					"parallelism":    int64(in.GPUs),
					"backoffLimit":   int64(0),
					"template": map[string]any{
						"metadata": map[string]any{"labels": map[string]any{
							model.GPUGroupLabel:   model.GPUGroupValue,
							model.JobIDLabel:      in.JobID,
							model.GenerationLabel: generation,
						}},
						"spec": renderPod(in, name, generation),
					},
				}},
			}},
		},
	}}, nil
}

func renderPod(in Input, name, generation string) map[string]any {
	pod := in.Settings.Pod
	group := map[string]any{"matchLabels": map[string]any{model.GPUGroupLabel: model.GPUGroupValue}}
	inits := []any{ckptInit(in, generation), ckptSidecar(in, generation)}
	if in.ProgressTable != "" {
		inits = append(inits, progressSidecar(in, generation))
	}
	spec := map[string]any{
		"restartPolicy":                 "Never",
		"terminationGracePeriodSeconds": pod.TerminationGraceSeconds, // 긴급 저장 없음, 주기 저장만
		"securityContext": map[string]any{ // ckpt-agent runs as 1000 and shares /ckpt with the trainer
			"runAsUser":  pod.RunAsUser,
			"runAsGroup": pod.RunAsUser,
			"fsGroup":    pod.RunAsUser,
		},
		"affinity": map[string]any{
			"podAffinity": required(map[string]any{ // 같은 AZ
				"labelSelector":     group,
				"namespaceSelector": map[string]any{}, // spare placeholders live in another namespace
				"topologyKey":       "topology.kubernetes.io/zone",
			}),
			"podAntiAffinity": required(map[string]any{ // 노드당 1개
				"labelSelector":     group,
				"namespaceSelector": map[string]any{},
				"topologyKey":       "kubernetes.io/hostname",
			}),
		},
		"tolerations": []any{map[string]any{"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"}},
		"volumes": []any{ // 노드 로컬 NVMe emptyDir
			emptyDir("ckpt", map[string]any{"sizeLimit": pod.CkptSize}),
			emptyDir("data", map[string]any{"sizeLimit": pod.DataSize}),
			emptyDir("code", map[string]any{"sizeLimit": "10Mi"}),
			emptyDir(progressVolume, map[string]any{"sizeLimit": "10Mi"}),
			emptyDir("dshm", map[string]any{"medium": "Memory", "sizeLimit": pod.ShmSize}),
		},
		"initContainers": inits,
		"containers":     []any{trainer(in, name, generation)},
	}
	if sa := in.Settings.ServiceAccount; sa != "" {
		spec["serviceAccountName"] = sa
	}
	return spec
}

// agentEnv is what ckpt-init and ckpt-sidecar share (Data checkpoint/ckpt/config.py).
func agentEnv(in Input, generation string) []any {
	return []any{
		env("JOB_ID", in.JobID),
		env("GENERATION", generation),
		env("AWS_REGION", in.Region),
		env("CKPT_BUCKET", in.Bucket),
		env("FENCING_TABLE", in.FencingTable),
		env("CONFIG_SHA256", in.ConfigSHA256),
		env("CHECKPOINT_FORMAT", in.CheckpointFormat),
		env("WORLD_SIZE", strconv.Itoa(in.GPUs)),
		env("CKPT_DIR", ckptDir),
		map[string]any{ // rank 0 sidecar writes the manifest and approves
			"name": "RANK",
			"valueFrom": map[string]any{"fieldRef": map[string]any{
				"fieldPath": "metadata.annotations['batch.kubernetes.io/job-completion-index']",
			}},
		},
		map[string]any{
			"name": "CLUSTER_NAME",
			"valueFrom": map[string]any{"configMapKeyRef": map[string]any{
				"name": ClusterInfoConfigMap, "key": "CLUSTER_NAME",
			}},
		},
	}
}

func ckptInit(in Input, generation string) map[string]any {
	e := append(agentEnv(in, generation),
		env("CONFIG_URI", fmt.Sprintf("s3://%s/code/%s/config.json", in.Bucket, in.JobID)),
		env("DATASET_URI", fmt.Sprintf("s3://%s/datasets/%s/v%d/", in.Bucket, in.DatasetID, in.DatasetVersion)),
		env("DATASET_MANIFEST_SHA256", in.DatasetManifestSHA256),
		env("CODE_DIR", codeDir),
		env("DATA_DIR", dataDir),
	)
	return map[string]any{
		"name":    "ckpt-init",
		"image":   in.Settings.Images.Checkpoint,
		"command": []any{"python", "-m", "ckpt.init_main"},
		"env":     e,
		"volumeMounts": []any{
			mount("code", codeDir, false),
			mount("data", dataDir, false),
			mount("ckpt", ckptDir, false),
		},
		"resources": map[string]any{
			"requests": map[string]any{"cpu": "500m", "memory": "512Mi"},
			"limits":   map[string]any{"memory": "512Mi"},
		},
	}
}

func ckptSidecar(in Input, generation string) map[string]any {
	e := append(agentEnv(in, generation),
		env("PROGRESS_FILE", progressFile),
		env("KAFKA_BOOTSTRAP", in.KafkaBootstrap),
		env("KAFKA_TOPIC_LIFECYCLE", in.KafkaTopic),
		env("IMAGE_DIGEST", imageDigest(in.Settings.Images.Trainer)),
		// S3 outbox for undelivered events needs events-outbox/* write, which the trainer role lacks.
		env("OUTBOX_PREFIX", ""),
		map[string]any{
			"name":      "NODE_NAME",
			"valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "spec.nodeName"}},
		},
	)
	return map[string]any{
		"name":          "ckpt-sidecar",
		"image":         in.Settings.Images.Checkpoint, // 같은 이미지, 기본 CMD = 사이드카
		"restartPolicy": "Always",                      // 네이티브 사이드카
		"env":           e,
		"volumeMounts": []any{
			mount("ckpt", ckptDir, false),
			mount(progressVolume, progressDir, true),
		},
		"resources": map[string]any{
			"requests": map[string]any{"cpu": "250m", "memory": "512Mi"},
			"limits":   map[string]any{"memory": "512Mi"},
		},
	}
}

func trainer(in Input, name, generation string) map[string]any {
	pod := in.Settings.Pod
	master := fmt.Sprintf("%s-%s-0-0.%s", name, replicatedJobName, name)
	script := fmt.Sprintf(`until getent hosts %s >/dev/null; do echo "waiting for %s"; sleep 2; done
torchrun --nnodes=%d --nproc-per-node=1 --node-rank="$JOB_COMPLETION_INDEX" --master-addr=%s --master-port=%d --max-restarts=0 %s
rc=$?
if [ -f "$CKPT_DIR/STOP" ]; then exit %d; fi
exit $rc`, master, master, in.GPUs, master, pod.MasterPort, trainEntry, stopExitCode)

	return map[string]any{
		"name":    "trainer",
		"image":   in.Settings.Images.Trainer, // train.py 포함
		"command": []any{"sh", "-c"},
		"args":    []any{script},
		"ports":   []any{map[string]any{"containerPort": pod.MasterPort}},
		"env": []any{ // 학습 값은 config.json, 실행 환경 값만 env
			env("JOB_ID", in.JobID),
			env("GENERATION", generation),
			env("AWS_REGION", in.Region),
			env("TRAIN_CONFIG", codeDir+"/config.json"),
			env("CONFIG_SHA256", in.ConfigSHA256),
			env("DATA_DIR", dataDir),
			env("CKPT_DIR", ckptDir),
			env("PROGRESS_FILE", progressFile),
			// g4dn.xlarge는 EFA가 없다. 노드 간 NCCL은 Pod 네트워크(eth0) TCP로 한다.
			env("NCCL_SOCKET_IFNAME", "eth0"),
			env("NCCL_IB_DISABLE", "1"),
			env("TOKENIZERS_PARALLELISM", "false"),
			env("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True"),
			env("HOME", "/tmp"), // the pod runs as 1000 with no home; HF caches go here
		},
		"resources": map[string]any{
			"requests": map[string]any{"cpu": pod.CPURequest, "memory": pod.MemoryRequest, "nvidia.com/gpu": "1"},
			"limits":   map[string]any{"memory": pod.MemoryLimit, "nvidia.com/gpu": "1"},
		},
		"volumeMounts": []any{
			mount("code", codeDir, true),
			mount("data", dataDir, true),
			mount("ckpt", ckptDir, false),
			mount(progressVolume, progressDir, false),
			mount("dshm", "/dev/shm", false),
		},
	}
}

// imageDigest returns the sha256:… part of a pinned image, or "".
func imageDigest(image string) string {
	if _, d, ok := strings.Cut(image, "@"); ok {
		return d
	}
	return ""
}

func required(term map[string]any) map[string]any {
	return map[string]any{"requiredDuringSchedulingIgnoredDuringExecution": []any{term}}
}

func emptyDir(name string, opts map[string]any) map[string]any {
	return map[string]any{"name": name, "emptyDir": opts}
}

func mount(volume, path string, readOnly bool) map[string]any {
	m := map[string]any{"name": volume, "mountPath": path}
	if readOnly {
		m["readOnly"] = true
	}
	return m
}

func env(name, value string) map[string]any {
	return map[string]any{"name": name, "value": value}
}
