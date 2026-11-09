// Package jobspec renders the JobSet a training job is submitted as, following the JobSet 템플릿
// (합의안). The object is stored in S3 as jobset.json and sent to the management cluster unchanged.
//
// 임시 구성 (FSDP 연동 테스트). 합의안과 다른 점은 아래뿐이다.
//   - ckpt-init · ckpt-sidecar 없음: ckpt-agent 이미지가 아직 없다.
//   - /code · /data · /ckpt는 emptyDir + 다운로드가 아니라 Mountpoint S3 볼륨이다.
//     GPU 노드 Role에 S3 권한이 없고, S3 권한은 Mountpoint CSI Role에만 있기 때문이다.
//   - train.py는 이미지 안(/opt/train)이 아니라 S3의 /code/train.py다.
//   - /model · MODEL_DIR · /ckpt-local · CKPT_LOCAL_DIR을 추가했다. Mountpoint가 rename을 지원하지 않아
//     DCP는 /ckpt-local에 쓴 뒤 /ckpt로 복사한다.
//   - trainer가 시작할 때 transformers를 설치하고 rank 0 DNS를 기다린다. 공개 이미지를 쓰기 때문이다.
package jobspec

import (
	"encoding/json"
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/settings"
)

const (
	replicatedJobName = "workers"
	trainerPackages   = "transformers==4.46.3" // matches torch 2.5.1 in the trainer image

	codeDir      = "/code"
	dataDir      = "/data"
	modelDir     = "/model"
	ckptDir      = "/ckpt"
	ckptLocalDir = "/ckpt-local"
)

// Input is everything that varies per job. Render has no side effects, so recovery can render
// the same way from a stored job.
type Input struct {
	JobID            string
	Generation       int
	Name             string
	Model            string
	Strategy         string
	GPUs             int
	DatasetID        string
	DatasetVersion   int
	ConfigSHA256     string
	CheckpointFormat string
	Hyperparameters  map[string]any
	ModelSubPath     string
	Queue            string
	Namespace        string
	Region           string
	ProgressTable    string
	Settings         settings.Settings
}

// Render builds the JobSet. Every value is a JSON type (string, int64, bool, []any, map[string]any)
// so the object can be deep-copied by the Kubernetes client.
func Render(in Input) (*unstructured.Unstructured, error) {
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
					"completionMode": "Indexed",
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
	spec := map[string]any{
		"restartPolicy":                 "Never",
		"terminationGracePeriodSeconds": pod.TerminationGraceSeconds,
		"affinity": map[string]any{
			"podAffinity": required(map[string]any{ // 같은 AZ
				"labelSelector":     group,
				"namespaceSelector": map[string]any{}, // placeholders live in gpu-standby
				"topologyKey":       "topology.kubernetes.io/zone",
			}),
			"podAntiAffinity": required(map[string]any{ // 노드당 1개
				"labelSelector":     group,
				"namespaceSelector": map[string]any{},
				"topologyKey":       "kubernetes.io/hostname",
			}),
		},
		"tolerations": []any{map[string]any{"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"}},
		"volumes": []any{
			claim("s3-code"), claim("s3-datasets"), claim("s3-checkpoints"),
			emptyDir("ckpt-local", map[string]any{"sizeLimit": pod.CkptLocalSize}),
			emptyDir(progressVolume, map[string]any{"sizeLimit": "10Mi"}),
			emptyDir("dshm", map[string]any{"medium": "Memory", "sizeLimit": pod.ShmSize}),
		},
		"initContainers": []any{progressSidecar(in, generation)},
		"containers":     []any{trainer(in, name, generation)},
	}
	if sa := in.Settings.ServiceAccount; sa != "" {
		spec["serviceAccountName"] = sa
	}
	return spec
}

func trainer(in Input, name, generation string) map[string]any {
	pod := in.Settings.Pod
	master := fmt.Sprintf("%s-%s-0-0.%s", name, replicatedJobName, name)
	script := fmt.Sprintf(`set -e
pip install --no-cache-dir --quiet %s
until getent hosts %s >/dev/null; do echo "waiting for %s"; sleep 2; done
exec torchrun --nnodes=%d --nproc-per-node=1 --node-rank="$JOB_COMPLETION_INDEX" --master-addr=%s --master-port=%d --max-restarts=0 %s/train.py`,
		trainerPackages, master, master, in.GPUs, master, pod.MasterPort, codeDir)

	return map[string]any{
		"name":    "trainer",
		"image":   in.Settings.Images.Trainer,
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
			env("MODEL_DIR", modelDir),
			env("CKPT_DIR", ckptDir),
			env("CKPT_LOCAL_DIR", ckptLocalDir),
			env("PROGRESS_FILE", progressFile),
			// g4dn.xlarge는 EFA가 없다. 노드 간 NCCL은 Pod 네트워크(eth0) TCP로 한다.
			env("NCCL_SOCKET_IFNAME", "eth0"),
			env("NCCL_IB_DISABLE", "1"),
			env("TOKENIZERS_PARALLELISM", "false"),
			env("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True"),
		},
		"resources": map[string]any{
			"requests": map[string]any{"cpu": pod.CPURequest, "memory": pod.MemoryRequest, "nvidia.com/gpu": "1"},
			"limits":   map[string]any{"memory": pod.MemoryLimit, "nvidia.com/gpu": "1"},
		},
		"volumeMounts": []any{
			s3Mount("s3-code", codeDir, in.JobID, true),
			s3Mount("s3-datasets", dataDir, fmt.Sprintf("%s/v%d", in.DatasetID, in.DatasetVersion), true),
			s3Mount("s3-datasets", modelDir, in.ModelSubPath, true),
			s3Mount("s3-checkpoints", ckptDir, in.JobID, false),
			map[string]any{"name": "ckpt-local", "mountPath": ckptLocalDir},
			map[string]any{"name": progressVolume, "mountPath": progressDir},
			map[string]any{"name": "dshm", "mountPath": "/dev/shm"},
		},
	}
}

func required(term map[string]any) map[string]any {
	return map[string]any{"requiredDuringSchedulingIgnoredDuringExecution": []any{term}}
}

func claim(name string) map[string]any {
	return map[string]any{"name": name, "persistentVolumeClaim": map[string]any{"claimName": name}}
}

func emptyDir(name string, opts map[string]any) map[string]any {
	return map[string]any{"name": name, "emptyDir": opts}
}

func s3Mount(volume, path, subPath string, readOnly bool) map[string]any {
	m := map[string]any{"name": volume, "mountPath": path, "subPath": subPath}
	if readOnly {
		m["readOnly"] = true
	}
	return m
}

func env(name, value string) map[string]any {
	return map[string]any{"name": name, "value": value}
}
