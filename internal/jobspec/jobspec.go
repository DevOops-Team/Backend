// Package jobspec turns an uploaded JobSet manifest into one ready for MultiKueue.
//
// The infra expects every training pod to:
//   - go through the queue-auto LocalQueue in the training namespace,
//   - carry the devoops/gpu-group=train label with zone affinity and hostname
//     anti-affinity, so training pods and standby placeholders share one AZ
//     and one pod lands per GPU node,
//   - tolerate the nvidia.com/gpu taint on Karpenter GPU nodes,
//   - read code/data and write checkpoints through Mountpoint S3 volumes
//     mounted at /code, /data and /ckpt (each scoped to the job ID).
package jobspec

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/yaml"
)

const (
	JobSetAPIVersion = "jobset.x-k8s.io/v1alpha2"
	JobSetKind       = "JobSet"

	QueueLabel    = "kueue.x-k8s.io/queue-name"
	JobIDLabel    = "devoops/job-id"
	GPUGroupLabel = "devoops/gpu-group"
	GPUGroupValue = "train"

	defaultMaxRestarts = 3
)

// Options are the values the backend fills in for every submitted job.
type Options struct {
	JobID         string
	Namespace     string
	QueueName     string
	ProgressTable string
}

// s3Volumes maps the PVC prepared by infra (40-addons) to its mount path.
var s3Volumes = []struct{ claim, mountPath string }{
	{"s3-code", "/code"},
	{"s3-datasets", "/data"},
	{"s3-checkpoints", "/ckpt"},
}

// Name returns the JobSet name for a job ID.
func Name(jobID string) string { return "job-" + jobID }

// Prepare parses manifest and injects what the infra requires.
func Prepare(manifest []byte, o Options) (*unstructured.Unstructured, error) {
	raw, err := yaml.YAMLToJSON(manifest)
	if err != nil {
		return nil, fmt.Errorf("job.yaml is not valid YAML: %w", err)
	}
	// Kubernetes JSON decoding keeps integers as int64 (encoding/json would give float64).
	obj := map[string]any{}
	if err := utiljson.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("job.yaml is not a YAML object: %w", err)
	}
	js := &unstructured.Unstructured{Object: obj}
	if js.GetAPIVersion() != JobSetAPIVersion || js.GetKind() != JobSetKind {
		return nil, fmt.Errorf("job.yaml must be %s %s, got %s %s", JobSetAPIVersion, JobSetKind, js.GetAPIVersion(), js.GetKind())
	}

	jobs, found, err := unstructured.NestedSlice(js.Object, "spec", "replicatedJobs")
	if err != nil || !found || len(jobs) == 0 {
		return nil, errors.New("job.yaml must define spec.replicatedJobs")
	}

	js.SetName(Name(o.JobID))
	js.SetNamespace(o.Namespace)
	js.SetLabels(merge(js.GetLabels(), map[string]string{
		QueueLabel: o.QueueName,
		JobIDLabel: o.JobID,
	}))

	if _, found, _ := unstructured.NestedFieldNoCopy(js.Object, "spec", "failurePolicy", "maxRestarts"); !found {
		if err := unstructured.SetNestedField(js.Object, int64(defaultMaxRestarts), "spec", "failurePolicy", "maxRestarts"); err != nil {
			return nil, err
		}
	}

	for i, j := range jobs {
		job, ok := j.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("spec.replicatedJobs[%d] is not an object", i)
		}
		if err := preparePodTemplate(job, o); err != nil {
			return nil, fmt.Errorf("spec.replicatedJobs[%d]: %w", i, err)
		}
	}
	if err := unstructured.SetNestedSlice(js.Object, jobs, "spec", "replicatedJobs"); err != nil {
		return nil, err
	}
	return js, nil
}

func preparePodTemplate(job map[string]any, o Options) error {
	tmpl, found, err := unstructured.NestedMap(job, "template", "spec", "template")
	if err != nil || !found {
		return errors.New("missing template.spec.template (pod template)")
	}

	labels, _, _ := unstructured.NestedStringMap(tmpl, "metadata", "labels")
	labels = merge(labels, map[string]string{GPUGroupLabel: GPUGroupValue, JobIDLabel: o.JobID})
	if err := unstructured.SetNestedStringMap(tmpl, labels, "metadata", "labels"); err != nil {
		return err
	}

	spec, _, _ := unstructured.NestedMap(tmpl, "spec")
	if spec == nil {
		return errors.New("missing pod spec")
	}

	spec["tolerations"] = append(slice(spec["tolerations"]), map[string]any{
		"key": "nvidia.com/gpu", "operator": "Equal", "value": "true", "effect": "NoSchedule",
	})

	groupSelector := map[string]any{"matchLabels": map[string]any{GPUGroupLabel: GPUGroupValue}}
	affinity, _ := spec["affinity"].(map[string]any)
	if affinity == nil {
		affinity = map[string]any{}
	}
	addRequiredTerm(affinity, "podAffinity", map[string]any{
		"labelSelector":     groupSelector,
		"namespaceSelector": map[string]any{}, // placeholders live in gpu-standby
		"topologyKey":       "topology.kubernetes.io/zone",
	})
	addRequiredTerm(affinity, "podAntiAffinity", map[string]any{
		"labelSelector":     groupSelector,
		"namespaceSelector": map[string]any{},
		"topologyKey":       "kubernetes.io/hostname",
	})
	spec["affinity"] = affinity

	volumes := slice(spec["volumes"])
	for _, v := range s3Volumes {
		volumes = append(volumes, map[string]any{
			"name":                  v.claim,
			"persistentVolumeClaim": map[string]any{"claimName": v.claim},
		})
	}
	spec["volumes"] = volumes

	containers := slice(spec["containers"])
	if len(containers) == 0 {
		return errors.New("pod spec has no containers")
	}
	for i, c := range containers {
		cm, ok := c.(map[string]any)
		if !ok {
			return fmt.Errorf("containers[%d] is not an object", i)
		}
		mounts := slice(cm["volumeMounts"])
		for _, v := range s3Volumes {
			mounts = append(mounts, map[string]any{"name": v.claim, "mountPath": v.mountPath, "subPath": o.JobID})
		}
		cm["volumeMounts"] = mounts
		cm["env"] = append(slice(cm["env"]),
			env("JOB_ID", o.JobID),
			env("PROGRESS_TABLE", o.ProgressTable),
			env("CODE_DIR", "/code"),
			env("DATA_DIR", "/data"),
			env("CKPT_DIR", "/ckpt"),
		)
		containers[i] = cm
	}
	spec["containers"] = containers

	tmpl["spec"] = spec
	return unstructured.SetNestedMap(job, tmpl, "template", "spec", "template")
}

func addRequiredTerm(affinity map[string]any, kind string, term map[string]any) {
	a, _ := affinity[kind].(map[string]any)
	if a == nil {
		a = map[string]any{}
	}
	const key = "requiredDuringSchedulingIgnoredDuringExecution"
	a[key] = append(slice(a[key]), term)
	affinity[kind] = a
}

func env(name, value string) map[string]any {
	return map[string]any{"name": name, "value": value}
}

func slice(v any) []any {
	s, _ := v.([]any)
	return s
}

func merge(base, extra map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}
