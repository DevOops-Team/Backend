package jobspec

import _ "embed"

// ProgressSidecarName is the native sidecar that copies the trainer's progress file to DynamoDB.
const ProgressSidecarName = "progress-reporter"

const (
	progressVolume = "progress"
	progressDir    = "/progress"
	progressFile   = progressDir + "/progress.json"

	// Small Python image from ECR Public (no Docker Hub rate limit). boto3 is installed at first use.
	progressImage = "public.ecr.aws/docker/library/python:3.12-slim"
)

//go:embed progress_reporter.py
var progressReporter string

// progressSidecar runs as an init container with restartPolicy Always (Kubernetes native sidecar):
// it starts before the trainer, keeps running beside it, and is stopped by the kubelet once the
// trainer exits, so the Job still completes.
func progressSidecar(o Options) map[string]any {
	return map[string]any{
		"name":          ProgressSidecarName,
		"image":         progressImage,
		"restartPolicy": "Always",
		"command":       []any{"python", "-u", "-c"},
		"args":          []any{progressReporter},
		"env": []any{
			env("JOB_ID", o.JobID),
			env("PROGRESS_TABLE", o.ProgressTable),
			env("PROGRESS_FILE", progressFile),
			env("AWS_REGION", o.Region),
			env("AWS_DEFAULT_REGION", o.Region),
			map[string]any{
				"name":      "NODE_NAME",
				"valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "spec.nodeName"}},
			},
		},
		"volumeMounts": []any{map[string]any{"name": progressVolume, "mountPath": progressDir}},
		// Kueue counts these in the quota (cpu 7 for two pods of 3 cpu) and they must fit next to the trainer.
		"resources": map[string]any{
			"requests": map[string]any{"cpu": "50m", "memory": "128Mi"},
			"limits":   map[string]any{"memory": "128Mi"},
		},
	}
}
