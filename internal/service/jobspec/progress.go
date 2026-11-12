package jobspec

import _ "embed"

// ProgressSidecarName is the native sidecar that copies the trainer's progress file to DynamoDB.
const ProgressSidecarName = "progress-reporter"

const (
	progressVolume = "progress"
	progressDir    = "/progress"
	progressFile   = progressDir + "/progress.json"
)

//go:embed progress_reporter.py
var progressReporter string

// progressSidecar runs as an init container with restartPolicy Always (Kubernetes native sidecar):
// it starts before the trainer, keeps running beside it, and is stopped by the kubelet once the
// trainer exits, so the Job still completes.
func progressSidecar(in Input, generation string) map[string]any {
	return map[string]any{
		"name":          ProgressSidecarName,
		"image":         in.Settings.Images.ProgressReporter,
		"restartPolicy": "Always",
		"command":       []any{"python", "-u", "-c"},
		"args":          []any{progressReporter},
		"env": []any{
			env("JOB_ID", in.JobID),
			env("GENERATION", generation),
			env("PROGRESS_TABLE", in.ProgressTable),
			env("PROGRESS_FILE", progressFile),
			env("AWS_REGION", in.Region),
			env("AWS_DEFAULT_REGION", in.Region),
			// The pod runs as 1000 and the public image has no boto3: pip falls back to a user install
			// under $HOME, which must be writable.
			env("HOME", "/tmp"),
			map[string]any{
				"name":      "NODE_NAME",
				"valueFrom": map[string]any{"fieldRef": map[string]any{"fieldPath": "spec.nodeName"}},
			},
		},
		"volumeMounts": []any{mount(progressVolume, progressDir, false)},
		// Kueue counts native sidecars in the quota, and they must fit next to the trainer on g4dn.xlarge.
		"resources": map[string]any{
			"requests": map[string]any{"cpu": "50m", "memory": "128Mi"},
			"limits":   map[string]any{"memory": "128Mi"},
		},
	}
}
