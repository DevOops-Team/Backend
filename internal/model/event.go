package model

import "time"

// EventKeyTimeFormat is the time prefix of JobEvent.Key; fixed width so keys sort in time order.
const EventKeyTimeFormat = "2006-01-02T15:04:05.000Z"

// ManagerCluster names the on-prem management cluster in timeline entries.
const ManagerCluster = "onprem"

// JobEvent is one step a job went through on its way to a GPU:
// backend → onprem Kueue → MultiKueue → worker Kueue → JobSet → kube-scheduler → Karpenter → kubelet.
// The backend writes the management-cluster steps, the job-watcher on every EKS worker writes the worker steps.
type JobEvent struct {
	JobID    string    `json:"jobId"`
	Key      string    `json:"key"`      // sort key: "<time>#<cluster>#<id>"; rewriting the same event overwrites it
	Time     time.Time `json:"time"`     // first seen
	LastTime time.Time `json:"lastTime"` // last seen (repeated events such as FailedScheduling)
	Cluster  string    `json:"cluster"`  // "onprem" or the EKS worker name
	Source   string    `json:"source"`   // backend, kueue-admission, multikueue, jobset, default-scheduler, karpenter, kubelet, ...
	Reason   string    `json:"reason"`
	Message  string    `json:"message"`
	Type     string    `json:"type"`   // Normal | Warning
	Object   string    `json:"object"` // Kind/name the event is about
	Count    int64     `json:"count"`
}

// SubmittedEvent is what the backend reports to Kafka job.lifecycle when it creates a job (type
// job.submitted, Data repo collector: data.job_runs).
type SubmittedEvent struct {
	JobID        string
	Generation   int
	JobsetName   string
	GPUs         int
	Strategy     string
	Queue        string
	ConfigSHA256 string
	ImageDigest  string // trainer image digest, "" when the image is not pinned
	ExpID        string // experiment the job belongs to (잡 투입기, GPU-172); "" outside an experiment
	// The submitted original (GPU-271): SHA-256 (lowercase hex) of the code/<id>/jobset.json bytes put in S3,
	// the same value as the fencing item's jobset_sha256, and that object's s3:// URI. The recovery controller
	// reads them from job.lifecycle instead of asking the backend at fault time.
	JobsetSHA256 string
	JobsetURI    string
	OccurredAt   time.Time
}
