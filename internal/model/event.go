package model

import "time"

// EventKeyTimeFormat is the time prefix of JobEvent.Key; fixed width so keys sort in time order.
const EventKeyTimeFormat = "2006-01-02T15:04:05.000Z"

// ManagerCluster names the management cluster (k3s) in timeline entries.
const ManagerCluster = "k3s"

// JobEvent is one step a job went through on its way to a GPU:
// backend → k3s Kueue → MultiKueue → worker Kueue → JobSet → kube-scheduler → Karpenter → kubelet.
// The backend writes the management-cluster steps, the job-watcher on every EKS worker writes the worker steps.
type JobEvent struct {
	JobID    string    `json:"jobId"`
	Key      string    `json:"key"`      // sort key: "<time>#<cluster>#<id>"; rewriting the same event overwrites it
	Time     time.Time `json:"time"`     // first seen
	LastTime time.Time `json:"lastTime"` // last seen (repeated events such as FailedScheduling)
	Cluster  string    `json:"cluster"`  // "k3s" or the EKS worker name
	Source   string    `json:"source"`   // backend, kueue-admission, multikueue, jobset, default-scheduler, karpenter, kubelet, ...
	Reason   string    `json:"reason"`
	Message  string    `json:"message"`
	Type     string    `json:"type"`   // Normal | Warning
	Object   string    `json:"object"` // Kind/name the event is about
	Count    int64     `json:"count"`
}
