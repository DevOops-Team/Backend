package repository

import (
	"context"
	"sort"
	"sync"

	"github.com/DevOops-Team/Backend/internal/model"
)

// Timeline keeps each job's timeline in memory (per backend replica).
//
// The DynamoDB events table it replaces never existed in AWS; the durable record of a job is now the
// Kafka job.lifecycle topic → collector → PostgreSQL (Data repo). Every replica runs its own tracker
// over the same cluster, so replicas converge; what is lost on a restart is only history older
// than the Kubernetes event TTL.
type Timeline struct {
	mu     sync.Mutex
	perJob int
	jobs   map[string]map[string]model.JobEvent
}

func NewTimeline(perJob int) *Timeline {
	return &Timeline{perJob: perJob, jobs: map[string]map[string]model.JobEvent{}}
}

// Put stores e, replacing the entry with the same key. Over perJob entries the oldest keys go.
func (t *Timeline) Put(_ context.Context, e model.JobEvent) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.jobs[e.JobID]
	if m == nil {
		m = map[string]model.JobEvent{}
		t.jobs[e.JobID] = m
	}
	m[e.Key] = e
	if len(m) > t.perJob {
		for _, k := range sortedKeys(m)[:len(m)-t.perJob] {
			delete(m, k)
		}
	}
	return nil
}

// List returns the job's timeline, oldest first (keys start with a fixed-width time).
func (t *Timeline) List(_ context.Context, jobID string) ([]model.JobEvent, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.jobs[jobID]
	out := make([]model.JobEvent, 0, len(m))
	for _, k := range sortedKeys(m) {
		out = append(out, m[k])
	}
	return out, nil
}

func sortedKeys(m map[string]model.JobEvent) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
