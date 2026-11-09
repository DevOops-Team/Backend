package timeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
)

// Source lists what the tracker turns into timeline entries (the management cluster's training namespace).
type Source interface {
	Events(ctx context.Context) ([]unstructured.Unstructured, error)
	Workloads(ctx context.Context) ([]unstructured.Unstructured, error)
}

// EventWriter stores timeline entries (DynamoDB devoops-job-events).
type EventWriter interface {
	Put(ctx context.Context, e model.JobEvent) error
}

// Tracker polls one cluster and writes new or changed entries. Kubernetes keeps events for about
// an hour, so the timeline in DynamoDB is what outlives them.
type Tracker struct {
	src     Source
	store   EventWriter
	cluster string
	seen    map[string]string // key → fingerprint of what was last written
}

func NewTracker(src Source, store EventWriter, cluster string) *Tracker {
	return &Tracker{src: src, store: store, cluster: cluster, seen: map[string]string{}}
}

// Sync does one pass. Entries that fail to write are retried on the next pass.
func (t *Tracker) Sync(ctx context.Context) error {
	events, err := t.src.Events(ctx)
	if err != nil {
		return fmt.Errorf("list events: %w", err)
	}
	workloads, err := t.src.Workloads(ctx)
	if err != nil {
		return fmt.Errorf("list workloads: %w", err)
	}

	var entries []model.JobEvent
	for i := range events {
		if e, ok := FromEvent(&events[i], t.cluster); ok {
			entries = append(entries, e)
		}
	}
	for i := range workloads {
		entries = append(entries, FromWorkload(&workloads[i], t.cluster)...)
	}

	// Only keys still listed are kept, so the map does not grow as Kubernetes expires events.
	var errs []error
	seen := make(map[string]string, len(entries))
	for _, e := range entries {
		fp := fmt.Sprintf("%d|%s|%s|%s", e.Count, e.LastTime.Format(time.RFC3339Nano), e.Reason, e.Message)
		if t.seen[e.Key] == fp {
			seen[e.Key] = fp
			continue
		}
		if err := t.store.Put(ctx, e); err != nil {
			errs = append(errs, err)
			continue
		}
		seen[e.Key] = fp
	}
	t.seen = seen
	return errors.Join(errs...)
}

// Run syncs every interval until ctx is done.
func (t *Tracker) Run(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if err := t.Sync(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("timeline sync failed", "cluster", t.cluster, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
