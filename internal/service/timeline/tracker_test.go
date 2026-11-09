package timeline

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/DevOops-Team/Backend/internal/model"
)

type fakeSource struct {
	events, workloads []unstructured.Unstructured
}

func (f *fakeSource) Events(context.Context) ([]unstructured.Unstructured, error) {
	return f.events, nil
}
func (f *fakeSource) Workloads(context.Context) ([]unstructured.Unstructured, error) {
	return f.workloads, nil
}

type fakeStore struct {
	puts []model.JobEvent
	err  error
}

func (f *fakeStore) Put(_ context.Context, e model.JobEvent) error {
	if f.err != nil {
		return f.err
	}
	f.puts = append(f.puts, e)
	return nil
}

func (f *fakeStore) List(context.Context, string) ([]model.JobEvent, error) { return nil, nil }

func TestSyncWritesEventsAndWorkloadSteps(t *testing.T) {
	src := &fakeSource{
		events:    []unstructured.Unstructured{*obj(t, quotaReserved)},
		workloads: []unstructured.Unstructured{*obj(t, multikueueWorkload)},
	}
	store := &fakeStore{}

	if err := NewTracker(src, store, "k3s").Sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.puts) != 3 {
		t.Fatalf("want QuotaReserved + MultiKueue Ready + Finished, got %d: %+v", len(store.puts), store.puts)
	}
}

// Polling every 2s must not rewrite unchanged items.
func TestSyncSkipsUnchangedAndRewritesRepeatedEvents(t *testing.T) {
	ev := obj(t, quotaReserved)
	src := &fakeSource{events: []unstructured.Unstructured{*ev}}
	store := &fakeStore{}
	tr := NewTracker(src, store, "k3s")

	sync(t, tr)
	sync(t, tr)
	if len(store.puts) != 1 {
		t.Fatalf("unchanged event written %d times", len(store.puts))
	}

	if err := unstructured.SetNestedField(ev.Object, int64(3), "count"); err != nil {
		t.Fatal(err)
	}
	src.events = []unstructured.Unstructured{*ev}
	sync(t, tr)
	if len(store.puts) != 2 || store.puts[1].Count != 3 || store.puts[1].Key != store.puts[0].Key {
		t.Errorf("repeated event should overwrite the same key with the new count: %+v", store.puts)
	}
}

func TestSyncRetriesFailedWrites(t *testing.T) {
	src := &fakeSource{events: []unstructured.Unstructured{*obj(t, quotaReserved)}}
	store := &fakeStore{err: errors.New("throttled")}
	tr := NewTracker(src, store, "k3s")

	if err := tr.Sync(context.Background()); err == nil {
		t.Error("expected the write error")
	}
	store.err = nil
	sync(t, tr)
	if len(store.puts) != 1 {
		t.Error("a failed write must be retried on the next sync")
	}
}

func sync(t *testing.T, tr *Tracker) {
	t.Helper()
	if err := tr.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}
}
