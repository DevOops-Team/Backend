package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DevOops-Team/Backend/internal/model"
)

// Feed reads Kafka job.lifecycle for the job screens (GPU-146).
//
// No consumer group: every backend replica reads every partition from the oldest record, so each
// replica rebuilds the same views on start and no offsets are committed. Without brokers it does nothing.
type Feed struct {
	client *kgo.Client
}

func NewFeed(brokers, topic string) (*Feed, error) {
	if brokers == "" {
		return &Feed{}, nil
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Feed{client: client}, nil
}

// Run passes every event to apply until ctx ends. Records that are not a valid envelope are skipped.
// The client reconnects by itself, so broker outages only delay the views.
func (f *Feed) Run(ctx context.Context, apply func(context.Context, model.LifecycleEvent) bool) {
	if f.client == nil {
		return
	}
	defer f.client.Close()
	for {
		fetches := f.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if !errors.Is(err, context.Canceled) {
				slog.Warn("kafka fetch", "topic", topic, "partition", partition, "err", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			e, err := DecodeLifecycle(r.Value)
			if err != nil {
				slog.Warn("skip job.lifecycle record", "partition", r.Partition, "offset", r.Offset, "err", err)
				return
			}
			apply(ctx, e)
		})
	}
}

// wireEvent is the envelope as other producers write it; generation and cluster may be missing.
type wireEvent struct {
	EventID    string         `json:"event_id"`
	Type       string         `json:"type"`
	OccurredAt string         `json:"occurred_at"`
	Source     string         `json:"source"`
	Cluster    *string        `json:"cluster"`
	JobID      *string        `json:"job_id"`
	Generation *int           `json:"generation"`
	Payload    map[string]any `json:"payload"`
}

// DecodeLifecycle parses one record with the collector's rules: event_id, type and an occurred_at with a
// time zone are required.
func DecodeLifecycle(raw []byte) (model.LifecycleEvent, error) {
	var w wireEvent
	if err := json.Unmarshal(raw, &w); err != nil {
		return model.LifecycleEvent{}, fmt.Errorf("invalid json: %w", err)
	}
	if w.EventID == "" || w.Type == "" || w.OccurredAt == "" {
		return model.LifecycleEvent{}, errors.New("missing event_id, type or occurred_at")
	}
	at, err := time.Parse(time.RFC3339Nano, w.OccurredAt)
	if err != nil {
		return model.LifecycleEvent{}, fmt.Errorf("occurred_at: %w", err)
	}
	e := model.LifecycleEvent{
		EventID: w.EventID, Type: w.Type, OccurredAt: at.UTC(), Source: w.Source, Payload: w.Payload,
	}
	if w.Cluster != nil {
		e.Cluster = *w.Cluster
	}
	if w.JobID != nil {
		e.JobID = *w.JobID
	}
	if w.Generation != nil {
		e.Generation = *w.Generation
	}
	return e, nil
}
