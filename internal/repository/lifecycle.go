package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/DevOops-Team/Backend/internal/model"
)

// sendTimeout bounds one publish. The job is already created when the backend reports it, so a slow
// or unreachable Kafka must not hold the request.
const sendTimeout = 10 * time.Second

// Lifecycle publishes the backend's job events to Kafka job.lifecycle (Data repo: collector →
// PostgreSQL data.job_runs). Without brokers it does nothing, for local runs.
type Lifecycle struct {
	client *kgo.Client
	topic  string
}

// NewLifecycle connects lazily: brokers is "host:port,host:port", empty = no Kafka.
func NewLifecycle(brokers, topic string) (*Lifecycle, error) {
	if brokers == "" {
		return &Lifecycle{topic: topic}, nil
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), // acks=all; the client is idempotent by default
		kgo.RecordDeliveryTimeout(sendTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	return &Lifecycle{client: client, topic: topic}, nil
}

// Submitted sends job.submitted and waits for the brokers to acknowledge it.
func (l *Lifecycle) Submitted(ctx context.Context, e model.SubmittedEvent) error {
	if l.client == nil {
		return nil
	}
	key, body, err := submittedRecord(e)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	res := l.client.ProduceSync(ctx, &kgo.Record{Topic: l.topic, Key: []byte(key), Value: body})
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("publish job.submitted: %w", err)
	}
	return nil
}

func (l *Lifecycle) Close() {
	if l.client != nil {
		l.client.Close()
	}
}

// envelope is the shared event shape (데이터 파트 설계 2절, Data collector/envelope.py).
type envelope struct {
	EventID       string         `json:"event_id"`
	SchemaVersion int            `json:"schema_version"`
	Type          string         `json:"type"`
	OccurredAt    string         `json:"occurred_at"`
	Source        string         `json:"source"`
	Cluster       string         `json:"cluster,omitempty"` // the worker that runs it; unknown at submission
	JobID         string         `json:"job_id"`
	Generation    int            `json:"generation"`
	Payload       map[string]any `json:"payload"`
}

// submittedRecord returns the message key (job id: one partition per job keeps its order) and body.
// event_id is fixed per job generation, so a retry is stored once.
func submittedRecord(e model.SubmittedEvent) (string, []byte, error) {
	body, err := json.Marshal(envelope{
		EventID:       fmt.Sprintf("submitted-%s-g%04d", e.JobID, e.Generation),
		SchemaVersion: 1,
		Type:          "job.submitted",
		OccurredAt:    e.OccurredAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		Source:        "backend",
		JobID:         e.JobID,
		Generation:    e.Generation,
		Payload: map[string]any{
			"jobset_name":   e.JobsetName,
			"gpus":          e.GPUs,
			"strategy":      e.Strategy,
			"queue":         e.Queue,
			"config_sha256": e.ConfigSHA256,
			"image_digest":  e.ImageDigest,
		},
	})
	if err != nil {
		return "", nil, fmt.Errorf("encode job.submitted: %w", err)
	}
	return e.JobID, body, nil
}
