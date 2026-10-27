// Package store holds the AWS-backed implementations: S3 for job inputs, DynamoDB for progress.
package store

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/DevOops-Team/Backend/internal/api"
)

// batchGetLimit is DynamoDB's maximum keys per BatchGetItem.
const batchGetLimit = 100

type batchGetter interface {
	BatchGetItem(ctx context.Context, in *dynamodb.BatchGetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
}

// Progress reads the item train.py (rank 0) overwrites every step.
type Progress struct {
	db    batchGetter
	table string
}

func NewProgress(db *dynamodb.Client, table string) *Progress {
	return &Progress{db: db, table: table}
}

// item mirrors the attribute names train.py writes.
type item struct {
	JobID              string  `dynamodbav:"job_id"`
	Status             string  `dynamodbav:"status"`
	Step               int64   `dynamodbav:"step"`
	TotalSteps         int64   `dynamodbav:"total_steps"`
	Loss               float64 `dynamodbav:"loss"`
	LastCheckpointStep int64   `dynamodbav:"last_checkpoint_step"`
	NodeName           string  `dynamodbav:"node_name"`
	UpdatedAt          int64   `dynamodbav:"updated_at"`
}

func (p *Progress) Get(ctx context.Context, jobIDs []string) (map[string]api.Progress, error) {
	out := map[string]api.Progress{}
	for start := 0; start < len(jobIDs); start += batchGetLimit {
		end := min(start+batchGetLimit, len(jobIDs))
		keys := make([]map[string]types.AttributeValue, 0, end-start)
		for _, id := range jobIDs[start:end] {
			keys = append(keys, map[string]types.AttributeValue{"job_id": &types.AttributeValueMemberS{Value: id}})
		}

		// Unprocessed keys are dropped: progress is polled again in two seconds.
		res, err := p.db.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
			RequestItems: map[string]types.KeysAndAttributes{p.table: {Keys: keys}},
		})
		if err != nil {
			return nil, fmt.Errorf("batch get progress: %w", err)
		}
		for _, raw := range res.Responses[p.table] {
			var it item
			if err := attributevalue.UnmarshalMap(raw, &it); err != nil {
				return nil, fmt.Errorf("decode progress: %w", err)
			}
			out[it.JobID] = api.Progress{
				Status:             it.Status,
				Step:               it.Step,
				TotalSteps:         it.TotalSteps,
				Loss:               it.Loss,
				LastCheckpointStep: it.LastCheckpointStep,
				NodeName:           it.NodeName,
				UpdatedAt:          it.UpdatedAt,
			}
		}
	}
	return out, nil
}
