package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/DevOops-Team/Backend/internal/model"
)

type itemPutter interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

// Fencing creates a job's first item in the fencing table (ADR-6 · ADR-10). ckpt-init refuses to start
// a pod whose job has no item (exit 6), and the trainer Pod role has no PutItem, so the backend writes it.
type Fencing struct {
	db    itemPutter
	table string
}

func NewFencing(db *dynamodb.Client, table string) *Fencing {
	return &Fencing{db: db, table: table}
}

// Create writes generation 1 only if the job has no item yet (체크포인트 규약 13절).
func (f *Fencing) Create(ctx context.Context, jobID string) error {
	_, err := f.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(f.table),
		Item: map[string]types.AttributeValue{
			"job_id":      &types.AttributeValueMemberS{Value: jobID},
			"generation":  &types.AttributeValueMemberN{Value: "1"},
			"retry_count": &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("attribute_not_exists(job_id)"),
	})
	var exists *types.ConditionalCheckFailedException
	if errors.As(err, &exists) {
		return fmt.Errorf("fencing item %s: %w", jobID, model.ErrAlreadyExists)
	}
	if err != nil {
		return fmt.Errorf("create fencing item: %w", err)
	}
	return nil
}
