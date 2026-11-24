package repository

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/DevOops-Team/Backend/internal/model"
)

type fencingDB interface {
	GetItem(ctx context.Context, in *dynamodb.GetItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

// Fencing writes the backend's part of the fencing table (ADR-6 · ADR-10): a job's first item, and the
// generation step of a user restart (GPU-240). ckpt-init refuses to start a pod whose job has no item
// (exit 6) or another generation (exit 5), and the trainer Pod role cannot write either, so the backend does.
type Fencing struct {
	db    fencingDB
	table string
}

func NewFencing(db *dynamodb.Client, table string) *Fencing {
	return &Fencing{db: db, table: table}
}

// JobsetSHA256Attr is the fencing item attribute holding the SHA-256 (hex) of the job's code/<id>/jobset.json
// as submitted. The backend writes it once with the item; nothing updates it, so the recovery controller can
// check the stored original against it (GPU-119).
const JobsetSHA256Attr = "jobset_sha256"

// Create writes generation 1 only if the job has no item yet (체크포인트 규약 13절), with the hash of the
// submitted jobset.json.
func (f *Fencing) Create(ctx context.Context, jobID, jobsetSHA256 string) error {
	_, err := f.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(f.table),
		Item: map[string]types.AttributeValue{
			"job_id":         &types.AttributeValueMemberS{Value: jobID},
			"generation":     &types.AttributeValueMemberN{Value: "1"},
			"retry_count":    &types.AttributeValueMemberN{Value: "0"},
			JobsetSHA256Attr: &types.AttributeValueMemberS{Value: jobsetSHA256},
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

// JobsetSHA256 returns the jobset.json hash recorded when the job was submitted, read with strong
// consistency. model.ErrNotFound when the job has no fencing item; "" when the item has no hash (jobs
// submitted before GPU-119).
func (f *Fencing) JobsetSHA256(ctx context.Context, jobID string) (string, error) {
	out, err := f.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(f.table),
		Key:                  map[string]types.AttributeValue{"job_id": &types.AttributeValueMemberS{Value: jobID}},
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("job_id, " + JobsetSHA256Attr), // job_id: an item without the hash is not empty
	})
	if err != nil {
		return "", fmt.Errorf("read fencing item: %w", err)
	}
	if len(out.Item) == 0 {
		return "", fmt.Errorf("fencing item %s: %w", jobID, model.ErrNotFound)
	}
	return stringOf(out.Item[JobsetSHA256Attr]), nil
}

// Restart moves a stopped job from generation from to from+1 for a user restart (GPU-240), the way the
// recovery controller does for a relocation but outside a recovery: only while no recovery is in progress
// (recovery_phase absent or COMPLETED), and it removes the owner so the new generation's ckpt-init can claim
// it. The approved checkpoint stays, so the new generation resumes from it. retry_count (automatic recovery
// attempts) is left alone; restart_count counts user restarts.
//
// A repeated call after a successful step (the item is already at from+1, no recovery) returns nil, so a
// restart whose JobSet submission failed can be retried. Any other mismatch is model.ErrGenerationMoved.
func (f *Fencing) Restart(ctx context.Context, jobID string, from int, at time.Time) error {
	to := from + 1
	_, err := f.db.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(f.table),
		Key:       map[string]types.AttributeValue{"job_id": &types.AttributeValueMemberS{Value: jobID}},
		UpdateExpression: aws.String("SET generation = :to, restart_requested_at = :at, " +
			"restart_count = if_not_exists(restart_count, :zero) + :one REMOVE #owner"),
		ConditionExpression: aws.String("generation = :from AND " +
			"(attribute_not_exists(recovery_phase) OR recovery_phase = :completed)"),
		ExpressionAttributeNames: map[string]string{"#owner": "owner"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":from":      &types.AttributeValueMemberN{Value: strconv.Itoa(from)},
			":to":        &types.AttributeValueMemberN{Value: strconv.Itoa(to)},
			":at":        &types.AttributeValueMemberS{Value: at.UTC().Format(time.RFC3339)},
			":zero":      &types.AttributeValueMemberN{Value: "0"},
			":one":       &types.AttributeValueMemberN{Value: "1"},
			":completed": &types.AttributeValueMemberS{Value: "COMPLETED"},
		},
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})
	var failed *types.ConditionalCheckFailedException
	if !errors.As(err, &failed) {
		if err != nil {
			return fmt.Errorf("advance fencing generation: %w", err)
		}
		return nil
	}
	if failed.Item == nil {
		return fmt.Errorf("fencing item %s: %w", jobID, model.ErrNotFound)
	}
	gen := numberOf(failed.Item["generation"])
	phase := stringOf(failed.Item["recovery_phase"])
	if gen == to && (phase == "" || phase == "COMPLETED") {
		return nil // this restart already advanced it
	}
	return fmt.Errorf("fencing item %s is at generation %d (recovery_phase %q), not %d: %w",
		jobID, gen, phase, from, model.ErrGenerationMoved)
}

func numberOf(v types.AttributeValue) int {
	n, ok := v.(*types.AttributeValueMemberN)
	if !ok {
		return 0
	}
	i, _ := strconv.Atoi(n.Value)
	return i
}

func stringOf(v types.AttributeValue) string {
	s, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		return ""
	}
	return s.Value
}
