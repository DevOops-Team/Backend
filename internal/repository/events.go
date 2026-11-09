package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/DevOops-Team/Backend/internal/model"
)

// eventTTL matches the 7-day TTL the infra (20-data) sets on the events table.
const eventTTL = 7 * 24 * time.Hour

type eventDB interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
}

// Events is the per-job timeline table (PK job_id, SK sk). The job-watcher on each EKS worker
// writes the same attributes (infra cluster-addons/job_watcher.py).
type Events struct {
	db    eventDB
	table string
	now   func() time.Time
}

func NewEvents(db *dynamodb.Client, table string) *Events {
	return &Events{db: db, table: table, now: time.Now}
}

type eventItem struct {
	JobID     string `dynamodbav:"job_id"`
	SK        string `dynamodbav:"sk"`
	Time      string `dynamodbav:"time"`
	LastTime  string `dynamodbav:"last_time"`
	Cluster   string `dynamodbav:"cluster"`
	Source    string `dynamodbav:"source"`
	Reason    string `dynamodbav:"reason"`
	Message   string `dynamodbav:"message"`
	Type      string `dynamodbav:"type"`
	Object    string `dynamodbav:"object"`
	Count     int64  `dynamodbav:"count"`
	ExpiresAt int64  `dynamodbav:"expires_at,omitempty"`
}

func (e *Events) Put(ctx context.Context, ev model.JobEvent) error {
	item, err := attributevalue.MarshalMap(eventItem{
		JobID: ev.JobID, SK: ev.Key,
		Time: ev.Time.UTC().Format(model.EventKeyTimeFormat), LastTime: ev.LastTime.UTC().Format(model.EventKeyTimeFormat),
		Cluster: ev.Cluster, Source: ev.Source, Reason: ev.Reason, Message: ev.Message, Type: ev.Type,
		Object: ev.Object, Count: ev.Count, ExpiresAt: e.now().Add(eventTTL).Unix(),
	})
	if err != nil {
		return err
	}
	_, err = e.db.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(e.table), Item: item})
	if err != nil {
		return fmt.Errorf("put event: %w", err)
	}
	return nil
}

// List returns the job's timeline, oldest first.
func (e *Events) List(ctx context.Context, jobID string) ([]model.JobEvent, error) {
	out := []model.JobEvent{}
	var start map[string]types.AttributeValue
	for {
		res, err := e.db.Query(ctx, &dynamodb.QueryInput{
			TableName:                 aws.String(e.table),
			KeyConditionExpression:    aws.String("job_id = :id"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":id": &types.AttributeValueMemberS{Value: jobID}},
			ScanIndexForward:          aws.Bool(true),
			ExclusiveStartKey:         start,
		})
		if err != nil {
			return nil, fmt.Errorf("query events: %w", err)
		}
		for _, raw := range res.Items {
			var it eventItem
			if err := attributevalue.UnmarshalMap(raw, &it); err != nil {
				return nil, fmt.Errorf("decode event: %w", err)
			}
			out = append(out, model.JobEvent{
				JobID: it.JobID, Key: it.SK, Time: parseKeyTime(it.Time), LastTime: parseKeyTime(it.LastTime),
				Cluster: it.Cluster, Source: it.Source, Reason: it.Reason, Message: it.Message, Type: it.Type,
				Object: it.Object, Count: it.Count,
			})
		}
		if len(res.LastEvaluatedKey) == 0 {
			return out, nil
		}
		start = res.LastEvaluatedKey
	}
}

func parseKeyTime(s string) time.Time {
	t, _ := time.Parse(model.EventKeyTimeFormat, s)
	return t
}
