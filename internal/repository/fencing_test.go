package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/DevOops-Team/Backend/internal/model"
)

type fakeFencingDB struct {
	got  *dynamodb.UpdateItemInput
	old  map[string]types.AttributeValue // set: the condition fails with this item
	fail error
}

func (f *fakeFencingDB) PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeFencingDB) UpdateItem(_ context.Context, in *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	f.got = in
	if f.fail != nil {
		return nil, f.fail
	}
	if f.old != nil {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("failed"), Item: f.old}
	}
	return &dynamodb.UpdateItemOutput{}, nil
}

func item(gen, phase string) map[string]types.AttributeValue {
	m := map[string]types.AttributeValue{"job_id": &types.AttributeValueMemberS{Value: "aa11"},
		"generation": &types.AttributeValueMemberN{Value: gen}}
	if phase != "" {
		m["recovery_phase"] = &types.AttributeValueMemberS{Value: phase}
	}
	return m
}

var restartAt = time.Date(2026, 11, 20, 4, 0, 0, 0, time.UTC)

func TestRestartAdvancesTheGenerationOutsideARecovery(t *testing.T) {
	db := &fakeFencingDB{}
	f := &Fencing{db: db, table: "fencing"}
	if err := f.Restart(t.Context(), "aa11", 2, restartAt); err != nil {
		t.Fatal(err)
	}
	in := db.got
	if *in.TableName != "fencing" || in.Key["job_id"].(*types.AttributeValueMemberS).Value != "aa11" {
		t.Errorf("table %s key %v", *in.TableName, in.Key)
	}
	for _, want := range []string{"SET generation = :to", "REMOVE #owner", "restart_count"} {
		if !strings.Contains(*in.UpdateExpression, want) {
			t.Errorf("update %q lacks %q", *in.UpdateExpression, want)
		}
	}
	if strings.Contains(*in.UpdateExpression, "retry_count") {
		t.Error("a user restart must not use up automatic recovery attempts")
	}
	if *in.ConditionExpression != "generation = :from AND (attribute_not_exists(recovery_phase) OR recovery_phase = :completed)" {
		t.Errorf("condition = %s", *in.ConditionExpression)
	}
	v := in.ExpressionAttributeValues
	if v[":from"].(*types.AttributeValueMemberN).Value != "2" || v[":to"].(*types.AttributeValueMemberN).Value != "3" ||
		v[":at"].(*types.AttributeValueMemberS).Value != "2026-11-20T04:00:00Z" {
		t.Errorf("values = %v", v)
	}
}

func TestRestartIsDoneWhenTheItemIsAlreadyAtTheNextGeneration(t *testing.T) {
	for _, phase := range []string{"", "COMPLETED"} {
		f := &Fencing{db: &fakeFencingDB{old: item("3", phase)}, table: "fencing"}
		if err := f.Restart(t.Context(), "aa11", 2, restartAt); err != nil {
			t.Errorf("phase %q: %v; a retried restart goes on to submit", phase, err)
		}
	}
}

func TestRestartRefusesAMovedGenerationOrARecovery(t *testing.T) {
	for _, old := range []map[string]types.AttributeValue{item("4", ""), item("2", "CLAIMED"), item("3", "GENERATION_ADVANCED")} {
		f := &Fencing{db: &fakeFencingDB{old: old}, table: "fencing"}
		if err := f.Restart(t.Context(), "aa11", 2, restartAt); !errors.Is(err, model.ErrGenerationMoved) {
			t.Errorf("item %v: err = %v, want ErrGenerationMoved", old, err)
		}
	}
	f := &Fencing{db: &fakeFencingDB{fail: errors.New("throttled")}, table: "fencing"}
	if err := f.Restart(t.Context(), "aa11", 2, restartAt); err == nil || errors.Is(err, model.ErrGenerationMoved) {
		t.Errorf("err = %v, want the DynamoDB error", err)
	}
}
