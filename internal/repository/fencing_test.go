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
	got    *dynamodb.UpdateItemInput
	put    *dynamodb.PutItemInput
	read   *dynamodb.GetItemInput
	stored map[string]types.AttributeValue // GetItem answers this item (nil = no item)
	old    map[string]types.AttributeValue // set: the condition fails with this item
	fail   error
}

func (f *fakeFencingDB) GetItem(_ context.Context, in *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	f.read = in
	if f.fail != nil {
		return nil, f.fail
	}
	return &dynamodb.GetItemOutput{Item: f.stored}, nil
}

func (f *fakeFencingDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.put = in
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

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestCreateRecordsTheJobsetHashWithGenerationOne(t *testing.T) {
	db := &fakeFencingDB{}
	f := &Fencing{db: db, table: "fencing"}
	if err := f.Create(t.Context(), "aa11", testDigest); err != nil {
		t.Fatal(err)
	}
	it := db.put.Item
	if it["job_id"].(*types.AttributeValueMemberS).Value != "aa11" || it["generation"].(*types.AttributeValueMemberN).Value != "1" ||
		it[JobsetSHA256Attr].(*types.AttributeValueMemberS).Value != testDigest {
		t.Errorf("item = %v", it)
	}
	if *db.put.ConditionExpression != "attribute_not_exists(job_id)" {
		t.Errorf("condition = %s", *db.put.ConditionExpression)
	}
}

func TestJobsetSHA256ReadsTheRecordedHashConsistently(t *testing.T) {
	stored := map[string]types.AttributeValue{"job_id": &types.AttributeValueMemberS{Value: "aa11"},
		JobsetSHA256Attr: &types.AttributeValueMemberS{Value: testDigest}}
	db := &fakeFencingDB{stored: stored}
	f := &Fencing{db: db, table: "fencing"}
	got, err := f.JobsetSHA256(t.Context(), "aa11")
	if err != nil || got != testDigest {
		t.Fatalf("JobsetSHA256 = %q, %v", got, err)
	}
	if !*db.read.ConsistentRead || db.read.Key["job_id"].(*types.AttributeValueMemberS).Value != "aa11" {
		t.Errorf("read = %+v", db.read)
	}

	// An item from before GPU-119 has no hash: "" (the service answers 422), not "no item".
	db.stored = map[string]types.AttributeValue{"job_id": &types.AttributeValueMemberS{Value: "aa11"}}
	if got, err := f.JobsetSHA256(t.Context(), "aa11"); err != nil || got != "" {
		t.Errorf("item without hash = %q, %v", got, err)
	}
	db.stored = nil
	if _, err := f.JobsetSHA256(t.Context(), "aa11"); !errors.Is(err, model.ErrNotFound) {
		t.Errorf("no item = %v, want ErrNotFound", err)
	}
}
