package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeBatchGetter struct {
	items    map[string]map[string]types.AttributeValue
	requests [][]string
}

func (f *fakeBatchGetter) BatchGetItem(_ context.Context, in *dynamodb.BatchGetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	var ids []string
	out := map[string][]map[string]types.AttributeValue{}
	for table, ka := range in.RequestItems {
		for _, k := range ka.Keys {
			id := k["job_id"].(*types.AttributeValueMemberS).Value
			ids = append(ids, id)
			if item, ok := f.items[id]; ok {
				out[table] = append(out[table], item)
			}
		}
	}
	f.requests = append(f.requests, ids)
	return &dynamodb.BatchGetItemOutput{Responses: out}, nil
}

func n(v string) *types.AttributeValueMemberN { return &types.AttributeValueMemberN{Value: v} }
func s(v string) *types.AttributeValueMemberS { return &types.AttributeValueMemberS{Value: v} }

func TestProgressDecodesItemWrittenByTrainPy(t *testing.T) {
	db := &fakeBatchGetter{items: map[string]map[string]types.AttributeValue{
		"abc123": {
			"job_id": s("abc123"), "status": s("RUNNING"), "step": n("1500"), "total_steps": n("5000"),
			"loss": n("0.42"), "last_checkpoint_step": n("1000"), "node_name": s("ip-10-0-1-2"), "updated_at": n("1730000000000"),
		},
	}}
	p := &Progress{db: db, table: "devoops-job-progress"}

	got, err := p.Get(context.Background(), []string{"abc123", "missing"})
	if err != nil {
		t.Fatal(err)
	}

	item, ok := got["abc123"]
	if !ok {
		t.Fatalf("abc123 missing from %v", got)
	}
	if item.Step != 1500 || item.TotalSteps != 5000 || item.Loss != 0.42 || item.LastCheckpointStep != 1000 || item.NodeName != "ip-10-0-1-2" || item.Status != "RUNNING" {
		t.Errorf("decoded %+v", item)
	}
	if _, ok := got["missing"]; ok {
		t.Error("job without progress should be absent")
	}
}

func TestProgressSplitsRequestsAtDynamoDBBatchLimit(t *testing.T) {
	db := &fakeBatchGetter{}
	p := &Progress{db: db, table: "t"}
	ids := make([]string, 250)
	for i := range ids {
		ids[i] = fmt.Sprintf("job%d", i)
	}

	if _, err := p.Get(context.Background(), ids); err != nil {
		t.Fatal(err)
	}

	if len(db.requests) != 3 || len(db.requests[0]) != 100 || len(db.requests[2]) != 50 {
		t.Errorf("batch sizes = %d requests, want 100/100/50", len(db.requests))
	}
}

func TestProgressSkipsCallForNoJobs(t *testing.T) {
	db := &fakeBatchGetter{}
	p := &Progress{db: db, table: "t"}

	if _, err := p.Get(context.Background(), nil); err != nil {
		t.Fatal(err)
	}

	if len(db.requests) != 0 {
		t.Errorf("made %d requests for no jobs", len(db.requests))
	}
}
