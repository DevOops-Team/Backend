package repository

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/DevOops-Team/Backend/internal/model"
)

type fakeEventDB struct {
	put     *dynamodb.PutItemInput
	queries []*dynamodb.QueryInput
	pages   [][]map[string]types.AttributeValue
}

func (f *fakeEventDB) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.put = in
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeEventDB) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.queries = append(f.queries, in)
	page := f.pages[len(f.queries)-1]
	out := &dynamodb.QueryOutput{Items: page}
	if len(f.queries) < len(f.pages) {
		out.LastEvaluatedKey = map[string]types.AttributeValue{"job_id": s("abc123"), "sk": s("next")}
	}
	return out, nil
}

func TestEventsPutWritesSchemaSharedWithJobWatcher(t *testing.T) {
	db := &fakeEventDB{}
	ev := &Events{db: db, table: "devoops-job-events", now: func() time.Time { return time.Unix(1000, 0) }}
	at := time.Date(2026, 10, 29, 2, 12, 40, 0, time.UTC)

	err := ev.Put(context.Background(), model.JobEvent{
		JobID: "abc123", Key: "2026-10-29T02:12:40.000Z#k3s#ev-1", Time: at, LastTime: at,
		Cluster: "k3s", Source: "kueue-admission", Reason: "QuotaReserved", Message: "m", Type: "Normal",
		Object: "Workload/w", Count: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	it := db.put.Item
	if *db.put.TableName != "devoops-job-events" {
		t.Errorf("table = %s", *db.put.TableName)
	}
	for k, want := range map[string]string{
		"job_id": "abc123", "sk": "2026-10-29T02:12:40.000Z#k3s#ev-1", "cluster": "k3s", "source": "kueue-admission",
		"reason": "QuotaReserved", "time": "2026-10-29T02:12:40.000Z", "last_time": "2026-10-29T02:12:40.000Z",
	} {
		if got := it[k].(*types.AttributeValueMemberS).Value; got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if got := it["count"].(*types.AttributeValueMemberN).Value; got != "2" {
		t.Errorf("count = %s", got)
	}
	if got := it["expires_at"].(*types.AttributeValueMemberN).Value; got != "605800" {
		t.Errorf("expires_at = %s, want now + 7 days in epoch seconds", got)
	}
}

func TestEventsListQueriesInTimeOrderAcrossPages(t *testing.T) {
	db := &fakeEventDB{pages: [][]map[string]types.AttributeValue{
		{{"job_id": s("abc123"), "sk": s("a"), "source": s("backend"), "reason": s("Submitted"), "time": s("2026-10-29T02:12:39.000Z"), "count": n("1")}},
		{{"job_id": s("abc123"), "sk": s("b"), "source": s("karpenter"), "reason": s("Nominated"), "time": s("2026-10-29T02:13:00.000Z"), "count": n("1")}},
	}}
	ev := &Events{db: db, table: "devoops-job-events"}

	got, err := ev.List(context.Background(), "abc123")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Reason != "Submitted" || got[1].Source != "karpenter" {
		t.Fatalf("got %+v", got)
	}
	if !got[0].Time.Equal(time.Date(2026, 10, 29, 2, 12, 39, 0, time.UTC)) {
		t.Errorf("time = %v", got[0].Time)
	}
	if q := db.queries[0]; !*q.ScanIndexForward || *q.KeyConditionExpression != "job_id = :id" {
		t.Errorf("query = %+v", q)
	}
	if db.queries[1].ExclusiveStartKey == nil {
		t.Error("second page must continue from LastEvaluatedKey")
	}
}
