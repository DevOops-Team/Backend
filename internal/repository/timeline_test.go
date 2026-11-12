package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DevOops-Team/Backend/internal/model"
)

func ev(job, key string) model.JobEvent {
	return model.JobEvent{JobID: job, Key: key, Reason: key, Time: time.Unix(0, 0)}
}

func TestTimelineListsOneJobOldestFirstAndOverwritesSameKey(t *testing.T) {
	tl := NewTimeline(10)
	ctx := context.Background()
	for _, e := range []model.JobEvent{ev("a", "2"), ev("b", "1"), ev("a", "1"), ev("a", "2")} {
		if err := tl.Put(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	got, err := tl.List(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Key != "1" || got[1].Key != "2" {
		t.Errorf("a = %+v, want keys 1, 2 once each", got)
	}
	if got, _ := tl.List(ctx, "none"); got == nil || len(got) != 0 {
		t.Errorf("unknown job = %#v, want an empty list", got)
	}
}

func TestTimelineKeepsTheNewestEntriesPerJob(t *testing.T) {
	tl := NewTimeline(3)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = tl.Put(ctx, ev("a", fmt.Sprintf("%02d", i)))
	}
	got, _ := tl.List(ctx, "a")
	if len(got) != 3 || got[0].Key != "02" || got[2].Key != "04" {
		t.Errorf("a = %+v, want the last three", got)
	}
}
