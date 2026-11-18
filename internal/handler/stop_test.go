package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DevOops-Team/Backend/internal/service"
)

type fakeStop struct {
	JobService
	err error
	got *string
}

func (f fakeStop) Stop(_ context.Context, id string) error {
	*f.got = id
	return f.err
}

func TestStopJobRoute(t *testing.T) {
	post := func(jobs JobService) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		newRouter(jobs).ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/jobs/0cb3b0c3/stop", nil))
		return rec
	}
	var got string
	if rec := post(fakeStop{got: &got}); rec.Code != http.StatusAccepted || got != "0cb3b0c3" {
		t.Errorf("POST stop = %d for %q", rec.Code, got)
	}
	rec := post(fakeStop{got: &got, err: &service.Error{Kind: service.KindConflict, Code: "RECOVERING", Msg: "being moved"}})
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "RECOVERING") {
		t.Errorf("POST stop while recovering = %d %s", rec.Code, rec.Body)
	}
}
