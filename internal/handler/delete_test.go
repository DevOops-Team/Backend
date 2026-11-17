package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DevOops-Team/Backend/internal/service"
)

type fakeDelete struct {
	JobService
	err error
	got *string
}

func (f fakeDelete) Delete(_ context.Context, id string) error {
	*f.got = id
	return f.err
}

func del(t *testing.T, jobs JobService, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	newRouter(jobs).ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, path, nil))
	return rec
}

func TestDeleteJobRoute(t *testing.T) {
	var got string
	rec := del(t, fakeDelete{got: &got}, "/api/jobs/0cb3b0c3")
	if rec.Code != http.StatusNoContent || got != "0cb3b0c3" {
		t.Errorf("DELETE = %d for %q", rec.Code, got)
	}

	rec = del(t, fakeDelete{got: &got, err: &service.Error{Kind: service.KindConflict, Code: "NOT_FINISHED", Msg: "only finished jobs can be deleted"}}, "/api/jobs/0cb3b0c3")
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "NOT_FINISHED") {
		t.Errorf("DELETE running = %d %s", rec.Code, rec.Body)
	}
}
