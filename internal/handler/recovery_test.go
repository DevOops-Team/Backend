package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service"
)

type fakeRecovery struct {
	md     model.RecoveryMetadata
	err    error
	gotID  *string
	gotGen *int
	called *bool
}

func (f fakeRecovery) Metadata(_ context.Context, id string, gen int) (model.RecoveryMetadata, error) {
	*f.called, *f.gotID, *f.gotGen = true, id, gen
	return f.md, f.err
}

const recoveryToken = "s3cr3t-service-token"

func recoveryGet(internal Internal, path, auth string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	NewRouter(nil, "http://localhost:5173", internal).ServeHTTP(rec, req)
	return rec
}

func newFakeRecovery(md model.RecoveryMetadata, err error) (fakeRecovery, *bool, *string, *int) {
	called, id, gen := new(bool), new(string), new(int)
	return fakeRecovery{md: md, err: err, called: called, gotID: id, gotGen: gen}, called, id, gen
}

func TestRecoveryMetadataRoute(t *testing.T) {
	md := model.RecoveryMetadata{SchemaVersion: 1, JobID: "abc123", Generation: 2, RecoveryEligible: true,
		ActiveNodes: []string{"n1"}, AllowRestartFromScratch: true}
	f, _, id, gen := newFakeRecovery(md, nil)
	rec := recoveryGet(Internal{Recovery: f, Token: recoveryToken}, "/internal/v1/recovery/jobs/abc123?generation=2",
		"Bearer "+recoveryToken)
	if rec.Code != http.StatusOK || *id != "abc123" || *gen != 2 {
		t.Fatalf("GET = %d %s (id %q gen %d)", rec.Code, rec.Body, *id, *gen)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schema_version", "job_id", "generation", "recovery_eligible", "jobset_uri", "jobset_sha256",
		"gpu_count", "cluster", "active_nodes", "placement_generation", "jobset_name", "jobset_uid", "observed_at",
		"allow_restart_from_scratch"} {
		if _, ok := got[key]; !ok {
			t.Errorf("response has no %s: %s", key, rec.Body)
		}
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q", rec.Header().Get("Cache-Control"))
	}
}

func TestRecoveryMetadataNeedsTheServiceToken(t *testing.T) {
	path := "/internal/v1/recovery/jobs/abc123?generation=1"
	for name, tc := range map[string]struct {
		token, auth string
		want        int
		code        string
	}{
		"not configured": {token: "", auth: "Bearer anything", want: http.StatusServiceUnavailable, code: "NOT_CONFIGURED"},
		"no header":      {token: recoveryToken, want: http.StatusUnauthorized, code: "UNAUTHORIZED"},
		"other scheme":   {token: recoveryToken, auth: "Basic " + recoveryToken, want: http.StatusUnauthorized, code: "UNAUTHORIZED"},
		"empty bearer":   {token: recoveryToken, auth: "Bearer ", want: http.StatusUnauthorized, code: "UNAUTHORIZED"},
		"wrong token":    {token: recoveryToken, auth: "Bearer nope", want: http.StatusUnauthorized, code: "UNAUTHORIZED"},
		"token prefix":   {token: recoveryToken, auth: "Bearer " + recoveryToken[:5], want: http.StatusUnauthorized, code: "UNAUTHORIZED"},
	} {
		f, called, _, _ := newFakeRecovery(model.RecoveryMetadata{}, nil)
		rec := recoveryGet(Internal{Recovery: f, Token: tc.token}, path, tc.auth)
		if rec.Code != tc.want || !strings.Contains(rec.Body.String(), tc.code) || *called {
			t.Errorf("%s: %d %s (service called %v), want %d %s", name, rec.Code, rec.Body, *called, tc.want, tc.code)
		}
	}
	// The scheme is case-insensitive.
	f, called, _, _ := newFakeRecovery(model.RecoveryMetadata{}, nil)
	if rec := recoveryGet(Internal{Recovery: f, Token: recoveryToken}, path, "bearer "+recoveryToken); rec.Code != http.StatusOK || !*called {
		t.Errorf("lowercase bearer = %d", rec.Code)
	}
}

func TestRecoveryMetadataGenerationParameter(t *testing.T) {
	for _, q := range []string{"", "?generation=", "?generation=0", "?generation=-1", "?generation=1.5", "?generation=x",
		"?generation=+1", "?generation=%201"} {
		f, called, _, _ := newFakeRecovery(model.RecoveryMetadata{}, nil)
		rec := recoveryGet(Internal{Recovery: f, Token: recoveryToken}, "/internal/v1/recovery/jobs/abc123"+q, "Bearer "+recoveryToken)
		if rec.Code != http.StatusUnprocessableEntity || *called {
			t.Errorf("%q: %d %s (service called %v), want 422", q, rec.Code, rec.Body, *called)
		}
	}
}

func TestRecoveryMetadataErrorStatuses(t *testing.T) {
	for want, err := range map[int]error{
		http.StatusNotFound:            &service.Error{Kind: service.KindNotFound, Code: "GENERATION_NOT_FOUND", Msg: "gone"},
		http.StatusUnprocessableEntity: &service.Error{Kind: service.KindUnprocessable, Code: "JOBSET_DIGEST_MISSING", Msg: "old"},
		http.StatusServiceUnavailable:  &service.Error{Kind: service.KindUnavailable, Code: "PLACEMENT_UNAVAILABLE", Msg: "later"},
	} {
		f, _, _, _ := newFakeRecovery(model.RecoveryMetadata{}, err)
		rec := recoveryGet(Internal{Recovery: f, Token: recoveryToken}, "/internal/v1/recovery/jobs/abc123?generation=1",
			"Bearer "+recoveryToken)
		if rec.Code != want || !strings.Contains(rec.Body.String(), err.(*service.Error).Code) {
			t.Errorf("%v: %d %s, want %d", err, rec.Code, rec.Body, want)
		}
	}
}
