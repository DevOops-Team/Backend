package handler

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service"
)

// fakeModels serves only the trained-model methods; the rest of JobService stays nil.
type fakeModels struct {
	JobService
	err error
}

func (f fakeModels) Model(context.Context, string) (model.TrainedModel, error) {
	return model.TrainedModel{JobID: "0cb3b0c3", Step: 694, Files: []model.ModelFile{{Name: "__0_0.distcp", Size: 6}}}, f.err
}

func (f fakeModels) ModelFileURL(_ context.Context, _, name string) (string, error) {
	return "https://bucket.s3.us-east-2.amazonaws.com/jobs/0cb3b0c3/" + name + "?X-Amz-Signature=x", f.err
}

func TestModelRoutes(t *testing.T) {
	r := newRouter(fakeModels{})

	rec := get(r, "/api/jobs/0cb3b0c3/model")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"step":694`) {
		t.Errorf("GET model = %d %s", rec.Code, rec.Body)
	}

	// The browser follows the redirect and downloads straight from S3.
	rec = get(r, "/api/jobs/0cb3b0c3/model/files/__0_0.distcp")
	if rec.Code != http.StatusFound ||
		rec.Header().Get("Location") != "https://bucket.s3.us-east-2.amazonaws.com/jobs/0cb3b0c3/__0_0.distcp?X-Amz-Signature=x" ||
		rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("GET model file = %d headers %v", rec.Code, rec.Header())
	}
}

func TestModelRoutesMapServiceErrors(t *testing.T) {
	r := newRouter(fakeModels{err: &service.Error{Kind: service.KindConflict, Msg: "training has not finished"}})
	for _, p := range []string{"/api/jobs/x/model", "/api/jobs/x/model/files/a"} {
		if rec := get(r, p); rec.Code != http.StatusConflict {
			t.Errorf("GET %s = %d, want 409", p, rec.Code)
		}
	}
}
