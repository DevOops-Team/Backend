package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service"
)

// fakeModels serves only the trained-model methods; the rest of JobService stays nil.
type fakeModels struct {
	JobService
	err     error
	written error
}

func (f fakeModels) Model(context.Context, string) (model.TrainedModel, error) {
	return model.TrainedModel{JobID: "0cb3b0c3", Step: 694, Files: []model.ModelFile{{Name: "__0_0.distcp", Size: 6}}}, f.err
}

func (f fakeModels) ModelFile(_ context.Context, _, name string) (model.ObjectStream, error) {
	if f.err != nil {
		return model.ObjectStream{}, f.err
	}
	return model.ObjectStream{Name: name, Body: io.NopCloser(strings.NewReader("shard0")), Size: 6}, nil
}

func (f fakeModels) ModelArchive(context.Context, string) (service.ModelArchive, error) {
	return service.ModelArchive{Name: "qwen-step-694.zip", Write: func(w io.Writer) error {
		_, _ = io.WriteString(w, "PK")
		return f.written
	}}, f.err
}

func TestModelRoutes(t *testing.T) {
	r := newRouter(fakeModels{})

	rec := get(r, "/api/jobs/0cb3b0c3/model")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"step":694`) {
		t.Errorf("GET model = %d %s", rec.Code, rec.Body)
	}

	rec = get(r, "/api/jobs/0cb3b0c3/model/files/__0_0.distcp")
	if rec.Code != http.StatusOK || rec.Body.String() != "shard0" || rec.Header().Get("Content-Length") != "6" ||
		rec.Header().Get("Content-Disposition") != `attachment; filename=__0_0.distcp` {
		t.Errorf("GET model file = %d %q headers %v", rec.Code, rec.Body, rec.Header())
	}

	rec = get(r, "/api/jobs/0cb3b0c3/model/archive")
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/zip" ||
		rec.Header().Get("Content-Disposition") != `attachment; filename=qwen-step-694.zip` {
		t.Errorf("GET archive = %d headers %v", rec.Code, rec.Header())
	}
}

func TestModelRoutesMapServiceErrors(t *testing.T) {
	r := newRouter(fakeModels{err: &service.Error{Kind: service.KindConflict, Msg: "training has not finished"}})
	for _, p := range []string{"/api/jobs/x/model", "/api/jobs/x/model/files/a", "/api/jobs/x/model/archive"} {
		if rec := get(r, p); rec.Code != http.StatusConflict {
			t.Errorf("GET %s = %d, want 409", p, rec.Code)
		}
	}
}

// A failure after the first byte must not end the body normally: the connection is cut instead.
func TestModelArchiveCutsTheConnectionOnAStreamError(t *testing.T) {
	srv := httptest.NewServer(newRouter(fakeModels{written: errors.New("s3 reset")}))
	defer srv.Close()
	res, err := http.Get(srv.URL + "/api/jobs/0cb3b0c3/model/archive")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if _, err := io.ReadAll(res.Body); err == nil {
		t.Errorf("read the whole body without error; want an unexpected EOF (headers %v, TE %v)", res.Header, res.TransferEncoding)
	}
}
