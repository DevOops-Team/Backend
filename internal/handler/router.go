// Package handler is the HTTP layer (Gin): it parses requests, calls the service and maps
// service errors to status codes. Business rules live in internal/service.
package handler

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/DevOops-Team/Backend/internal/middleware"
	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service"
)

// JobService is what the handlers need from internal/service.JobService.
type JobService interface {
	Options() model.TrainingOptions
	Submit(ctx context.Context, spec model.SubmitSpec, src service.UploadSource) (model.CreatedJob, error)
	List(ctx context.Context) ([]model.Job, error)
	Get(ctx context.Context, id string) (model.Job, error)
	Events(ctx context.Context, id string) ([]model.JobEvent, error)
	Files(ctx context.Context, id string) ([]model.File, error)
	File(ctx context.Context, id, kind, name string) (model.FileContent, error)
	Model(ctx context.Context, id string) (model.TrainedModel, error)
	ModelFileURL(ctx context.Context, id, name string) (string, error)
	Delete(ctx context.Context, id string) error
	Stop(ctx context.Context, id string) error
	Restart(ctx context.Context, id string) error
}

// NewRouter wires middleware and routes. The API routes must match api/openapi.yaml
// (TestOpenAPISpecMatchesRoutes); the docs routes are not part of the API.
func NewRouter(jobs JobService, allowedOrigin string, internal Internal) *gin.Engine {
	r := gin.New()
	r.Use(middleware.Logger(), gin.Recovery(), middleware.CORS(allowedOrigin))
	if err := r.SetTrustedProxies(nil); err != nil { // no proxy in front; ignore X-Forwarded-For
		panic(err)
	}

	h := &jobHandler{jobs: jobs}
	r.GET("/healthz", healthz)
	r.GET("/api/training/options", h.options)
	r.POST("/api/jobs", h.create)
	r.GET("/api/jobs", h.list)
	r.GET("/api/jobs/:id", h.get)
	r.DELETE("/api/jobs/:id", h.remove)
	r.POST("/api/jobs/:id/stop", h.stop)
	r.POST("/api/jobs/:id/restart", h.restart)
	r.GET("/api/jobs/:id/events", h.events)
	r.GET("/api/jobs/:id/files", h.files)
	r.GET("/api/jobs/:id/files/:kind/:name", h.file)
	r.GET("/api/jobs/:id/model", h.model)
	r.GET("/api/jobs/:id/model/files/:name", h.modelFile)

	// Service-to-service API for the recovery controller (GPU-119); bearer token, not proxied by the frontend.
	in := r.Group("/internal/v1", serviceAuth(internal.Token))
	in.GET("/recovery/jobs/:id", (&recoveryHandler{recovery: internal.Recovery}).metadata)

	r.GET("/openapi.yaml", openAPISpec)
	r.GET("/swagger/", swaggerUI)
	return r
}

// docsPaths are served next to the API but not documented in it.
var docsPaths = map[string]bool{"/openapi.yaml": true, "/swagger/": true}
