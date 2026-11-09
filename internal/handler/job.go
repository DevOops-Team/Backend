package handler

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service"
)

// maxRequestBytes caps a job submission; it is a small JSON document.
const maxRequestBytes = 1 << 20

type jobHandler struct {
	jobs JobService
}

func healthz(c *gin.Context) { c.String(http.StatusOK, "ok") }

// create handles POST /api/jobs. Unknown top-level keys are errors, so a typo in a field name
// cannot silently fall back to a default.
func (h *jobHandler) create(c *gin.Context) {
	dec := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, maxRequestBytes))
	dec.DisallowUnknownFields()
	var req model.SubmitJobRequest
	if err := dec.Decode(&req); err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "invalid JSON body: " + err.Error(), Code: "VALIDATION"})
		return
	}
	created, err := h.jobs.Submit(c.Request.Context(), req)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

func (h *jobHandler) list(c *gin.Context) {
	jobs, err := h.jobs.List(c.Request.Context())
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, model.ItemList[model.Job]{Items: jobs})
}

func (h *jobHandler) get(c *gin.Context) {
	job, err := h.jobs.Get(c.Request.Context(), c.Param("id"))
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, job)
}

func (h *jobHandler) events(c *gin.Context) {
	events, err := h.jobs.Events(c.Request.Context(), c.Param("id"))
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, events)
}

func (h *jobHandler) files(c *gin.Context) {
	files, err := h.jobs.Files(c.Request.Context(), c.Param("id"))
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, files)
}

func (h *jobHandler) file(c *gin.Context) {
	content, err := h.jobs.File(c.Request.Context(), c.Param("id"), c.Param("kind"), c.Param("name"))
	writeContent(c, content, err)
}

func (h *jobHandler) models(c *gin.Context) {
	models, err := h.jobs.Models(c.Request.Context())
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, model.ItemList[model.Model]{Items: models})
}

func (h *jobHandler) modelFile(c *gin.Context) {
	content, err := h.jobs.ModelFile(c.Request.Context(), c.Param("name"), c.Param("file"))
	writeContent(c, content, err)
}

func (h *jobHandler) datasets(c *gin.Context) {
	datasets, err := h.jobs.Datasets(c.Request.Context())
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, model.ItemList[model.Dataset]{Items: datasets})
}

func (h *jobHandler) dataset(c *gin.Context) {
	dataset, err := h.jobs.Dataset(c.Request.Context(), c.Param("id"))
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, dataset)
}

func (h *jobHandler) datasetFile(c *gin.Context) {
	version, err := strconv.Atoi(c.Param("version"))
	if err != nil {
		c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: "version must be an integer", Code: "VALIDATION"})
		return
	}
	content, err := h.jobs.DatasetFile(c.Request.Context(), c.Param("id"), version, c.Param("name"))
	writeContent(c, content, err)
}

// writeContent returns the start of one file as text. X-Truncated and X-Total-Size tell the viewer
// when it is not the whole file.
func writeContent(c *gin.Context, content model.FileContent, err error) {
	if err != nil {
		fail(c, err)
		return
	}
	c.Header("X-Truncated", strconv.FormatBool(content.Truncated))
	c.Header("X-Total-Size", strconv.FormatInt(content.TotalSize, 10))
	c.Data(http.StatusOK, "text/plain; charset=utf-8", content.Data)
}

// fail maps a service error to its status and passes its message through unchanged.
func fail(c *gin.Context, err error) {
	var se *service.Error
	if !errors.As(err, &se) {
		slog.Error("unexpected error", "path", c.Request.URL.Path, "err", err)
		c.JSON(http.StatusInternalServerError, model.ErrorResponse{Error: "internal error", Code: "INTERNAL"})
		return
	}
	status := http.StatusInternalServerError
	switch se.Kind {
	case service.KindInvalid:
		status = http.StatusBadRequest
	case service.KindNotFound:
		status = http.StatusNotFound
	case service.KindConflict:
		status = http.StatusConflict
	case service.KindUnprocessable:
		status = http.StatusUnprocessableEntity
	case service.KindUpstream:
		status = http.StatusBadGateway
		slog.Error(se.Msg, "path", c.Request.URL.Path)
	}
	c.JSON(status, model.ErrorResponse{Error: se.Msg, Code: se.ErrorCode(), Fields: se.Fields})
}
