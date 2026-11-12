package handler

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/DevOops-Team/Backend/internal/model"
	"github.com/DevOops-Team/Backend/internal/service"
)

// maxSpecBytes caps the "spec" part, a small JSON document.
const maxSpecBytes = 64 << 10

type jobHandler struct {
	jobs JobService
}

func healthz(c *gin.Context) { c.String(http.StatusOK, "ok") }

func (h *jobHandler) options(c *gin.Context) {
	c.JSON(http.StatusOK, h.jobs.Options())
}

// create handles the multipart POST /api/jobs: first the "spec" JSON part, then the model folder
// ("model/<folder>/<path>") and the training data ("data/<file>.jsonl") as file parts. The parts are
// streamed to the service one by one, so the request body is never held in memory or on disk.
func (h *jobHandler) create(c *gin.Context) {
	mr, err := c.Request.MultipartReader()
	if err != nil {
		bad(c, "multipart/form-data body required: "+err.Error())
		return
	}
	first, err := mr.NextPart()
	if err != nil || first.FormName() != "spec" {
		bad(c, `the first part must be "spec" (JSON)`)
		return
	}
	dec := json.NewDecoder(io.LimitReader(first, maxSpecBytes))
	dec.DisallowUnknownFields()
	var spec model.SubmitSpec
	if err := dec.Decode(&spec); err != nil {
		bad(c, "invalid spec JSON: "+err.Error())
		return
	}
	created, err := h.jobs.Submit(c.Request.Context(), spec, &partSource{r: mr})
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusCreated, created)
}

// partSource turns multipart parts into uploads. The path rides in the form name, because
// Part.FileName() keeps only the base name and a model folder has subdirectories.
type partSource struct {
	r *multipart.Reader
}

func (p *partSource) Next() (service.Upload, error) {
	part, err := p.r.NextPart()
	if err != nil {
		return service.Upload{}, err // io.EOF after the last part
	}
	field, path, _ := strings.Cut(part.FormName(), "/")
	return service.Upload{Field: field, Path: path, Body: part}, nil
}

func bad(c *gin.Context, msg string) {
	c.JSON(http.StatusBadRequest, model.ErrorResponse{Error: msg, Code: "VALIDATION"})
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
	case service.KindTooLarge:
		status = http.StatusRequestEntityTooLarge
	case service.KindUpstream:
		status = http.StatusBadGateway
		slog.Error(se.Msg, "path", c.Request.URL.Path)
	}
	c.JSON(status, model.ErrorResponse{Error: se.Msg, Code: se.ErrorCode(), Fields: se.Fields})
}
