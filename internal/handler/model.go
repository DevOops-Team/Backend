package handler

import (
	"log/slog"
	"mime"
	"net/http"

	"github.com/gin-gonic/gin"
)

// model returns the trained model of a finished job: its final checkpoint and the files in it.
func (h *jobHandler) model(c *gin.Context) {
	m, err := h.jobs.Model(c.Request.Context(), c.Param("id"))
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, m)
}

// modelFile streams one checkpoint file from S3 without holding it in memory.
func (h *jobHandler) modelFile(c *gin.Context) {
	f, err := h.jobs.ModelFile(c.Request.Context(), c.Param("id"), c.Param("name"))
	if err != nil {
		fail(c, err)
		return
	}
	defer f.Body.Close()
	c.DataFromReader(http.StatusOK, f.Size, "application/octet-stream", f.Body, map[string]string{
		"Content-Disposition": attachment(f.Name),
	})
}

// modelArchive streams the whole model as a zip. Errors before the first byte map to a status; after it
// the response is already 200, so a failure is logged and the connection is cut (the client sees a
// broken download, never a truncated zip that looks whole).
func (h *jobHandler) modelArchive(c *gin.Context) {
	a, err := h.jobs.ModelArchive(c.Request.Context(), c.Param("id"))
	if err != nil {
		fail(c, err)
		return
	}
	c.Header("Content-Type", "application/zip")
	c.Header("Content-Disposition", attachment(a.Name))
	c.Status(http.StatusOK)
	if err := a.Write(c.Writer); err != nil {
		slog.Error("model archive", "path", c.Request.URL.Path, "err", err)
		cut(c)
	}
}

// cut closes the connection without ending the chunked body, so the client and nginx see an incomplete
// response. gin.Recovery swallows http.ErrAbortHandler, and gin refuses to hijack once body bytes are
// written, so this flushes (headers go out as chunked) and hijacks the net/http writer underneath.
func cut(c *gin.Context) {
	c.Abort()
	c.Writer.Flush()
	var rw http.ResponseWriter = c.Writer
	if u, ok := rw.(interface{ Unwrap() http.ResponseWriter }); ok {
		rw = u.Unwrap()
	}
	if conn, _, err := http.NewResponseController(rw).Hijack(); err == nil {
		_ = conn.Close()
	}
}

func attachment(name string) string {
	return mime.FormatMediaType("attachment", map[string]string{"filename": name})
}
