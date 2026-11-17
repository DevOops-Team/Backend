package handler

import (
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

// modelFile redirects to a presigned S3 URL signed at click time, so the browser downloads the
// checkpoint file straight from S3 and nothing large passes through the backend or nginx.
func (h *jobHandler) modelFile(c *gin.Context) {
	url, err := h.jobs.ModelFileURL(c.Request.Context(), c.Param("id"), c.Param("name"))
	if err != nil {
		fail(c, err)
		return
	}
	c.Header("Cache-Control", "no-store") // a cached redirect would point at an expired signature
	c.Redirect(http.StatusFound, url)
}
