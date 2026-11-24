package handler

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/DevOops-Team/Backend/internal/model"
)

// RecoveryLookup is what the internal recovery API needs from internal/service.RecoveryService.
type RecoveryLookup interface {
	Metadata(ctx context.Context, id string, generation int) (model.RecoveryMetadata, error)
}

// Internal is the service-to-service API under /internal, for the recovery controller (GPU-119). It is not
// behind the frontend's /api/ proxy, so only in-cluster callers reach it.
type Internal struct {
	Recovery RecoveryLookup
	// Token is the bearer token callers must send (RECOVERY_API_TOKEN). Empty = the API is not configured
	// and every request gets 503.
	Token string
}

// serviceAuth checks Authorization: Bearer <token> against the configured token in constant time.
func serviceAuth(token string) gin.HandlerFunc {
	want := sha256.Sum256([]byte(token))
	return func(c *gin.Context) {
		if token == "" {
			c.AbortWithStatusJSON(http.StatusServiceUnavailable,
				model.ErrorResponse{Error: "the internal API has no service token configured", Code: "NOT_CONFIGURED"})
			return
		}
		scheme, got, ok := strings.Cut(c.GetHeader("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || got == "" {
			c.Header("WWW-Authenticate", `Bearer realm="internal"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized,
				model.ErrorResponse{Error: "Authorization: Bearer <service token> required", Code: "UNAUTHORIZED"})
			return
		}
		sum := sha256.Sum256([]byte(got))
		if subtle.ConstantTimeCompare(sum[:], want[:]) != 1 {
			c.Header("WWW-Authenticate", `Bearer realm="internal", error="invalid_token"`)
			c.AbortWithStatusJSON(http.StatusUnauthorized,
				model.ErrorResponse{Error: "invalid service token", Code: "UNAUTHORIZED"})
			return
		}
		c.Next()
	}
}

type recoveryHandler struct {
	recovery RecoveryLookup
}

// metadata answers GET /internal/v1/recovery/jobs/:id?generation=N (K8s recovery-controller
// docs/backend-metadata-api.md v1).
func (h *recoveryHandler) metadata(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	raw := c.Query("generation")
	gen, err := strconv.Atoi(raw)
	if err != nil || strings.TrimSpace(raw) != raw || strings.HasPrefix(raw, "+") || gen < 1 {
		c.JSON(http.StatusUnprocessableEntity,
			model.ErrorResponse{Error: "generation must be an integer of at least 1", Code: "INVALID_REQUEST"})
		return
	}
	md, err := h.recovery.Metadata(c.Request.Context(), c.Param("id"), gen)
	if err != nil {
		fail(c, err)
		return
	}
	c.JSON(http.StatusOK, md)
}
