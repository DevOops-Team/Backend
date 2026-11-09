package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

const frontend = "http://localhost:5173"

func router() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(CORS(frontend))
	r.GET("/api/jobs", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	return r
}

func serve(r http.Handler, method, origin string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/api/jobs", nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	return rec
}

// POST /api/jobs from the browser is preceded by a preflight; there is no OPTIONS route.
func TestCORSAnswersPreflightForFrontendOrigin(t *testing.T) {
	rec := serve(router(), http.MethodOptions, frontend)

	if rec.Code != http.StatusNoContent {
		t.Errorf("status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != frontend {
		t.Errorf("allow-origin = %q", got)
	}
	if got := rec.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, "POST") {
		t.Errorf("allow-methods = %q", got)
	}
}

func TestCORSExposesPreviewHeadersToFrontend(t *testing.T) {
	rec := serve(router(), http.MethodGet, frontend)

	got := rec.Header().Get("Access-Control-Expose-Headers")
	if !strings.Contains(got, "X-Truncated") || !strings.Contains(got, "X-Total-Size") {
		t.Errorf("expose-headers = %q, browser cannot read truncation headers", got)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, request should reach the handler", rec.Code)
	}
}

func TestCORSAllowsNoOtherOrigin(t *testing.T) {
	rec := serve(router(), http.MethodGet, "https://evil.example")

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("allow-origin = %q for a foreign origin", got)
	}
}
