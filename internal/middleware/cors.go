// Package middleware holds the Gin middleware every request goes through.
package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// CORS allows the one frontend origin. Preflight (OPTIONS) is answered here: there are no OPTIONS
// routes, and Gin runs global middleware on unmatched routes too.
func CORS(allowedOrigin string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if origin := c.GetHeader("Origin"); origin != "" && origin == allowedOrigin {
			h := c.Writer.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Content-Type")
			h.Set("Access-Control-Expose-Headers", "X-Truncated, X-Total-Size")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
