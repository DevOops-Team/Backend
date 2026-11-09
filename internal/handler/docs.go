package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/DevOops-Team/Backend/api"
)

func openAPISpec(c *gin.Context) {
	c.Data(http.StatusOK, "application/yaml; charset=utf-8", api.OpenAPISpec)
}

// swaggerUIVersion pins swagger-ui-dist, loaded by the browser from jsDelivr.
const swaggerUIVersion = "5.34.0"

const swaggerUIPage = `<!doctype html>
<html lang="ko">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DevOops Backend API</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@` + swaggerUIVersion + `/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@` + swaggerUIVersion + `/swagger-ui-bundle.js"></script>
<script>
window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui" });
</script>
</body>
</html>
`

func swaggerUI(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(swaggerUIPage))
}
