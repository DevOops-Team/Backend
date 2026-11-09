// Package api holds the OpenAPI (Swagger) spec of the backend, maintained by hand in openapi.yaml.
// internal/handler serves it at /openapi.yaml and /swagger/, and its tests check it against the routes.
package api

import _ "embed"

//go:embed openapi.yaml
var OpenAPISpec []byte
