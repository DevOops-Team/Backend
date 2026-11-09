package handler

import (
	"net/http"
	"regexp"
	"sort"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"

	"github.com/DevOops-Team/Backend/api"
)

var ginParam = regexp.MustCompile(`:(\w+)`)

// openapi.yaml is maintained by hand, so this keeps it in step with the routes Gin actually serves.
func TestOpenAPISpecMatchesRoutes(t *testing.T) {
	var spec struct {
		OpenAPI string                               `json:"openapi"`
		Paths   map[string]map[string]map[string]any `json:"paths"`
	}
	if err := yaml.Unmarshal(api.OpenAPISpec, &spec); err != nil {
		t.Fatalf("api/openapi.yaml: %v", err)
	}
	if !strings.HasPrefix(spec.OpenAPI, "3.") {
		t.Fatalf("openapi = %q, want 3.x", spec.OpenAPI)
	}

	var documented []string
	for p, ops := range spec.Paths {
		for method := range ops {
			documented = append(documented, strings.ToUpper(method)+" "+p)
		}
	}
	var served []string
	for _, rt := range newRouter(&fakeJobs{}).Routes() {
		if docsPaths[rt.Path] {
			continue
		}
		served = append(served, rt.Method+" "+ginParam.ReplaceAllString(rt.Path, "{$1}"))
	}
	sort.Strings(documented)
	sort.Strings(served)
	if strings.Join(documented, "\n") != strings.Join(served, "\n") {
		t.Errorf("api/openapi.yaml paths and Gin routes differ\nspec:\n  %s\nroutes:\n  %s",
			strings.Join(documented, "\n  "), strings.Join(served, "\n  "))
	}
}

func TestServesOpenAPISpecAndSwaggerUI(t *testing.T) {
	r := newRouter(&fakeJobs{})

	rec := get(r, "/openapi.yaml")
	if rec.Code != http.StatusOK || rec.Body.String() != string(api.OpenAPISpec) {
		t.Fatalf("GET /openapi.yaml = %d, want the embedded spec", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/yaml") {
		t.Errorf("Content-Type = %q, want application/yaml", ct)
	}

	rec = get(r, "/swagger/")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `url: "/openapi.yaml"`) {
		t.Fatalf("GET /swagger/ = %d, want Swagger UI pointing at /openapi.yaml:\n%s", rec.Code, rec.Body)
	}
}
