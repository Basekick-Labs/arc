package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
)

// metricsGetJSON serves one GET through handler and decodes the JSON body.
func metricsGetJSON(t *testing.T, path string, handler fiber.Handler, accept string) map[string]any {
	t.Helper()
	app := fiber.New()
	app.Get(path, handler)
	req := httptest.NewRequest("GET", path, nil)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	return body
}

// TestQueryPoolHasNoDeadQueryCounters pins that /api/v1/metrics/query-pool no
// longer serves pool.queries_total / pool.query_errors_total.
//
// Regression test for #1747: both mapped to DuckDB-level counters that nothing
// increments, so they read 0 beside the real query_requests / query_errors,
// and a dashboard reading queries_total concluded the node served no queries.
// Absence is checked by key, not by value: a leftover mapping to a missing
// snapshot key would serve null, which a nil or zero check would accept.
func TestQueryPoolHasNoDeadQueryCounters(t *testing.T) {
	s := &Server{}
	before := metricsGetJSON(t, "/api/v1/metrics/query-pool", s.queryPoolMetricsHandler, "")
	pool, ok := before["pool"].(map[string]any)
	if !ok {
		t.Fatalf("pool = %T, want an object", before["pool"])
	}
	for _, k := range []string{"queries_total", "query_errors_total"} {
		if _, ok := pool[k]; ok {
			t.Errorf("pool serves %q, a counter nothing increments (#1747)", k)
		}
	}

	// The real counters stay, and move.
	errs, ok := pool["query_errors"].(float64)
	if !ok {
		t.Fatalf("pool.query_errors = %v, want a number", pool["query_errors"])
	}
	for _, k := range []string{"query_requests", "query_success"} {
		if _, ok := pool[k].(float64); !ok {
			t.Errorf("pool.%s = %v, want a number", k, pool[k])
		}
	}
	metrics.Get().IncQueryErrors()
	after := metricsGetJSON(t, "/api/v1/metrics/query-pool", s.queryPoolMetricsHandler, "")
	if got := after["pool"].(map[string]any)["query_errors"].(float64); got-errs != 1 {
		t.Errorf("pool.query_errors moved by %v after one IncQueryErrors, want 1", got-errs)
	}
}

// TestJSONMetricsHaveNoDeadQueryCounters pins the two other JSON surfaces of
// the snapshot: /api/v1/metrics and /metrics with Accept: application/json
// (#1747).
func TestJSONMetricsHaveNoDeadQueryCounters(t *testing.T) {
	s := &Server{}
	for _, tc := range []struct {
		path    string
		handler fiber.Handler
		accept  string
	}{
		{"/api/v1/metrics", s.apiMetricsHandler, ""},
		{"/metrics", s.metricsHandler, "application/json"},
	} {
		body := metricsGetJSON(t, tc.path, tc.handler, tc.accept)
		if _, ok := body["query_requests_total"]; !ok {
			t.Errorf("%s: query_requests_total missing; not the snapshot?", tc.path)
		}
		for _, k := range []string{"db_queries_total", "db_query_errors_total"} {
			if _, ok := body[k]; ok {
				t.Errorf("%s serves %q, a counter nothing increments (#1747)", tc.path, k)
			}
		}
	}
}
