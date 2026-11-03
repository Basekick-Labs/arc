package api

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
	recoverMiddleware "github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/rs/zerolog"
)

// #764: a transform that reads a scope the permission check did not see is
// refused on every query endpoint before anything runs. The divergence is
// staged through the transform cache: the extractor sees db1.cpu, which the
// token may read, while the cached transform reads db2.secret, which it may
// not. Before the emitted scope was checked, each of these ran the db2 read.
func TestEmittedScopeOutsideTheCheckedSetIsRefusedOnEveryEndpoint(t *testing.T) {
	metrics.Init(zerolog.Nop())
	const secretRead = "SELECT * FROM read_parquet('./data/db2/secret/**/*.parquet')"
	for _, tc := range []struct {
		name, method, path, body, sql string
	}{
		{"query", "POST", "/api/v1/query", `{"sql":"SELECT * FROM db1.cpu"}`, "SELECT * FROM db1.cpu"},
		{"arrow", "POST", "/api/v1/query/arrow", `{"sql":"SELECT * FROM db1.cpu"}`, "SELECT * FROM db1.cpu"},
		{"estimate", "POST", "/api/v1/query/estimate", `{"sql":"SELECT * FROM db1.cpu"}`, "SELECT * FROM db1.cpu"},
		{"measurement", "GET", "/api/v1/query/cpu?database=db1", "", "SELECT * FROM db1.cpu ORDER BY time DESC LIMIT 100 OFFSET 0"},
	} {
		for _, diverged := range []bool{true, false} {
			name := tc.name + "/consistent"
			if diverged {
				name = tc.name + "/diverged"
			}
			t.Run(name, func(t *testing.T) {
				h := newParityTestHandler()
				h.rbacManager = &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"db1": true}}
				if diverged {
					h.queryCache.SetWithMetadata(h.queryTransformCacheKey(tc.sql, ""), secretRead, []string{"db2\x00secret"})
				}
				app := fiber.New()
				app.Use(tokenMiddleware(1, "reader"))
				app.Use(recoverMiddleware.New())
				h.RegisterRoutes(app)

				req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				if tc.body != "" {
					req.Header.Set("Content-Type", "application/json")
				}
				resp, err := app.Test(req, -1)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(resp.Body)
				refused := resp.StatusCode == fiber.StatusForbidden && strings.Contains(string(body), "db2.secret")
				if diverged && !refused {
					t.Fatalf("status %d, want 403 naming db2.secret: %s", resp.StatusCode, body)
				}
				if !diverged && resp.StatusCode == fiber.StatusForbidden {
					t.Fatalf("an authorised db1 query was refused: %s", body)
				}
			})
		}
	}
}
