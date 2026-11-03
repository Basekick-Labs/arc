package api

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"

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
		// lookups the endpoint's existing checks make; the emitted check
		// adds none when the transform reads what they saw.
		lookups int64
	}{
		{"query", "POST", "/api/v1/query", `{"sql":"SELECT * FROM db1.cpu"}`, "SELECT * FROM db1.cpu", 1},
		{"arrow", "POST", "/api/v1/query/arrow", `{"sql":"SELECT * FROM db1.cpu"}`, "SELECT * FROM db1.cpu", 1},
		{"estimate", "POST", "/api/v1/query/estimate", `{"sql":"SELECT * FROM db1.cpu"}`, "SELECT * FROM db1.cpu", 1},
		// checkMeasurementPermission, then the SQL check.
		{"measurement", "GET", "/api/v1/query/cpu?database=db1", "", "SELECT * FROM db1.cpu ORDER BY time DESC LIMIT 100 OFFSET 0", 2},
	} {
		for _, diverged := range []bool{true, false} {
			name := tc.name + "/consistent"
			if diverged {
				name = tc.name + "/diverged"
			}
			t.Run(name, func(t *testing.T) {
				h := newParityTestHandler()
				rbac := &countingRBAC{mockRBACChecker: mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"db1": true}}}
				h.rbacManager = rbac
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
				// When the transform reads what the source check saw, the
				// emitted check makes no RBAC call of its own.
				if !diverged && rbac.calls.Load() != tc.lookups {
					t.Fatalf("%d RBAC lookups for a consistent query, want the existing checks' %d", rbac.calls.Load(), tc.lookups)
				}
			})
		}
	}
}

// countingRBAC counts permission lookups, each batch counted once.
type countingRBAC struct {
	mockRBACChecker
	calls atomic.Int64
}

func (m *countingRBAC) CheckPermission(req *auth.PermissionCheckRequest) *auth.PermissionCheckResult {
	m.calls.Add(1)
	return m.mockRBACChecker.CheckPermission(req)
}

func (m *countingRBAC) CheckPermissionsBatch(reqs []*auth.PermissionCheckRequest) []*auth.PermissionCheckResult {
	m.calls.Add(1)
	return m.mockRBACChecker.CheckPermissionsBatch(reqs)
}

// measurementRBAC grants read on exactly the listed db.measurement pairs, the
// shape of a tenant role scoped to measurements.
type measurementRBAC struct {
	mockRBACChecker
	granted map[string]bool
}

func (m *measurementRBAC) CheckPermissionsBatch(reqs []*auth.PermissionCheckRequest) []*auth.PermissionCheckResult {
	out := make([]*auth.PermissionCheckResult, len(reqs))
	for i, r := range reqs {
		out[i] = &auth.PermissionCheckResult{Allowed: m.granted[r.Database+"."+r.Measurement]}
	}
	return out
}

// A measurement no tier holds a file of is read as empty (#1364). What that
// read records is the measurement's own scope, never the placeholder glob's,
// so a token granted only that measurement gets its empty result, not a 403.
func TestNoTierReadRecordsTheMeasurementScope(t *testing.T) {
	for _, hotWasSource := range []bool{false, true} {
		h := newParityTestHandler()
		ctx, collected := withStoragePathFailure(context.Background())
		if expr := h.noTierRead(ctx, "sch", "stale", "FROM", "", hotWasSource); expr == "" {
			t.Fatalf("hotWasSource=%v: no read built: %v", hotWasSource, collected.err)
		}
		refs := storagePathReferences(collected)
		if len(refs) != 1 || refs[0] != (TableReference{Database: "sch", Measurement: "stale"}) {
			t.Fatalf("hotWasSource=%v: recorded %v, want only sch.stale", hotWasSource, refs)
		}

		h.rbacManager = &measurementRBAC{mockRBACChecker: mockRBACChecker{enabled: true}, granted: map[string]bool{"sch.stale": true}}
		app := fiber.New()
		app.Use(tokenMiddleware(1, "tenant"))
		app.Get("/", func(c *fiber.Ctx) error {
			if err := h.checkEmittedQueryPermissions(c, "read", refs); err != nil {
				return c.Status(fiber.StatusForbidden).SendString(err.Error())
			}
			return c.SendStatus(fiber.StatusOK)
		})
		resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != fiber.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("hotWasSource=%v: a token granted sch.stale was refused its empty read: %s", hotWasSource, body)
		}
	}
}
