package api

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
)

// These requests bypass the HTTP validator deliberately. The transformer is
// also the boundary before query dispatch, so it must never pass user supplied
// filesystem readers or replacement scans through as if Arc had emitted them.
func TestGetTransformedSQLRejectsUserStoragePaths(t *testing.T) {
	h := newParityTestHandler()
	const root = "./data/db2/**/*.parquet"

	tests := []struct {
		name string
		sql  string
	}{
		{name: "read_parquet", sql: "SELECT * FROM read_parquet('" + root + "')"},
		{name: "parquet_scan", sql: "SELECT * FROM parquet_scan('" + root + "')"},
		{name: "quoted parquet_scan", sql: `SELECT * FROM "parquet_scan"('` + root + `')`},
		{name: "glob", sql: "SELECT * FROM glob('./data/**')"},
		{name: "read_text", sql: "SELECT * FROM read_text('/etc/passwd')"},
		{name: "nested reader", sql: "WITH t AS (SELECT * FROM read_parquet('" + root + "')) SELECT * FROM t"},
		{name: "cross join replacement scan", sql: "SELECT * FROM db1.cpu, './data/db2/x.parquet' b"},
		{name: "nested reader with authorised table", sql: "SELECT * FROM db1.cpu WHERE x IN (SELECT y FROM read_parquet('" + root + "'))"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := h.getTransformedSQL(context.Background(), tt.sql, ""); err == nil {
				t.Fatalf("getTransformedSQL accepted a user-supplied storage path in %q", tt.sql)
			}
		})
	}
}

func TestGetTransformedSQLAllowsStoragePathAsValue(t *testing.T) {
	h := newParityTestHandler()
	query := "SELECT './data/db2/x.parquet' AS path"
	got, _, err := h.getTransformedSQL(context.Background(), query, "")
	if err != nil {
		t.Fatalf("getTransformedSQL rejected a path-shaped value literal: %v", err)
	}
	if got != query {
		t.Fatalf("transformed SQL = %q, want original query %q", got, query)
	}
}

func TestGetTransformedSQLWithRefsPreservesResolvedReferencesOnCacheHit(t *testing.T) {
	h := newParityTestHandler()
	query := "SELECT * FROM db1.cpu JOIN db2.mem ON db1.cpu.host = db2.mem.host"

	firstSQL, firstCached, firstRefs, err := h.getTransformedSQLWithRefs(context.Background(), query, "")
	if err != nil {
		t.Fatalf("first transform: %v", err)
	}
	if firstCached {
		t.Fatal("first transform unexpectedly hit the cache")
	}
	if got, want := encodeStoragePathReferences(firstRefs), []string{"db1\x00cpu", "db2\x00mem"}; !equalStorageRefStrings(got, want) {
		t.Fatalf("first resolved references = %q, want %q", got, want)
	}

	secondSQL, secondCached, secondRefs, err := h.getTransformedSQLWithRefs(context.Background(), query, "")
	if err != nil {
		t.Fatalf("cached transform: %v", err)
	}
	if !secondCached {
		t.Fatal("second transform did not use the cache")
	}
	if secondSQL != firstSQL {
		t.Fatalf("cached SQL differs from first transform:\n first: %s\nsecond: %s", firstSQL, secondSQL)
	}
	if got, want := encodeStoragePathReferences(secondRefs), []string{"db1\x00cpu", "db2\x00mem"}; !equalStorageRefStrings(got, want) {
		t.Fatalf("cached resolved references = %q, want %q", got, want)
	}
}

func TestCheckEmittedQueryPermissionsChecksReferencesMissingFromExtractor(t *testing.T) {
	rbac := &mockRBACChecker{
		enabled:    true,
		allowedDBs: map[string]bool{"db1": true},
	}
	h := &QueryHandler{rbacManager: rbac}
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		c.Locals("token_info", &auth.TokenInfo{ID: 1})
		return c.Next()
	})
	app.Get("/", func(c *fiber.Ctx) error {
		if err := h.checkQueryPermissionsForDefaultDB(c, "SELECT * FROM db1.cpu", "read", ""); err != nil {
			return c.SendStatus(fiber.StatusForbidden)
		}
		err := h.checkEmittedQueryPermissions(c, "read", []TableReference{
			{Database: "db2", Measurement: "secret"},
		})
		if err == nil {
			return c.SendStatus(fiber.StatusOK)
		}
		return c.SendStatus(fiber.StatusForbidden)
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("status = %d, want %d", resp.StatusCode, fiber.StatusForbidden)
	}
}

func equalStorageRefStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
