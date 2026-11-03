package api

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
)

// The #764 shapes: a user-supplied reader or replacement scan. Each is refused
// by ValidateSQLRequest, which every query endpoint runs before the transform,
// so the transform never sees one.
func TestValidateSQLRequestRejectsUserStoragePaths(t *testing.T) {
	const root = "./data/db2/**/*.parquet"
	for _, tc := range []struct{ name, sql string }{
		{"read_parquet", "SELECT * FROM read_parquet('" + root + "')"},
		{"parquet_scan", "SELECT * FROM parquet_scan('" + root + "')"},
		{"quoted parquet_scan", `SELECT * FROM "parquet_scan"('` + root + `')`},
		{"glob", "SELECT * FROM glob('./data/**')"},
		{"read_text", "SELECT * FROM read_text('/etc/passwd')"},
		{"nested reader", "WITH t AS (SELECT * FROM read_parquet('" + root + "')) SELECT * FROM t"},
		{"cross join replacement scan", "SELECT * FROM db1.cpu, './data/db2/x.parquet' b"},
		{"nested reader with authorised table", "SELECT * FROM db1.cpu WHERE x IN (SELECT y FROM read_parquet('" + root + "'))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateSQLRequest(tc.sql); err == nil {
				t.Fatalf("ValidateSQLRequest accepted a user-supplied storage path in %q", tc.sql)
			}
		})
	}
}

func TestGetTransformedSQLAllowsPathShapedValue(t *testing.T) {
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

func TestGetTransformedSQLWithRefsPreservesReferencesOnCacheHit(t *testing.T) {
	h := newParityTestHandler()
	query := "SELECT * FROM db1.cpu JOIN db2.mem ON db1.cpu.host = db2.mem.host"
	firstSQL, firstCached, firstRefs, err := h.getTransformedSQLWithRefs(context.Background(), query, "")
	if err != nil || firstCached {
		t.Fatalf("first transform: cached=%v err=%v", firstCached, err)
	}
	want := []string{"db1\x00cpu", "db2\x00mem"}
	if got := encodeStoragePathReferences(firstRefs); !equalStorageRefStrings(got, want) {
		t.Fatalf("first references = %q, want %q", got, want)
	}
	secondSQL, secondCached, secondRefs, err := h.getTransformedSQLWithRefs(context.Background(), query, "")
	if err != nil || !secondCached || secondSQL != firstSQL {
		t.Fatalf("cached transform: cached=%v err=%v; SQL equal=%v", secondCached, err, secondSQL == firstSQL)
	}
	if got := encodeStoragePathReferences(secondRefs); !equalStorageRefStrings(got, want) {
		t.Fatalf("cached references = %q, want %q", got, want)
	}
}

func TestCheckEmittedQueryPermissionsChecksUnextractedScopes(t *testing.T) {
	rbac := &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"db1": true}}
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
		err := h.checkEmittedQueryPermissions(c, "read", []TableReference{{Database: "db2", Measurement: "secret"}})
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

func TestGetTransformedSQLRejectsCachedPathsWithoutProvenance(t *testing.T) {
	const query = "SELECT * FROM db1.cpu"
	for _, metadata := range [][]string{nil, {"invalid"}, {"db1\x00"}} {
		h := newParityTestHandler()
		cacheKey := h.queryTransformCacheKey(query, "")
		h.queryCache.SetWithMetadata(cacheKey, "SELECT * FROM read_parquet('./data/db2/secret/**/*.parquet')", metadata)
		if _, cached, _, err := h.getTransformedSQLWithRefs(context.Background(), query, ""); err == nil || !cached {
			t.Fatalf("metadata %q: cached=%v err=%v, want rejected cache hit", metadata, cached, err)
		}
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

// A transform that reads storage with nothing recorded is refused, as Arc's
// own inconsistency (500), on every path that returns one; one that reads
// nothing, or recorded what it reads, is not.
func TestCheckTransformProvenance(t *testing.T) {
	read := "SELECT * FROM read_parquet('./data/db1/cpu/**/*.parquet')"
	if err := checkTransformProvenance(read, nil); !errors.Is(err, errTransformProvenance) || transformErrorStatus(err) != fiber.StatusInternalServerError {
		t.Fatalf("unrecorded read: err %v, status %d; want errTransformProvenance, 500", err, transformErrorStatus(err))
	}
	if err := checkTransformProvenance(read, []TableReference{{Database: "db1", Measurement: "cpu"}}); err != nil {
		t.Fatalf("recorded read refused: %v", err)
	}
	if err := checkTransformProvenance("SELECT 1", nil); err != nil {
		t.Fatalf("a query reading nothing refused: %v", err)
	}
}
