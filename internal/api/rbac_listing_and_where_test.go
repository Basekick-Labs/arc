package api

import (
	"context"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// authedDatabasesRig wraps setupAuthedDatabasesHandler with a cleanup func,
// since that helper returns the temp dir for the caller to remove.
func authedDatabasesRig(t *testing.T) (*DatabasesHandler, *fiber.App, *auth.AuthManager, func()) {
	t.Helper()
	h, app, am, tmpDir := setupAuthedDatabasesHandler(t)
	return h, app, am, func() { os.RemoveAll(tmpDir) }
}

// ---------------------------------------------------------------------------
// GET /api/v1/query/:measurement — the `where` fragment is a table position
// ---------------------------------------------------------------------------

// The route gated only the path parameters. The transform rewrites table
// references inside the user-supplied `where` fragment like any other table
// position, so a subquery there read a measurement nothing had authorized.
// No x-arc-database header is required.
func TestQueryMeasurement_WhereFragmentIsAuthorized(t *testing.T) {
	tests := []struct {
		name  string
		where string
		// allowed: the fragment names nothing beyond the path measurement
		allowed bool
	}{
		{name: "qualified subquery into another database", where: "1=(SELECT count(*) FROM db2.secrets)"},
		{name: "bare FROM subquery, no SELECT keyword", where: "1=(FROM db2.secrets)"},
		{name: "bare name resolves to default", where: "1=(FROM secrets)"},
		{name: "IN-list subquery", where: "x IN (SELECT y FROM db2.secrets)"},
		{name: "string-prefix oracle", where: "(SELECT min(v) FROM db2.secrets) LIKE 'T%'"},

		// Must still be allowed: no second measurement is referenced.
		{name: "ordinary predicate", where: "host = 'web1'", allowed: true},
		{name: "FROM inside EXTRACT is not a table", where: "EXTRACT(YEAR FROM time) = 2026", allowed: true},
		{name: "no where at all", where: "", allowed: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Granted db1 only. Any reference outside db1 must be denied.
			rbac := &mockRBACChecker{
				enabled:      true,
				allowedDBs:   map[string]bool{"db1": true},
				deniedReason: "no read permission",
			}
			app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

			target := "/api/v1/query/cpu?database=db1&limit=1"
			if tt.where != "" {
				target += "&where=" + url.QueryEscape(tt.where)
			}
			resp, err := app.Test(httptest.NewRequest("GET", target, nil), -1)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			if tt.allowed {
				if resp.StatusCode == fiber.StatusForbidden {
					t.Errorf("legitimate where %q was denied: %s", tt.where, body)
				}
				return
			}
			if resp.StatusCode != fiber.StatusForbidden {
				t.Errorf("where %q must be denied (it reads a measurement outside the granted database), got %d: %s",
					tt.where, resp.StatusCode, body)
			}
		})
	}
}

// The database that a BARE reference in `where` resolves to must be the same
// on both sides. This route takes its database from ?database= and hands the
// rewriter an empty header, so a bare name resolves to "default" — and must be
// CHECKED as "default", not as the x-arc-database header. Checking the header
// instead would authorize <header>/secrets while reading default/secrets:
// a bypass introduced by the fix itself.
func TestQueryMeasurement_BareWhereRefIgnoresHeader(t *testing.T) {
	// Granted db1 (the path database) and the header's database, but NOT
	// "default". If the check folded the header onto the bare ref it would
	// find a grant and allow the query, while the rewriter read default/secrets.
	rbac := &mockRBACChecker{
		enabled:      true,
		allowedDBs:   map[string]bool{"db1": true, "sensitive": true},
		deniedReason: "no read permission",
	}
	app := setupQueryRBACTest(t, rbac, tokenMiddleware(1, "test-token"))

	req := httptest.NewRequest("GET",
		"/api/v1/query/cpu?database=db1&limit=1&where="+url.QueryEscape("1=(FROM secrets)"), nil)
	req.Header.Set("x-arc-database", "sensitive")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != fiber.StatusForbidden {
		t.Errorf("a bare ref in `where` must be checked as default/secrets (what the rewriter reads), "+
			"not as sensitive/secrets (the header); got %d: %s", resp.StatusCode, body)
	}
}

// ---------------------------------------------------------------------------
// GET /api/v1/databases* — listing endpoints
// ---------------------------------------------------------------------------

// The three listing endpoints carried no auth middleware at all: only
// POST/DELETE got adminAuth. Any valid token — including a write-only one with
// no read permission — could enumerate every database and measurement name.
func TestDatabasesListing_RequiresReadPermission(t *testing.T) {
	_, app, am, cleanup := authedDatabasesRig(t)
	defer cleanup()

	readTok := mustCreateToken(t, am, "reader", "read")
	writeTok := mustCreateToken(t, am, "writer", "write")

	for _, path := range []string{
		"/api/v1/databases",
		"/api/v1/databases/testdb",
		"/api/v1/databases/testdb/measurements",
	} {
		t.Run("write-only token denied "+path, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			req.Header.Set("Authorization", "Bearer "+writeTok)
			resp, err := app.Test(req, -1)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != fiber.StatusForbidden {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("a write-only token must not enumerate names, got %d: %s", resp.StatusCode, body)
			}
		})

		t.Run("read token allowed "+path, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			req.Header.Set("Authorization", "Bearer "+readTok)
			resp, err := app.Test(req, -1)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode == fiber.StatusUnauthorized || resp.StatusCode == fiber.StatusForbidden {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("a read token must still list, got %d: %s", resp.StatusCode, body)
			}
		})
	}

	t.Run("no token still rejected", func(t *testing.T) {
		resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/databases", nil), -1)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != fiber.StatusUnauthorized {
			t.Errorf("expected 401 without a token, got %d", resp.StatusCode)
		}
	})
}

// With RBAC on, the listing endpoints must apply the same bar their query-path
// twins do: `*` for list-everything (SHOW DATABASES), the named database for
// the scoped routes (SHOW TABLES FROM db).
func TestDatabasesListing_RBACScoping(t *testing.T) {
	h, app, am, cleanup := authedDatabasesRig(t)
	defer cleanup()

	// Granted db1 only; "*" is denied (mockRBACChecker denies the wildcard
	// unless allowAll).
	h.SetRBACManager(&mockRBACChecker{
		enabled:      true,
		allowedDBs:   map[string]bool{"db1": true},
		deniedReason: "no read permission",
	})
	tok := mustCreateToken(t, am, "scoped", "read")

	get := func(path string) int {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if code := get("/api/v1/databases"); code != fiber.StatusForbidden {
		t.Errorf("listing every database needs a grant covering every database (the SHOW DATABASES bar), got %d", code)
	}
	if code := get("/api/v1/databases/db2"); code != fiber.StatusForbidden {
		t.Errorf("a database the caller has no grant for must be 403, got %d", code)
	}
	if code := get("/api/v1/databases/db2/measurements"); code != fiber.StatusForbidden {
		t.Errorf("measurements of an ungranted database must be 403, got %d", code)
	}
	if code := get("/api/v1/databases/db1"); code == fiber.StatusForbidden {
		t.Errorf("the granted database must still be readable, got %d", code)
	}
	if code := get("/api/v1/databases/db1/measurements"); code == fiber.StatusForbidden {
		t.Errorf("measurements of the granted database must still be readable, got %d", code)
	}
}

// A caller whose grants in db1 are per-MEASUREMENT must be able to enumerate
// inside db1 — it does hold a grant there — but must see only the names it
// can actually read. Gating the listing instead would deny it entirely, which
// is the shape that made measurement-level grants unusable; returning every
// name would disclose measurements it cannot read. Filtering is what makes a
// listing table-level.
//
// The gate and the filter ask deliberately different questions:
// CanAccessAnythingIn for "may you enumerate here", then a per-name batch
// check for "which of these may you read".
func TestDatabasesListing_PerMeasurementGrantSeesOnlyGrantedNames(t *testing.T) {
	h, app, am, cleanup := authedDatabasesRig(t)
	defer cleanup()

	// db1 is enumerable; within it only "cpu" is readable.
	h.SetRBACManager(&mockRBACChecker{
		enabled:            true,
		allowedDBs:         map[string]bool{"db1": true},
		deniedMeasurements: map[string]bool{"db1.secrets": true},
		deniedReason:       "no read permission",
	})
	tok := mustCreateToken(t, am, "measurement-scoped", "read")

	req := httptest.NewRequest("GET", "/api/v1/databases/db1/measurements", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	// The gate must not refuse it.
	if resp.StatusCode == fiber.StatusForbidden {
		t.Fatalf("a token holding a grant inside db1 must be able to enumerate it, got 403: %s", body)
	}
	// And an ungranted name must not appear. (The rig has no db1 on disk, so
	// this is a 404 with no names; the assertion is that if names ever are
	// returned, "secrets" is not among them.)
	if strings.Contains(string(body), "secrets") {
		t.Errorf("an ungranted measurement name leaked into the listing: %s", body)
	}
}

// An RBAC-only token — auth.PermissionsNone, "whose access comes solely from
// team/role grants" per that constant's own doc comment — must be able to
// reach these endpoints and be scoped by its grants.
//
// Before this change it could not: withReadAuth required the coarse "read"
// bit, which such a token deliberately lacks, so the one token class the
// per-database check exists for was refused by the middleware in front of it.
// The route now uses withResourceReadAuth, which consults RBAC.
//
// This uses the REAL *auth.RBACManager, not mockRBACChecker. That is only
// possible because enforcement no longer consults the license: the manager is
// built with LicenseClient nil, so IsRBACEnabled() is false throughout, and
// the grants still decide. If someone re-gates enforcement on the license,
// this test fails — which is the point.
func TestDatabasesListing_RBACOnlyTokenIsScopedNotRefused(t *testing.T) {
	_, app, am, cleanup := authedDatabasesRig(t)
	defer cleanup()
	ctx := context.Background()

	rm := auth.NewRBACManager(&auth.RBACManagerConfig{
		DB:            am.GetDB(),
		LicenseClient: nil,
		Logger:        zerolog.Nop(),
	})

	org, err := rm.CreateOrganization(ctx, &auth.CreateOrganizationRequest{Name: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	team, err := rm.CreateTeam(ctx, org.ID, &auth.CreateTeamRequest{Name: "tenant1"})
	if err != nil {
		t.Fatal(err)
	}
	// Database-level read on db1 only, with no measurement grants, so the
	// role-level permission applies to the whole database.
	if _, err := rm.CreateRole(ctx, team.ID, &auth.CreateRoleRequest{
		DatabasePattern: "db1", Permissions: []string{"read"},
	}); err != nil {
		t.Fatal(err)
	}

	raw, err := am.CreateToken(ctx, "rbac-only", "no coarse perms", auth.PermissionsNone, nil)
	if err != nil {
		t.Fatal(err)
	}
	info := am.VerifyToken(raw)
	if info == nil {
		t.Fatal("VerifyToken returned nil")
	}
	if len(info.Permissions) != 0 {
		t.Fatalf("expected no coarse permissions, got %v", info.Permissions)
	}
	if _, err := rm.AddTokenToTeam(ctx, info.ID, team.ID); err != nil {
		t.Fatal(err)
	}

	// The handler must be given the checker BEFORE its routes are registered,
	// so re-register onto a fresh app with the checker installed.
	h2 := NewDatabasesHandler(&mockLocalBackend{basePath: "./data"}, &config.DeleteConfig{Enabled: true}, am, zerolog.Nop())
	h2.SetRBACManager(rm)
	app2 := fiber.New()
	app2.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
	h2.RegisterRoutes(app2)
	_ = app

	get := func(path string) int {
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer "+raw)
		resp, err := app2.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	if code := get("/api/v1/databases/db1"); code == fiber.StatusForbidden || code == fiber.StatusUnauthorized {
		t.Errorf("an RBAC-only token granted db1 must reach the handler, got %d", code)
	}
	if code := get("/api/v1/databases/db2"); code != fiber.StatusForbidden {
		t.Errorf("an ungranted database must be 403 for an RBAC-only token, got %d", code)
	}
	if code := get("/api/v1/databases"); code != fiber.StatusForbidden {
		t.Errorf("listing every database needs a grant covering every database, got %d", code)
	}
}

// Compaction and retention are admin-domain operator features: compaction is
// cluster-wide work that cannot be configured per team or per database, and
// retention policies are configured by admins only. Both sets of read-only
// routes previously took any authenticated token, while returning
// {database, measurement, partition_path} rows naming every tenant — the same
// enumeration surface closed on the database listings.
func TestOperatorListings_RequireAdmin(t *testing.T) {
	_, _, am, cleanup := authedDatabasesRig(t)
	defer cleanup()

	app := fiber.New()
	app.Use(auth.NewMiddleware(auth.MiddlewareConfig{AuthManager: am}))
	NewCompactionHandler(nil, nil, nil, am, zerolog.Nop()).RegisterRoutes(app)

	readTok := mustCreateToken(t, am, "reader-ops", "read")
	adminTok := mustCreateToken(t, am, "admin-ops", "read,write,delete,admin")

	for _, path := range []string{
		"/api/v1/compaction/status",
		"/api/v1/compaction/stats",
		"/api/v1/compaction/candidates",
		"/api/v1/compaction/jobs",
		"/api/v1/compaction/history",
	} {
		t.Run("read token denied "+path, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			req.Header.Set("Authorization", "Bearer "+readTok)
			resp, err := app.Test(req, -1)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != fiber.StatusForbidden {
				body, _ := io.ReadAll(resp.Body)
				t.Errorf("a non-admin token must not read operator listings, got %d: %s", resp.StatusCode, body)
			}
		})
		t.Run("admin token reaches handler "+path, func(t *testing.T) {
			req := httptest.NewRequest("GET", path, nil)
			req.Header.Set("Authorization", "Bearer "+adminTok)
			resp, err := app.Test(req, -1)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode == fiber.StatusForbidden || resp.StatusCode == fiber.StatusUnauthorized {
				t.Errorf("an admin token must reach the handler, got %d", resp.StatusCode)
			}
		})
	}
}
