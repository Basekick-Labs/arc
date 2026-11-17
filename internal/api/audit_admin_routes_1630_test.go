package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/audit"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1630: the token and RBAC admin routes, behind the auth middleware and the
// audit middleware in the order cmd/arc/main.go installs them, produce audit
// events. Until #1630 main registered these routes before audit, and none
// was ever recorded. (The order in main itself is checked at startup by
// checkAuditCoverage, unit-tested in cmd/arc.)
func TestTokenAndRBACAdminRoutesAreAudited(t *testing.T) {
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), 5*time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	ctx := context.Background()
	admin, err := am.CreateToken(ctx, "admin-1630", "", "read,write,delete,admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := am.CreateToken(ctx, "reader-1630", "", "read", nil)
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	al, err := audit.NewLogger(&audit.LoggerConfig{DB: db, Config: &config.AuditLogConfig{Enabled: true, RetentionDays: 90}, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	al.Start()

	app := fiber.New()
	mc := auth.DefaultMiddlewareConfig()
	mc.AuthManager = am
	app.Use(auth.NewMiddleware(mc))
	app.Use(audit.Middleware(al, false))
	h := NewAuthHandler(am, zerolog.Nop())
	rm := auth.NewRBACManager(&auth.RBACManagerConfig{DB: am.GetDB(), Logger: zerolog.Nop()})
	h.SetRBACManager(rm)
	h.RegisterRoutes(app)
	h.RegisterTokenMembershipRoutes(app)
	NewRBACHandler(am, rm, zerolog.Nop()).RegisterRoutes(app)

	do := func(method, path, token, body string) (int, map[string]any) {
		t.Helper()
		var r io.Reader
		if body != "" {
			r = strings.NewReader(body)
		}
		req := httptest.NewRequest(method, path, r)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out := map[string]any{}
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}

	status, body := do("POST", "/api/v1/auth/tokens", admin, `{"name":"svc-1630","permissions":["read"]}`)
	created, _ := body["token"].(string)
	if status != 201 || created == "" {
		t.Fatalf("create: %d %v", status, body)
	}
	id := int64(0)
	if tokens, _ := am.ListTokens(); true {
		for _, tk := range tokens {
			if tk.Name == "svc-1630" {
				id = tk.ID
			}
		}
	}
	tok := fmt.Sprintf("/api/v1/auth/tokens/%d", id)
	status, body = do("POST", tok+"/rotate", admin, "")
	rotated, _ := body["new_token"].(string)
	if status != 200 || rotated == "" {
		t.Fatalf("rotate: %d %v", status, body)
	}
	if status, body = do("PATCH", tok, admin, `{"description":"x"}`); status != 200 {
		t.Fatalf("patch: %d %v", status, body)
	}
	if status, body = do("POST", tok+"/revoke", admin, ""); status != 200 {
		t.Fatalf("revoke: %d %v", status, body)
	}
	if status, body = do("DELETE", tok, admin, ""); status != 200 {
		t.Fatalf("delete: %d %v", status, body)
	}
	do("POST", "/api/v1/auth/tokens", reader, `{"name":"nope","permissions":["read"]}`) // 403 from RequireAdmin
	do("POST", "/api/v1/auth/tokens", "", `{"name":"anon","permissions":["read"]}`)     // 401 from the auth middleware
	do("POST", "/api/v1/rbac/organizations", admin, `{"name":"org-1630"}`)              // reaches the RBAC routes (403 without the licence)
	do("GET", "/api/v1/auth/tokens", admin, "")                                         // a read: not recorded without include_reads

	al.Stop() // drains the writer
	entries, err := al.Query(ctx, &audit.QueryFilter{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, e := range entries {
		got[e.EventType+" "+e.Method+" "+e.Path+" "+e.Actor]++
		for _, field := range []string{e.Path, e.Actor, e.Detail, e.Database, e.Measurement, e.UserAgent} {
			if (created != "" && strings.Contains(field, created)) || (rotated != "" && strings.Contains(field, rotated)) || strings.Contains(field, admin) {
				t.Errorf("event %s records a token secret: %q", e.EventType, field)
			}
		}
	}
	for _, want := range []string{
		"token.created POST /api/v1/auth/tokens admin-1630",
		"token.rotated POST " + tok + "/rotate admin-1630",
		"token.updated PATCH " + tok + " admin-1630",
		"token.revoked POST " + tok + "/revoke admin-1630",
		"token.deleted DELETE " + tok + " admin-1630",
		"auth.failed POST /api/v1/auth/tokens reader-1630",
		"auth.failed POST /api/v1/rbac/organizations admin-1630",
	} {
		if got[want] != 1 {
			t.Errorf("event %q recorded %d times, want 1 (all: %v)", want, got[want], got)
		}
	}
	// 401s from the auth middleware itself do not reach audit (it runs
	// after auth), and reads are skipped without include_reads.
	if len(entries) != 7 {
		t.Errorf("%d events, want 7: %v", len(entries), got)
	}
}
