package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// Pins the status contract of the token routes: a duplicate name is a 409, not
// a 500, and every route that names a token by ID answers 404 for an ID with no
// token, on a standalone node and in a cluster alike.
//
// RequireAdmin is not mounted: these tests exercise the handlers' error-to-status
// mapping, not the admin gate in front of them.
func setupTokenStatusTest(t *testing.T) (*fiber.App, *auth.AuthManager) {
	t.Helper()

	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), 5*time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	t.Cleanup(func() {
		am.SetRaftProposer(nil)
		am.Close()
	})

	h := NewAuthHandler(am, zerolog.Nop())
	app := fiber.New()
	app.Post("/tokens", h.createToken)
	app.Patch("/tokens/:id", h.updateToken)
	app.Delete("/tokens/:id", h.deleteToken)
	app.Post("/tokens/:id/rotate", h.rotateToken)
	app.Post("/tokens/:id/revoke", h.revokeToken)
	app.Get("/tokens/:id", h.getToken)
	return app, am
}

// droppingTokenProposer stands in for the cluster FSM, which applies an update,
// revoke, rotate or delete of an unknown token ID as a no-op and reports
// success. It counts proposals so a test can require that none was made.
type droppingTokenProposer struct {
	proposals atomic.Int64
}

func (p *droppingTokenProposer) Propose(context.Context, uint8, []byte, time.Duration) error {
	p.proposals.Add(1)
	return nil
}

func (p *droppingTokenProposer) IsLeader() bool { return true }

// tokenIDNamed returns the ID of the token with this name.
func tokenIDNamed(t *testing.T, am *auth.AuthManager, name string) int64 {
	t.Helper()
	tokens, err := am.ListTokens()
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	for _, tk := range tokens {
		if tk.Name == name {
			return tk.ID
		}
	}
	t.Fatalf("no token named %q", name)
	return 0
}

func TestTokenRoutesDuplicateNameIsConflict(t *testing.T) {
	app, am := setupTokenStatusTest(t)

	for _, body := range []string{`{"name":"alpha"}`, `{"name":"bravo"}`} {
		if status, msg := doRBACRequest(t, app, "POST", "/tokens", body); status != http.StatusCreated {
			t.Fatalf("seed %s: status %d (%s)", body, status, msg)
		}
	}

	status, msg := doRBACRequest(t, app, "POST", "/tokens", `{"name":"alpha"}`)
	if status != http.StatusConflict || msg != "token with name 'alpha' already exists" {
		t.Errorf("duplicate create: %d %q, want 409 %q", status, msg, "token with name 'alpha' already exists")
	}

	bravo := fmt.Sprintf("/tokens/%d", tokenIDNamed(t, am, "bravo"))
	status, msg = doRBACRequest(t, app, "PATCH", bravo, `{"name":"alpha"}`)
	if status != http.StatusConflict || msg != "token with name 'alpha' already exists" {
		t.Errorf("rename to a taken name: %d %q, want 409 %q", status, msg, "token with name 'alpha' already exists")
	}
	tokenIDNamed(t, am, "bravo") // the refused rename left the name alone
}

func TestTokenRoutesUnknownIDIsNotFound(t *testing.T) {
	for _, clustered := range []bool{false, true} {
		t.Run(fmt.Sprintf("clustered=%v", clustered), func(t *testing.T) {
			app, am := setupTokenStatusTest(t)
			prop := &droppingTokenProposer{}
			if clustered {
				am.SetRaftProposer(prop)
			}

			for _, tc := range []struct{ method, path, body string }{
				{"PATCH", "/tokens/99", `{"description":"x"}`},
				{"PATCH", "/tokens/99", `{}`},
				{"DELETE", "/tokens/99", ""},
				{"POST", "/tokens/99/rotate", ""},
				{"POST", "/tokens/99/revoke", ""},
			} {
				status, msg := doRBACRequest(t, app, tc.method, tc.path, tc.body)
				if status != http.StatusNotFound || msg != "Token not found" {
					t.Errorf("%s %s %s: %d %q, want 404 %q", tc.method, tc.path, tc.body, status, msg, "Token not found")
				}
			}
			if n := prop.proposals.Load(); n != 0 {
				t.Errorf("%d proposals for an unknown ID, want 0", n)
			}
		})
	}
}

// TestTokenRoutesDeleteTwice: the first DELETE removes the token, the second
// finds nothing.
func TestTokenRoutesDeleteTwice(t *testing.T) {
	app, am := setupTokenStatusTest(t)
	if status, msg := doRBACRequest(t, app, "POST", "/tokens", `{"name":"svc"}`); status != http.StatusCreated {
		t.Fatalf("create: status %d (%s)", status, msg)
	}
	path := fmt.Sprintf("/tokens/%d", tokenIDNamed(t, am, "svc"))
	if status, msg := doRBACRequest(t, app, "DELETE", path, ""); status != http.StatusOK {
		t.Fatalf("first delete: %d %q, want 200", status, msg)
	}
	if status, msg := doRBACRequest(t, app, "DELETE", path, ""); status != http.StatusNotFound || msg != "Token not found" {
		t.Errorf("second delete: %d %q, want 404", status, msg)
	}
}

// TestTokenRoutesRotateRevokedIsConflict: rotating a revoked token answered
// 200 with a secret that could never authenticate. It is a 409 now, and the
// token is still there, revoked (#1625).
func TestTokenRoutesRotateRevokedIsConflict(t *testing.T) {
	app, am := setupTokenStatusTest(t)
	if status, msg := doRBACRequest(t, app, "POST", "/tokens", `{"name":"svc"}`); status != http.StatusCreated {
		t.Fatalf("create: status %d (%s)", status, msg)
	}
	path := fmt.Sprintf("/tokens/%d", tokenIDNamed(t, am, "svc"))
	if status, msg := doRBACRequest(t, app, "POST", path+"/revoke", ""); status != http.StatusOK {
		t.Fatalf("revoke: %d %q, want 200", status, msg)
	}

	status, msg := doRBACRequest(t, app, "POST", path+"/rotate", "")
	if status != http.StatusConflict || msg != tokenRevokedRotateMessage {
		t.Errorf("rotate of a revoked token: %d %q, want 409 %q", status, msg, tokenRevokedRotateMessage)
	}

	req := httptest.NewRequest("GET", path, nil)
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	var got struct {
		Token struct {
			Enabled *bool `json:"enabled"`
		} `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode GET %s: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK || got.Token.Enabled == nil || *got.Token.Enabled {
		t.Errorf("GET after the refused rotate: status %d, enabled %v, want 200 and false", resp.StatusCode, got.Token.Enabled)
	}

	// An enabled token still rotates.
	if status, msg := doRBACRequest(t, app, "POST", "/tokens", `{"name":"live"}`); status != http.StatusCreated {
		t.Fatalf("create: status %d (%s)", status, msg)
	}
	live := fmt.Sprintf("/tokens/%d/rotate", tokenIDNamed(t, am, "live"))
	if status, msg := doRBACRequest(t, app, "POST", live, ""); status != http.StatusOK {
		t.Errorf("rotate of an enabled token: %d %q, want 200", status, msg)
	}
}
