package api

import (
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func TestParseExpiresIn(t *testing.T) {
	const (
		invalid = "Invalid expires_in format. Use duration like '24h' or '7d'"
		tooLong = "expires_in is too long: at most 36500d (100 years); omit it for a token that does not expire"
	)
	for _, tc := range []struct {
		in      string
		want    time.Duration
		wantErr string
	}{
		{in: "24h", want: 24 * time.Hour},
		{in: "90m", want: 90 * time.Minute},
		{in: "7d", want: 7 * 24 * time.Hour},
		{in: "+7d", want: 7 * 24 * time.Hour},
		{in: "36500d", want: 36500 * 24 * time.Hour},
		{in: "876000h", want: 876000 * time.Hour},

		{in: "-24h", wantErr: "expires_in must be positive"},
		{in: "0s", wantErr: "expires_in must be positive"},
		{in: "0d", wantErr: "expires_in must be positive"},
		{in: "-7d", wantErr: "expires_in must be positive"},
		{in: "36501d", wantErr: tooLong},
		{in: "876001h", wantErr: tooLong},
		// Before, this day count overflowed time.Duration into an expiry 219
		// years out (2246).
		{in: "99999999d", wantErr: tooLong},
		{in: "-99999999d", wantErr: "expires_in must be positive"},
		// Unchecked, this count wrapped round to a positive duration of
		// about 100 years.
		{in: "-177004d", wantErr: "expires_in must be positive"},
		{in: "-9223372036854775808d", wantErr: "expires_in must be positive"},

		{in: "", wantErr: invalid},
		{in: "d", wantErr: invalid},
		{in: "7", wantErr: invalid},
		{in: "1.5d", wantErr: invalid},
		{in: "7days", wantErr: invalid},
		{in: "99999999999999999999d", wantErr: invalid},
	} {
		got, err := parseExpiresIn(tc.in)
		if tc.wantErr != "" {
			if err == nil || err.Error() != tc.wantErr {
				t.Errorf("parseExpiresIn(%q) = %v, %v; want error %q", tc.in, got, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("parseExpiresIn(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
}

func setupTokenExpiresInTest(t *testing.T) (*fiber.App, *auth.AuthManager) {
	t.Helper()
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), 5*time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewAuthManager: %v", err)
	}
	t.Cleanup(func() { am.Close() })

	// RequireAdmin is not mounted: these tests exercise request validation.
	h := NewAuthHandler(am, zerolog.Nop())
	app := fiber.New()
	app.Post("/tokens", h.createToken)
	app.Patch("/tokens/:id", h.updateToken)
	return app, am
}

func tokenNamed(t *testing.T, am *auth.AuthManager, name string) *auth.TokenInfo {
	t.Helper()
	tokens, err := am.ListTokens()
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	for i := range tokens {
		if tokens[i].Name == name {
			return &tokens[i]
		}
	}
	return nil
}

// TestTokenRoutesRefuseAnExpiryNotInTheFuture: a create or update whose
// expires_in is zero, negative or overflows is a 400 and writes nothing. Before,
// each was a 201 or 200 that stored an expiry in the past (or, for an
// overflowing day count, an arbitrary one).
func TestTokenRoutesRefuseAnExpiryNotInTheFuture(t *testing.T) {
	app, am := setupTokenExpiresInTest(t)

	for _, in := range []string{"-24h", "0d", "-7d", "99999999d", "-177004d"} {
		body := fmt.Sprintf(`{"name":"t%s","expires_in":%q}`, in, in)
		if status, msg := doRBACRequest(t, app, "POST", "/tokens", body); status != http.StatusBadRequest {
			t.Errorf("create with expires_in %q: %d %q, want 400", in, status, msg)
		}
		if tok := tokenNamed(t, am, "t"+in); tok != nil {
			t.Errorf("create with expires_in %q stored a token expiring %v", in, tok.ExpiresAt)
		}
	}

	if status, msg := doRBACRequest(t, app, "POST", "/tokens", `{"name":"svc","expires_in":"7d"}`); status != http.StatusCreated {
		t.Fatalf("create with expires_in 7d: %d %q", status, msg)
	}
	svc := tokenNamed(t, am, "svc")
	if svc == nil || svc.ExpiresAt == nil || time.Until(*svc.ExpiresAt) < 6*24*time.Hour {
		t.Fatalf("create with expires_in 7d stored %+v", svc)
	}
	stored := *svc.ExpiresAt

	path := fmt.Sprintf("/tokens/%d", svc.ID)
	for _, in := range []string{"-24h", "0s", "0d", "99999999d", "-177004d"} {
		body := fmt.Sprintf(`{"expires_in":%q}`, in)
		if status, msg := doRBACRequest(t, app, "PATCH", path, body); status != http.StatusBadRequest {
			t.Errorf("update with expires_in %q: %d %q, want 400", in, status, msg)
		}
	}
	if got := tokenNamed(t, am, "svc").ExpiresAt; got == nil || !got.Equal(stored) {
		t.Errorf("a refused update changed the expiry from %v to %v", stored, got)
	}

	if status, msg := doRBACRequest(t, app, "PATCH", path, `{"expires_in":"30d"}`); status != http.StatusOK {
		t.Fatalf("update with expires_in 30d: %d %q", status, msg)
	}
	if got := tokenNamed(t, am, "svc").ExpiresAt; got == nil || time.Until(*got) < 29*24*time.Hour {
		t.Errorf("update with expires_in 30d stored %v", got)
	}
}

// TestTokenUpdateRefusesAnEmptyName: create requires a name, so a rename must
// not clear it.
func TestTokenUpdateRefusesAnEmptyName(t *testing.T) {
	app, am := setupTokenExpiresInTest(t)
	if status, msg := doRBACRequest(t, app, "POST", "/tokens", `{"name":"svc"}`); status != http.StatusCreated {
		t.Fatalf("create: %d %q", status, msg)
	}
	path := fmt.Sprintf("/tokens/%d", tokenNamed(t, am, "svc").ID)

	status, msg := doRBACRequest(t, app, "PATCH", path, `{"name":""}`)
	if status != http.StatusBadRequest || msg != "Token name cannot be empty" {
		t.Errorf("rename to an empty name: %d %q, want 400 %q", status, msg, "Token name cannot be empty")
	}
	if tokenNamed(t, am, "svc") == nil {
		t.Error("the refused rename changed the token's name")
	}

	if status, msg := doRBACRequest(t, app, "PATCH", path, `{"name":"renamed"}`); status != http.StatusOK {
		t.Fatalf("rename: %d %q", status, msg)
	}
	if tokenNamed(t, am, "renamed") == nil {
		t.Error("the rename was not stored")
	}
}
