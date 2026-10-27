package auth

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A forwarded request is held to the middleware's required permission like
// a presented token.
func TestForwardedIdentityHonoursRequiredPermission(t *testing.T) {
	am, err := NewAuthManager(filepath.Join(t.TempDir(), "a.db"), time.Minute, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer am.Close()
	tok, err := am.CreateToken(context.Background(), "w", "", "write", nil)
	if err != nil {
		t.Fatal(err)
	}
	info := am.VerifyToken(tok)
	for perm, want := range map[string]int{"admin": 403, "write": 204} {
		app := fiber.New()
		app.Use(NewMiddleware(MiddlewareConfig{AuthManager: am, RequiredPermission: perm,
			ForwardIdentity: func(*fiber.Ctx) (bool, int64, string, error) { return true, info.ID, info.Fingerprint, nil }}))
		app.Post("/x", func(c *fiber.Ctx) error { return c.SendStatus(204) })
		resp, err := app.Test(httptest.NewRequest("POST", "/x", nil), -1)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != want {
			t.Errorf("required %s: %d, want %d", perm, resp.StatusCode, want)
		}
	}
}

// The by-ID cache of forwarded tokens is cleared when a token changes and
// expires like the token cache.
func TestTokenForForward_CacheFollowsTokenChanges(t *testing.T) {
	am, err := NewAuthManager(filepath.Join(t.TempDir(), "a.db"), 20*time.Millisecond, 10, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer am.Close()
	tok, err := am.CreateToken(context.Background(), "w", "", "write", nil)
	if err != nil {
		t.Fatal(err)
	}
	info := am.VerifyToken(tok)
	if am.TokenForForward(info.ID, info.Fingerprint) == nil {
		t.Fatal("valid token refused")
	}
	if am.TokenForForward(info.ID, "other") != nil {
		t.Fatal("another fingerprint accepted")
	}
	// A change made behind the cache is seen once the entry expires.
	if _, err := am.GetDB().Exec(`UPDATE api_tokens SET enabled = 0 WHERE id = ?`, info.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(40 * time.Millisecond)
	if am.TokenForForward(info.ID, info.Fingerprint) != nil {
		t.Fatal("disabled token still served from the cache")
	}
	// A revocation through the manager is seen at once.
	tok2, _ := am.CreateToken(context.Background(), "w2", "", "write", nil)
	info2 := am.VerifyToken(tok2)
	if am.TokenForForward(info2.ID, info2.Fingerprint) == nil {
		t.Fatal("valid token refused")
	}
	if err := am.RevokeToken(context.Background(), info2.ID); err != nil {
		t.Fatal(err)
	}
	if am.TokenForForward(info2.ID, info2.Fingerprint) != nil {
		t.Fatal("revoked token still served from the cache")
	}
}
