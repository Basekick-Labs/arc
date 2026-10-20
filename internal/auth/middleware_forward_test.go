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
