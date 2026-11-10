package api

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #1552: resetting the cold mark's seed changes what every reader trusts, so
// it needs an admin token; without clustering it says so.
func TestColdMarkResetRequiresAdmin(t *testing.T) {
	am, err := auth.NewAuthManager(t.TempDir()+"/auth.db", time.Second, 100, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer am.Close()
	app := fiber.New()
	NewClusterHandler(nil, am, nil, zerolog.Nop()).RegisterRoutes(app)
	resp, err := app.Test(httptest.NewRequest("POST", "/api/v1/cluster/cold-mark/reset", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusUnauthorized {
		t.Fatalf("unauthenticated reset returned %d, want 401", resp.StatusCode)
	}
}

func TestColdMarkResetWithoutClustering(t *testing.T) {
	app := fiber.New()
	NewClusterHandler(nil, nil, nil, zerolog.Nop()).RegisterRoutes(app)
	resp, err := app.Test(httptest.NewRequest("POST", "/api/v1/cluster/cold-mark/reset", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusConflict {
		t.Fatalf("reset without clustering returned %d, want 409", resp.StatusCode)
	}
}
