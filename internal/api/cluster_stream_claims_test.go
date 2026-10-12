package api

import (
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
)

// #1207: the stream takeover view is routed as a GET, and says so when the
// node is not clustered.
func TestStreamClaimsRouteWithoutClustering(t *testing.T) {
	app := demoteTestApp(t)
	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/cluster/stream-claims", nil))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != fiber.StatusConflict {
		t.Fatalf("status %d, want 409", resp.StatusCode)
	}
}
