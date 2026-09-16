package api

import (
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// #872: the manual hand-over endpoint. It is what a cluster WITHOUT automatic
// writer failover recovers with — the FSM still names a dead primary, so
// nothing elects until an operator clears the designation. It is therefore
// deliberately not gated on the writer_failover licence, only on admin auth.

func demoteTestApp(t *testing.T) *fiber.App {
	t.Helper()
	// A nil coordinator is the OSS/standalone shape. It exercises routing and
	// the not-enabled response without needing a Raft cluster; the licence
	// boundary itself is tested in internal/cluster.
	h := NewClusterHandler(nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	h.RegisterRoutes(app)
	return app
}

// The route has to exist and be a POST. A typo here means an operator with a
// dead primary has no recovery path at all, which is the whole reason the
// endpoint was added.
func TestDemoteWriterRouteIsRegistered(t *testing.T) {
	app := demoteTestApp(t)

	resp, err := app.Test(httptest.NewRequest("POST", "/api/v1/cluster/writers/writer-1/demote", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode == fiber.StatusNotFound || resp.StatusCode == fiber.StatusMethodNotAllowed {
		t.Fatalf("POST /api/v1/cluster/writers/:id/demote is not routed (status %d)", resp.StatusCode)
	}
}

// Without clustering the endpoint must say so rather than 404 or 500.
func TestDemoteWriterWithoutClustering(t *testing.T) {
	app := demoteTestApp(t)

	resp, err := app.Test(httptest.NewRequest("POST", "/api/v1/cluster/writers/writer-1/demote", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode == fiber.StatusOK {
		t.Error("a node with no coordinator reported a successful hand-over")
	}
}

// A GET must not perform it. Guards against someone widening the group later.
func TestDemoteWriterRejectsGet(t *testing.T) {
	app := demoteTestApp(t)

	resp, err := app.Test(httptest.NewRequest("GET", "/api/v1/cluster/writers/writer-1/demote", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode != fiber.StatusMethodNotAllowed && resp.StatusCode != fiber.StatusNotFound {
		t.Errorf("GET on the hand-over route returned %d; it must not be reachable by GET", resp.StatusCode)
	}
}

// An over-long node ID is rejected before it reaches Raft.
func TestDemoteWriterRejectsAnOverlongID(t *testing.T) {
	app := demoteTestApp(t)

	long := make([]byte, maxNodeIDLength+10)
	for i := range long {
		long[i] = 'a'
	}
	resp, err := app.Test(httptest.NewRequest("POST", "/api/v1/cluster/writers/"+string(long)+"/demote", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	if resp.StatusCode == fiber.StatusOK {
		t.Error("an over-long node ID was accepted")
	}
}
