package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

type spokeRig struct {
	app       *fiber.App
	agent     *edgesync.Agent
	backend   storage.Backend
	transport *edgesync.MemoryTransport
}

func newSpokeRig(t *testing.T) *spokeRig {
	t.Helper()

	dir, err := os.MkdirTemp("", "spoke-api-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	db, err := sql.Open("sqlite3", dir+"/ledger.db")
	if err != nil {
		t.Fatalf("ledger db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	ledger, err := edgesync.NewLedger(db, zerolog.Nop())
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	transport := edgesync.NewMemoryTransport()

	agent, err := edgesync.NewAgent(edgesync.AgentConfig{
		Ledger: ledger, Transport: transport, Backend: backend,
		HubID: "ground-station", SpokeID: "rocket-01", Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("agent: %v", err)
	}

	h, err := NewEdgeSyncSpokeHandler(agent, nil, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("handler: %v", err)
	}

	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	return &spokeRig{app: app, agent: agent, backend: backend, transport: transport}
}

func (r *spokeRig) do(t *testing.T, method, path string) (*http.Response, map[string]any) {
	t.Helper()
	resp, err := r.app.Test(httptest.NewRequest(method, path, nil), 30_000)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(raw) > 0 {
		json.Unmarshal(raw, &out)
	}
	return resp, out
}

func TestEdgeSyncSpoke_RunReportsWhatHappened(t *testing.T) {
	rig := newSpokeRig(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%d.parquet", i)
		if err := rig.backend.Write(ctx, p, []byte(fmt.Sprintf("payload %d", i))); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	resp, body := rig.do(t, http.MethodPost, "/api/v1/spoke-sync/run")
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200; body=%v", resp.StatusCode, body)
	}
	if body["discovered"] != float64(3) {
		t.Errorf("discovered = %v, want 3", body["discovered"])
	}
	if body["sent"] != float64(3) {
		t.Errorf("sent = %v, want 3", body["sent"])
	}
	// Empty rather than null, so a caller need not special-case the field.
	if _, ok := body["conflicts"].([]any); !ok {
		t.Errorf("conflicts = %v, want an empty array", body["conflicts"])
	}
	if body["warning"] != nil {
		t.Errorf("a clean run carried a warning: %v", body["warning"])
	}
}

func TestEdgeSyncSpoke_RunSurfacesConflicts(t *testing.T) {
	rig := newSpokeRig(t)
	ctx := context.Background()

	const p = "metrics/cpu/2026/08/07/14/contested.parquet"
	if err := rig.backend.Write(ctx, p, []byte("the spoke's version")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// The hub holds different content at the same path.
	rig.transport.Seed("ground-station", p, "aa"+fmt.Sprint(1), 19)

	resp, body := rig.do(t, http.MethodPost, "/api/v1/spoke-sync/run")
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	// Reported in full, not counted: an operator has to know which files to
	// look at, and a count alone would not say.
	conflicts, _ := body["conflicts"].([]any)
	if len(conflicts) != 1 {
		t.Fatalf("conflicts = %v, want one", body["conflicts"])
	}
	first, _ := conflicts[0].(map[string]any)
	if first["path"] != p {
		t.Errorf("conflict path = %v, want %q", first["path"], p)
	}
	if body["warning"] == nil {
		t.Error("a run with conflicts carried no warning")
	}
}

func TestEdgeSyncSpoke_StatusReportsLag(t *testing.T) {
	rig := newSpokeRig(t)
	ctx := context.Background()

	if err := rig.backend.Write(ctx, "metrics/cpu/2026/08/07/14/f.parquet", []byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Before any sync there is nothing to be behind.
	_, before := rig.do(t, http.MethodGet, "/api/v1/spoke-sync/status")
	if before["last_synced_at"] != nil {
		t.Error("a spoke that has never synced reports a last-synced time")
	}

	rig.do(t, http.MethodPost, "/api/v1/spoke-sync/run")

	_, after := rig.do(t, http.MethodGet, "/api/v1/spoke-sync/status")
	if after["pending"] != float64(0) {
		t.Errorf("pending = %v after a full sync, want 0", after["pending"])
	}
	if after["synced"] != float64(1) {
		t.Errorf("synced = %v, want 1", after["synced"])
	}
	// The number an operator actually watches.
	if after["seconds_since_last_sync"] == nil {
		t.Error("status does not report sync lag")
	}
}

func TestEdgeSyncSpoke_LedgerShowsStuckFiles(t *testing.T) {
	rig := newSpokeRig(t)
	ctx := context.Background()

	const p = "metrics/cpu/2026/08/07/14/stuck.parquet"
	if err := rig.backend.Write(ctx, p, []byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A hub that keeps rejecting the content.
	rig.transport.ScriptPut("ground-station", p, &edgesync.PutResult{Outcome: edgesync.OutcomeChecksumMismatch})
	rig.do(t, http.MethodPost, "/api/v1/spoke-sync/run")

	resp, body := rig.do(t, http.MethodGet, "/api/v1/spoke-sync/ledger")
	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	entries, _ := body["entries"].([]any)
	if len(entries) != 1 {
		t.Fatalf("entries = %v, want one", body["entries"])
	}
	row, _ := entries[0].(map[string]any)

	// Without this an operator would have to open the SQLite file to answer
	// "why is this not syncing?".
	if row["attempts"] != float64(1) {
		t.Errorf("attempts = %v, want 1", row["attempts"])
	}
	if row["last_error"] == nil {
		t.Error("the ledger view does not show why the transfer failed")
	}
}

func TestEdgeSyncSpoke_LedgerLimitIsBounded(t *testing.T) {
	rig := newSpokeRig(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/08/07/14/f_%02d.parquet", i)
		if err := rig.backend.Write(ctx, p, []byte("payload")); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	// Discovery only, no send, so everything stays pending.
	rig.transport.Close()
	rig.do(t, http.MethodPost, "/api/v1/spoke-sync/run")

	_, body := rig.do(t, http.MethodGet, "/api/v1/spoke-sync/ledger?limit=2")
	entries, _ := body["entries"].([]any)
	if len(entries) != 2 {
		t.Errorf("entries = %d with limit=2, want 2", len(entries))
	}

	// An unbounded response on a long-running edge box would be enormous, so
	// the cap is enforced rather than trusted.
	_, capped := rig.do(t, http.MethodGet, "/api/v1/spoke-sync/ledger?limit=99999")
	if capped["limit"] != float64(1000) {
		t.Errorf("limit = %v for an oversized request, want it capped at 1000", capped["limit"])
	}

	for _, bad := range []string{"0", "-1", "abc"} {
		resp, _ := rig.do(t, http.MethodGet, "/api/v1/spoke-sync/ledger?limit="+bad)
		if resp.StatusCode != fiber.StatusBadRequest {
			t.Errorf("limit=%q: status = %d, want 400", bad, resp.StatusCode)
		}
	}
}

func TestEdgeSyncSpoke_RunFailureIs503(t *testing.T) {
	rig := newSpokeRig(t)
	ctx := context.Background()

	if err := rig.backend.Write(ctx, "metrics/cpu/2026/08/07/14/f.parquet", []byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}
	// A hub that cannot be reached. The pass failed for a reason outside the
	// operator's request, so 503 tells them to retry rather than to fix their
	// call.
	rig.transport.Close()

	resp, _ := rig.do(t, http.MethodPost, "/api/v1/spoke-sync/run")
	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestNewEdgeSyncSpokeHandler_RequiresAnAgent(t *testing.T) {
	// Neither an agent nor an exporter means every route would 503.
	if _, err := NewEdgeSyncSpokeHandler(nil, nil, nil, zerolog.Nop()); err == nil {
		t.Error("a spoke handler was created with neither an agent nor an exporter")
	}
}

// Fiber's Group().Use() matches by string PREFIX, not by path segment, so a
// group at "/api/v1/sync" also matches "/api/v1/sync-spoke/...". Mounting the
// operator controls under such a name would silently subject them to the hub's
// body limit and middleware. This pins the property rather than the name: any
// future rename that reintroduces the collision fails here.
func TestEdgeSyncSpoke_PrefixDoesNotCollideWithHubGroups(t *testing.T) {
	app := fiber.New(fiber.Config{DisableStartupMessage: true})

	hubRan := false
	hub := app.Group("/api/v1/sync")
	hub.Use(func(c *fiber.Ctx) error { hubRan = true; return c.Next() })
	hub.Post("/file", func(c *fiber.Ctx) error { return c.SendString("hub") })

	adminRan := false
	admin := app.Group("/api/v1/sync-spokes")
	admin.Use(func(c *fiber.Ctx) error { adminRan = true; return c.Next() })
	admin.Get("/", func(c *fiber.Ctx) error { return c.SendString("admin") })

	rig := newSpokeRig(t)
	h, err := NewEdgeSyncSpokeHandler(rig.agent, nil, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	h.RegisterRoutes(app)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/api/v1/spoke-sync/status", nil), 30_000)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if hubRan {
		t.Error("the hub group's middleware ran on a spoke route: the prefixes collide")
	}
	if adminRan {
		t.Error("the spoke-admin group's middleware ran on a spoke route: the prefixes collide")
	}
}

// A file that exhausted its retries is terminal until someone intervenes, and
// is the one most likely to have prompted an operator to open this endpoint.
// A pending-only view would hide exactly that.
func TestEdgeSyncSpoke_LedgerShowsExhaustedFiles(t *testing.T) {
	rig := newSpokeRig(t)
	ctx := context.Background()

	const p = "metrics/cpu/2026/08/07/14/doomed.parquet"
	if err := rig.backend.Write(ctx, p, []byte("payload")); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Keep failing it until the agent gives up.
	for i := 0; i < edgesync.DefaultMaxAttempts+2; i++ {
		rig.transport.ScriptPut("ground-station", p, &edgesync.PutResult{Outcome: edgesync.OutcomeChecksumMismatch})
		rig.do(t, http.MethodPost, "/api/v1/spoke-sync/run")
	}

	_, body := rig.do(t, http.MethodGet, "/api/v1/spoke-sync/ledger")
	entries, _ := body["entries"].([]any)
	if len(entries) != 1 {
		t.Fatalf("entries = %v, want the exhausted file to still be listed", body["entries"])
	}
	row, _ := entries[0].(map[string]any)
	if row["state"] != "failed" {
		t.Errorf("state = %v, want %q: an exhausted file is invisible in the ledger view", row["state"], "failed")
	}
	if row["last_error"] == nil {
		t.Error("the exhausted file does not say why it failed")
	}
}
