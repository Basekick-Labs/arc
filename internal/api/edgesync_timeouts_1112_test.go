package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// The manual sync pass is bounded by the configured edge_sync.spoke.sync_timeout,
// not a constant (#1112): a bound shorter than the pass stops it. Same agent,
// same file; only the handler's timeout differs.
func TestEdgeSyncSpoke_RunHonoursConfiguredTimeout(t *testing.T) {
	rig := newSpokeRig(t)
	if err := rig.backend.Write(context.Background(), "metrics/cpu/2026/08/07/14/f.parquet", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	h, err := NewEdgeSyncSpokeHandler(rig.agent, nil, nil, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	h.SetTimeouts(time.Nanosecond, 0)
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/api/v1/spoke-sync/run", nil), 30_000)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode == fiber.StatusOK {
		t.Fatal("a 1ns sync_timeout let the pass complete; the handler is not using the configured bound")
	}
	if _, held := rig.transport.Has("ground-station", "metrics/cpu/2026/08/07/14/f.parquet"); held {
		t.Fatal("the hub received the file under a 1ns bound; the pass was not stopped by it")
	}

	// The default handler on the same rig completes the pass.
	ok, body := rig.do(t, http.MethodPost, "/api/v1/spoke-sync/run")
	if ok.StatusCode != fiber.StatusOK || body["sent"] != float64(1) {
		t.Fatalf("default handler: status %d body %v, want 200 with the file sent", ok.StatusCode, body)
	}
}

// A non-positive value keeps the current bound, so a zero can never become an
// already-expired context; positive values replace it.
func TestEdgeSyncHandlers_SetTimeouts(t *testing.T) {
	rig := newSpokeRig(t)
	h, err := NewEdgeSyncSpokeHandler(rig.agent, nil, nil, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if h.syncTimeout != defaultSyncRunTimeout || h.exportTimeout != defaultBundleExportTimeout {
		t.Fatalf("defaults = %s/%s", h.syncTimeout, h.exportTimeout)
	}
	h.SetTimeouts(0, -time.Second)
	if h.syncTimeout != defaultSyncRunTimeout || h.exportTimeout != defaultBundleExportTimeout {
		t.Fatalf("non-positive values changed the bounds: %s/%s", h.syncTimeout, h.exportTimeout)
	}
	h.SetTimeouts(3*time.Minute, 7*time.Minute)
	if h.syncTimeout != 3*time.Minute || h.exportTimeout != 7*time.Minute {
		t.Fatalf("bounds = %s/%s, want 3m/7m", h.syncTimeout, h.exportTimeout)
	}

	imp := &EdgeSyncImportHandler{timeout: defaultBundleImportTimeout}
	imp.SetTimeout(0)
	if imp.timeout != defaultBundleImportTimeout {
		t.Fatalf("zero changed the import bound to %s", imp.timeout)
	}
	imp.SetTimeout(9 * time.Minute)
	if imp.timeout != 9*time.Minute {
		t.Fatalf("import bound = %s, want 9m", imp.timeout)
	}
}
