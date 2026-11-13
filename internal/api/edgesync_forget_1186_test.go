package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/edgesync"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

type forgetRig struct {
	app     *fiber.App
	idx     *edgesync.HubIndex
	backend storage.Backend
}

func newForgetRig(t *testing.T, wire bool) *forgetRig {
	t.Helper()
	dir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(dir, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	reg, err := edgesync.NewRegistry(db, newAdminTestCipher(t), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	idx, err := edgesync.NewHubIndex(db, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	backend, err := storage.NewLocalBackend(filepath.Join(dir, "data"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewEdgeSyncAdminHandler(reg, nil, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if wire {
		h.SetReceiptRecovery(idx, backend)
	}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	return &forgetRig{app: app, idx: idx, backend: backend}
}

func (r *forgetRig) forget(t *testing.T, spoke string, body any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sync-spokes/"+spoke+"/forget", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func (r *forgetRig) record(t *testing.T, spoke, path string) {
	t.Helper()
	if err := r.idx.Record(context.Background(), &edgesync.ReceivedRecord{
		SpokeID: spoke, SourcePath: path, HubPath: spoke + "/" + path, SHA256: "aa", SizeBytes: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

// The explicit recovery (#1186): removed receipts under the database are
// forgotten, so the spoke re-sends them; compacted-only receipts stay.
func TestEdgeSyncForget_ForgetsRemovedReceipts(t *testing.T) {
	r := newForgetRig(t, true)
	ctx := context.Background()
	r.record(t, "rocket01", "telemetry/cpu/a.parquet")
	r.record(t, "rocket01", "telemetry/cpu/b.parquet")
	r.record(t, "rocket01", "telemetry/cpu/c.parquet")
	if err := r.idx.MarkCompacted(ctx, "rocket01", []string{"telemetry/cpu/c.parquet"}); err != nil {
		t.Fatal(err)
	}
	if err := r.idx.MarkRemoved(ctx, "rocket01", []string{"telemetry/cpu/a.parquet", "telemetry/cpu/b.parquet"}, edgesync.RemovedByOperatorDelete); err != nil {
		t.Fatal(err)
	}
	code, body := r.forget(t, "rocket01", map[string]string{"database": "telemetry"})
	if code != fiber.StatusOK || body["forgotten"] != float64(2) {
		t.Fatalf("status %d body %v, want 200 forgotten=2", code, body)
	}
	held, _ := r.idx.Lookup(ctx, "rocket01", []string{"telemetry/cpu/a.parquet", "telemetry/cpu/c.parquet"})
	if _, ok := held["telemetry/cpu/a.parquet"]; ok {
		t.Error("removed receipt survived the forget")
	}
	if hf, ok := held["telemetry/cpu/c.parquet"]; !ok || !hf.Compacted {
		t.Error("a compacted-only receipt was forgotten")
	}
}

// Refused while files remain under the namespace: re-sent originals next to
// a surviving compacted output would duplicate rows (#611).
func TestEdgeSyncForget_RefusedWhileNamespaceHoldsFiles(t *testing.T) {
	r := newForgetRig(t, true)
	ctx := context.Background()
	r.record(t, "rocket01", "telemetry/cpu/a.parquet")
	if err := r.idx.MarkSpokeRemoved(ctx, "rocket01", edgesync.RemovedByOperatorDelete); err != nil {
		t.Fatal(err)
	}
	if err := r.backend.Write(ctx, "rocket01/telemetry/cpu/compacted.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if code, _ := r.forget(t, "rocket01", map[string]string{"database": "telemetry"}); code != fiber.StatusConflict {
		t.Fatalf("status %d, want 409", code)
	}
	if held, _ := r.idx.Lookup(ctx, "rocket01", []string{"telemetry/cpu/a.parquet"}); len(held) != 1 {
		t.Fatal("a refused forget removed the receipt")
	}
	// A sibling database's files do not block this one.
	if err := r.backend.Delete(ctx, "rocket01/telemetry/cpu/compacted.parquet"); err != nil {
		t.Fatal(err)
	}
	if err := r.backend.Write(ctx, "rocket01/telemetry2/cpu/x.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if code, body := r.forget(t, "rocket01", map[string]string{"database": "telemetry"}); code != fiber.StatusOK {
		t.Fatalf("status %d body %v, want 200 (telemetry2 is another database)", code, body)
	}
}

func TestEdgeSyncForget_Validation(t *testing.T) {
	r := newForgetRig(t, true)
	for _, body := range []map[string]string{{}, {"database": ""}, {"database": "a/b"}, {"database": `a\b`}} {
		if code, _ := r.forget(t, "rocket01", body); code != fiber.StatusBadRequest {
			t.Errorf("body %v: status %d, want 400", body, code)
		}
	}
	if code, _ := newForgetRig(t, false).forget(t, "rocket01", map[string]string{"database": "telemetry"}); code != fiber.StatusServiceUnavailable {
		t.Errorf("unwired: status %d, want 503", code)
	}
}

// The route sits in the admin group: with auth on, a request without an
// admin token is refused before the handler runs.
func TestEdgeSyncForget_RequiresAdmin(t *testing.T) {
	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), 0, 0, zerolog.Nop())
	if err != nil {
		t.Skipf("auth manager unavailable in this test build: %v", err)
	}
	defer am.Close()
	reg, err := edgesync.NewRegistry(am.GetDB(), newAdminTestCipher(t), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewEdgeSyncAdminHandler(reg, am, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(app)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/sync-spokes/rocket01/forget", bytes.NewReader([]byte(`{"database":"telemetry"}`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req, testRequestTimeoutMS)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != fiber.StatusUnauthorized && resp.StatusCode != fiber.StatusForbidden {
		t.Fatalf("status %d without a token, want 401/403", resp.StatusCode)
	}
}

// The guard sees the cold tier too: a hot prefix that is empty while a cold
// copy of the namespace survives (a delete whose cold cleanup failed part way)
// is refused, since the spoke's re-sent files would sit next to that copy.
func TestEdgeSyncForget_RefusedWhileColdTierHoldsFiles(t *testing.T) {
	r := newForgetRig(t, false)
	dir := t.TempDir()
	cold, err := storage.NewLocalBackend(filepath.Join(dir, "cold"), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite3", filepath.Join(dir, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	reg, err := edgesync.NewRegistry(db, newAdminTestCipher(t), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewEdgeSyncAdminHandler(reg, nil, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	h.SetReceiptRecovery(r.idx, r.backend)
	h.SetColdStorage(func() storage.Backend { return cold })
	r.app = fiber.New(fiber.Config{DisableStartupMessage: true})
	h.RegisterRoutes(r.app)

	ctx := context.Background()
	r.record(t, "rocket01", "telemetry/cpu/a.parquet")
	if err := r.idx.MarkSpokeRemoved(ctx, "rocket01", edgesync.RemovedByOperatorDelete); err != nil {
		t.Fatal(err)
	}
	if err := cold.Write(ctx, "rocket01/telemetry/cpu/2026/01/01/00/compacted.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if code, _ := r.forget(t, "rocket01", map[string]string{"database": "telemetry"}); code != fiber.StatusConflict {
		t.Fatalf("status %d with a cold copy present, want 409", code)
	}
	if err := cold.Delete(ctx, "rocket01/telemetry/cpu/2026/01/01/00/compacted.parquet"); err != nil {
		t.Fatal(err)
	}
	if code, body := r.forget(t, "rocket01", map[string]string{"database": "telemetry"}); code != fiber.StatusOK || body["forgotten"] != float64(1) {
		t.Fatalf("status %d body %v, want 200 forgotten=1 once cold is empty", code, body)
	}
}
