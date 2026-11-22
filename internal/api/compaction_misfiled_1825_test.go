package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// The files compaction leaves alone as misfiled are listed and cleared
// through the API, by an admin token only.
func TestCompactionMisfiledRoutes1825(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	temp := t.TempDir()
	const stray = "db/cpu/2026/11/18/23/stray.parquet"
	found := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	entries := []compaction.MisfiledEntry{
		{Tier: "daily", Path: stray, Partition: "db/cpu/2026/11/18/23", MinTime: found.Add(-80 * time.Hour), MaxTime: found.Add(-79 * time.Hour), FoundAt: found},
		{Tier: "hourly", Path: stray, Partition: "db/cpu/2026/11/18/23", MinTime: found.Add(-80 * time.Hour), MaxTime: found.Add(-79 * time.Hour), FoundAt: found},
		{Tier: "hourly", Path: "db/cpu/2026/11/18/23/other.parquet", Partition: "db/cpu/2026/11/18/23", FoundAt: found},
	}
	data, _ := json.Marshal(entries)
	if err := os.WriteFile(filepath.Join(temp, "misfiled.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend, LockManager: compaction.NewLockManager(), TempDirectory: temp,
		CycleTimeout: time.Minute, Logger: zerolog.Nop(),
	})

	am, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), 5*time.Minute, 100, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { am.Close() })
	ctx := context.Background()
	admin, err := am.CreateToken(ctx, "admin-1825", "", "read,write,delete,admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := am.CreateToken(ctx, "reader-1825", "", "read", nil)
	if err != nil {
		t.Fatal(err)
	}
	app := fiber.New()
	mc := auth.DefaultMiddlewareConfig()
	mc.AuthManager = am
	app.Use(auth.NewMiddleware(mc))
	NewCompactionHandler(manager, nil, nil, am, nil, zerolog.Nop()).RegisterRoutes(app)

	do := func(method, url, token string) (int, map[string]interface{}) {
		t.Helper()
		req := httptest.NewRequest(method, url, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		var out map[string]interface{}
		_ = json.Unmarshal(body, &out)
		return resp.StatusCode, out
	}

	for _, method := range []string{"GET", "DELETE"} {
		if code, _ := do(method, "/api/v1/compaction/misfiled", ""); code != 401 {
			t.Errorf("%s without a token: %d, want 401", method, code)
		}
		if code, _ := do(method, "/api/v1/compaction/misfiled", reader); code != 403 {
			t.Errorf("%s with a read-only token: %d, want 403", method, code)
		}
	}

	code, list := do("GET", "/api/v1/compaction/misfiled", admin)
	if code != 200 || list["count"] != float64(3) || list["scope"] != "this node" {
		t.Fatalf("list: %d %v", code, list)
	}
	first := list["files"].([]interface{})[0].(map[string]interface{})
	if first["path"] != "db/cpu/2026/11/18/23/other.parquet" || first["tier"] != "hourly" || first["found_at"] != "2026-11-22T10:00:00Z" {
		t.Fatalf("first entry: %v", first)
	}

	if code, out := do("DELETE", "/api/v1/compaction/misfiled?tier=daily&path="+stray, admin); code != 200 || out["cleared"] != float64(1) {
		t.Fatalf("clear one tier of one path: %d %v", code, out)
	}
	if _, list := do("GET", "/api/v1/compaction/misfiled", admin); list["count"] != float64(2) {
		t.Fatalf("after clearing one entry: %v", list)
	}
	if code, out := do("DELETE", "/api/v1/compaction/misfiled", admin); code != 200 || out["cleared"] != float64(2) {
		t.Fatalf("clear all: %d %v", code, out)
	}
	if _, list := do("GET", "/api/v1/compaction/misfiled", admin); list["count"] != float64(0) {
		t.Fatalf("after clearing all: %v", list)
	}
}
