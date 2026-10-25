package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// A partition remembered as too wide to compact is listed, flagged among the
// candidates, and cleared through the API.
func TestCompactionTooWideRoutes(t *testing.T) {
	root := t.TempDir()
	const partition = "db/wide/2026/10/20/10"
	var files []string
	for i := 0; i < 12; i++ {
		rel := fmt.Sprintf("%s/f%02d.parquet", partition, i)
		files = append(files, rel)
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte("PAR1"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	temp := t.TempDir()
	cfg := &compaction.ManagerConfig{
		StorageBackend: backend,
		LockManager:    compaction.NewLockManager(),
		TempDirectory:  temp,
		CycleTimeout:   time.Minute,
		Logger:         zerolog.Nop(),
		Tiers: []compaction.Tier{compaction.NewHourlyTier(&compaction.HourlyTierConfig{
			StorageBackend: backend, MinAgeHours: 1, MinFiles: 10, Enabled: true, Logger: zerolog.Nop(),
		})},
	}
	// Remembered by an earlier process under the settings this one runs with.
	probe := compaction.NewManager(cfg)
	settings := probe.TooWideSettings()
	entry := []compaction.TooWideEntry{{Tier: "hourly", Partition: partition, Files: files, Reason: "memory_error", Strikes: 1, Settings: settings}}
	data, _ := json.Marshal(entry)
	if err := os.WriteFile(filepath.Join(temp, "too_wide.json"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	manager := compaction.NewManager(cfg)
	app := fiber.New()
	NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop()).RegisterRoutes(app)

	do := func(method, url string) map[string]interface{} {
		t.Helper()
		resp, err := app.Test(httptest.NewRequest(method, url, nil), -1)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != 200 {
			t.Fatalf("%s %s: %d %s", method, url, resp.StatusCode, body)
		}
		var out map[string]interface{}
		if err := json.Unmarshal(body, &out); err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		return out
	}

	list := do("GET", "/api/v1/compaction/too-wide")
	parts := list["partitions"].([]interface{})
	if len(parts) != 1 {
		t.Fatalf("list %v", list)
	}
	p := parts[0].(map[string]interface{})
	if p["partition"] != partition || p["skipped"] != true || p["files"] != float64(12) || p["reason"] != "memory_error" {
		t.Fatalf("entry %v", p)
	}

	cands := do("GET", "/api/v1/compaction/candidates")["candidates"].([]interface{})
	if len(cands) != 1 || cands[0].(map[string]interface{})["too_wide"] != true {
		t.Fatalf("candidates %v", cands)
	}

	if got := do("DELETE", "/api/v1/compaction/too-wide?tier=daily")["cleared"]; got != float64(0) {
		t.Fatalf("cleared another tier: %v", got)
	}
	if got := do("DELETE", "/api/v1/compaction/too-wide")["cleared"]; got != float64(1) {
		t.Fatalf("cleared %v", got)
	}
	if n := do("GET", "/api/v1/compaction/too-wide")["count"]; n != float64(0) {
		t.Fatalf("still listed: %v", n)
	}
	if c := do("GET", "/api/v1/compaction/candidates")["candidates"].([]interface{}); c[0].(map[string]interface{})["too_wide"] != false {
		t.Fatalf("still flagged: %v", c)
	}
}
