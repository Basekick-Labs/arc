package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func postJSON1884(t *testing.T, app *fiber.App, path string) (int, map[string]interface{}) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, path, nil), 20000)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var out map[string]interface{}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("POST %s returned non-JSON %q", path, body)
	}
	return resp.StatusCode, out
}

// Each process has its own boot id.
func TestBootIDDiffersPerManager1884(t *testing.T) {
	a, _ := cyclesRig(t)
	b, _ := cyclesRig(t)
	if a.BootID() == "" || len(a.BootID()) != 16 || a.BootID() == b.BootID() {
		t.Fatalf("boot ids %q and %q: want two distinct 16-hex values", a.BootID(), b.BootID())
	}
}

// A lookup with another process's boot_id is refused even when this process
// has a record with that number: that record is another cycle. The same
// boot_id, or none, finds it.
func TestGetCycleRefusesAnotherProcessesID1884(t *testing.T) {
	manager, app := cyclesRig(t)
	id, err := manager.RunCompactionCycleForDatabase(t.Context(), "db_d", nil)
	if err != nil {
		t.Fatal(err)
	}
	path := fmt.Sprintf("/api/v1/compaction/cycles/%d", id)

	status, body := getJSON(t, app, path+"?boot_id=0123456789abcdef")
	if status != http.StatusNotFound || body["boot_id_mismatch"] != true || body["boot_id"] != manager.BootID() {
		t.Fatalf("mismatched boot_id: status %d body %v; want 404 boot_id_mismatch with this process's boot_id", status, body)
	}
	status, body = getJSON(t, app, path+"?boot_id="+manager.BootID())
	if status != http.StatusOK || body["boot_id"] != manager.BootID() {
		t.Fatalf("matching boot_id: status %d body %v", status, body)
	}
	for _, q := range []string{"", "?boot_id="} { // absent, or empty: as before
		if status, body = getJSON(t, app, path+q); status != http.StatusOK {
			t.Fatalf("GET %s%s: status %d body %v, want 200", path, q, status, body)
		}
	}
}

// A claimed cycle not yet recorded is not "claimed" for another process's id.
func TestGetCycleMismatchBeatsTheClaimedAnswer1884(t *testing.T) {
	manager, app := cyclesRig(t)
	claim, err := manager.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Release()
	path := fmt.Sprintf("/api/v1/compaction/cycles/%d", claim.ID)
	if status, body := getJSON(t, app, path+"?boot_id=0123456789abcdef"); status != http.StatusNotFound || body["boot_id_mismatch"] != true {
		t.Fatalf("status %d body %v; want 404 boot_id_mismatch, not the claimed answer", status, body)
	}
	if status, body := getJSON(t, app, path); status != http.StatusOK || body["status"] != "claimed" || body["boot_id"] != manager.BootID() {
		t.Fatalf("status %d body %v; want the claimed answer with boot_id", status, body)
	}
}

// /history with another process's cycle id returns no jobs: the jobs here with
// that number belong to another cycle.
func TestHistoryRefusesAnotherProcessesID1884(t *testing.T) {
	manager, app := cyclesRig(t)
	id, err := manager.RunCompactionCycleForDatabase(t.Context(), "db_d", nil)
	if err != nil {
		t.Fatal(err)
	}
	status, body := getJSON(t, app, fmt.Sprintf("/api/v1/compaction/history?cycle_id=%d&boot_id=0123456789abcdef", id))
	if status != http.StatusOK || body["cycle_retained"] != false || body["boot_id_mismatch"] != true {
		t.Fatalf("status %d body %v", status, body)
	}
	if jobs, _ := body["recent_jobs"].([]interface{}); len(jobs) != 0 {
		t.Fatalf("returned %d jobs of another cycle", len(jobs))
	}
}

// /stats carries the boot id, so a client polling it can tell a restart.
func TestStatsCarriesBootID1884(t *testing.T) {
	manager, app := cyclesRig(t)
	if status, body := getJSON(t, app, "/api/v1/compaction/stats"); status != http.StatusOK || body["boot_id"] != manager.BootID() {
		t.Fatalf("status %d boot_id %v, want %s", status, body["boot_id"], manager.BootID())
	}
}

// ?wait answers with the finished cycle; a bad value claims nothing.
func TestTriggerWait1884(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	manager := compaction.NewManager(&compaction.ManagerConfig{StorageBackend: backend, CycleTimeout: time.Minute, Logger: zerolog.Nop()})
	manager.Tiers = []compaction.Tier{compaction.NewHourlyTier(&compaction.HourlyTierConfig{StorageBackend: backend, MinAgeHours: 0, MinFiles: 10, Enabled: true, Logger: zerolog.Nop()})}
	app := fiber.New()
	NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop()).RegisterRoutes(app)

	before := manager.RunningCycleID()
	if status, body := postJSON1884(t, app, "/api/v1/compaction/trigger?wait=banana"); status != http.StatusBadRequest {
		t.Fatalf("wait=banana: status %d body %v, want 400", status, body)
	}
	if manager.RunningCycleID() != before {
		t.Fatal("a rejected wait claimed a cycle")
	}
	status, body := postJSON1884(t, app, "/api/v1/compaction/trigger?wait=10s")
	if status != http.StatusOK || body["status"] != "completed" || body["boot_id"] != manager.BootID() {
		t.Fatalf("wait=10s: status %d body %v; want 200 with the finished cycle", status, body)
	}
}

// A wait that runs out answers 202 with the trigger's answer; the cycle
// carries on.
func TestTriggerWaitTimesOut1884(t *testing.T) {
	manager, _ := cyclesRig(t)
	claim, err := manager.ClaimCycle() // claimed, never recorded: still running
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Release()
	h := NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	app.Get("/w", func(c *fiber.Ctx) error {
		return h.waitForCycle(c, claim.ID, 300*time.Millisecond, fiber.Map{"status": "running", "cycle_id": claim.ID})
	})
	start := time.Now()
	status, body := getJSON(t, app, "/w")
	if status != http.StatusAccepted || body["status"] != "running" {
		t.Fatalf("status %d body %v; want 202 running", status, body)
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("took %v for a 300 ms wait", took)
	}
}

func TestParseTriggerWait1884(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"", 0, false}, {"90s", 90 * time.Second, false}, {"1h", triggerWaitMax, false},
		{"0s", 0, true}, {"-5s", 0, true}, {"5", 0, true},
	} {
		got, err := parseTriggerWait(tc.in)
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("parseTriggerWait(%q) = %v, %v; want %v, err=%v", tc.in, got, err, tc.want, tc.wantErr)
		}
	}
}
