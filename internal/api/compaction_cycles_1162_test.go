package api

// Per-cycle lookup endpoints (#1162). /compaction/stats carries a single
// global last_cycle, so the cycle_id the trigger hands back had nowhere to be
// resolved; these routes resolve it.

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// cyclesRig builds a handler over a tier-less manager, so a cycle completes
// immediately and still records -- the cheap way to seed history.
func cyclesRig(t *testing.T) (*compaction.Manager, *fiber.App) {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend, CycleTimeout: time.Minute, Logger: zerolog.Nop(),
	})
	handler := NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)
	return manager, app
}

func getJSON(t *testing.T, app *fiber.App, path string) (int, map[string]interface{}) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest(http.MethodGet, path, nil), 10000)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("GET %s returned non-JSON %q: %v", path, string(body), err)
	}
	return resp.StatusCode, out
}

// A retained cycle resolves by id, carrying the scope that makes it
// identifiable.
func TestGetCycleReturnsTheRetainedRecordIssue1162(t *testing.T) {
	manager, app := cyclesRig(t)
	cycleID, err := manager.RunCompactionCycleForDatabase(t.Context(), "db_d", nil)
	if err != nil {
		t.Fatalf("seed cycle failed: %v", err)
	}

	status, body := getJSON(t, app, fmt.Sprintf("/api/v1/compaction/cycles/%d", cycleID))
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %v)", status, body)
	}
	if got := body["cycle_id"]; got != float64(cycleID) {
		t.Errorf("cycle_id = %v, want %d", got, cycleID)
	}
	if got := body["status"]; got != "completed" {
		t.Errorf("status = %v, want completed", got)
	}
	dbs, ok := body["databases"].([]interface{})
	if !ok || len(dbs) != 1 || dbs[0] != "db_d" {
		t.Errorf("databases = %v, want [db_d]", body["databases"])
	}
	if _, ok := body["finished_at"]; !ok {
		t.Error("finished_at missing on a completed cycle")
	}
	if _, ok := body["duration_seconds"]; !ok {
		t.Error("duration_seconds missing")
	}
	// A zero time must never surface as 0001-01-01: omitempty does nothing
	// for a time.Time, which is why the response is built conditionally.
	for _, key := range []string{"started_at", "finished_at"} {
		if s, _ := body[key].(string); strings.HasPrefix(s, "0001-01-01") {
			t.Errorf("%s = %q, a zero time leaked into the response", key, s)
		}
	}
}

// A cycle covering every database reports databases: null. Deliberately not
// [] -- that would be ambiguous with "no databases".
func TestGetCycleReportsUnscopedCycleAsNullIssue1162(t *testing.T) {
	manager, app := cyclesRig(t)
	cycleID, err := manager.RunCompactionCycleForTiers(t.Context(), nil)
	if err != nil {
		t.Fatalf("seed cycle failed: %v", err)
	}

	status, body := getJSON(t, app, fmt.Sprintf("/api/v1/compaction/cycles/%d", cycleID))
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if got, present := body["databases"]; !present || got != nil {
		t.Errorf("databases = %v (present=%v), want explicit null for an all-database cycle", got, present)
	}
}

// The window this endpoint exists for: the trigger answers with the cycle id
// as soon as it holds the claim, before the cycle body has recorded anything.
// An operator looking up the id they were just handed must not be told it
// never ran.
func TestGetCycleAnswersClaimedBeforeTheRecordExistsIssue1162(t *testing.T) {
	manager, app := cyclesRig(t)

	claim, err := manager.ClaimCycle()
	if err != nil {
		t.Fatalf("ClaimCycle: %v", err)
	}
	defer claim.Release()

	status, body := getJSON(t, app, fmt.Sprintf("/api/v1/compaction/cycles/%d", claim.ID))
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a claimed-but-unrecorded cycle (body %v)", status, body)
	}
	if got := body["status"]; got != "claimed" {
		t.Errorf("status = %v, want claimed", got)
	}
	if got := body["cycle_id"]; got != float64(claim.ID) {
		t.Errorf("cycle_id = %v, want %d", got, claim.ID)
	}
}

// Once the claim is gone, the same id is a plain 404 -- the synthesized
// "claimed" answer must not outlive the claim.
func TestGetCycleStopsSynthesizingAfterTheClaimIsReleasedIssue1162(t *testing.T) {
	manager, app := cyclesRig(t)

	claim, err := manager.ClaimCycle()
	if err != nil {
		t.Fatalf("ClaimCycle: %v", err)
	}
	id := claim.ID
	claim.Release()

	status, body := getJSON(t, app, fmt.Sprintf("/api/v1/compaction/cycles/%d", id))
	if status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 after the claim was released (body %v)", status, body)
	}
}

// The three absent-record cases each get their own wording. The messages are
// the whole value of the 404: "too old", "not here", and "nothing at all" lead
// an operator to different next steps.
func TestGetCycleDistinguishesWhyARecordIsAbsentIssue1162(t *testing.T) {
	t.Run("empty history omits the range", func(t *testing.T) {
		_, app := cyclesRig(t)
		status, body := getJSON(t, app, "/api/v1/compaction/cycles/7")
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", status)
		}
		// Ids start at 1, so a 0/0 range would make every id look newer than
		// the newest retained cycle.
		if _, present := body["oldest_retained"]; present {
			t.Errorf("oldest_retained present on an empty history: %v", body["oldest_retained"])
		}
		if _, present := body["newest_retained"]; present {
			t.Errorf("newest_retained present on an empty history: %v", body["newest_retained"])
		}
		if msg, _ := body["message"].(string); !strings.Contains(msg, "no compaction cycle") {
			t.Errorf("message = %q, want it to say nothing has run here", msg)
		}
	})

	t.Run("newer than retained does not claim it never ran", func(t *testing.T) {
		manager, app := cyclesRig(t)
		if _, err := manager.RunCompactionCycleForTiers(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		status, body := getJSON(t, app, "/api/v1/compaction/cycles/99999")
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", status)
		}
		msg, _ := body["message"].(string)
		// Cycle ids restart at 1 on restart, so an id recorded before one did
		// run here even though nothing is retained for it.
		if strings.Contains(strings.ToLower(msg), "never ran") {
			t.Errorf("message asserts the cycle never ran, which is false across a restart: %q", msg)
		}
		if !strings.Contains(msg, "restart") {
			t.Errorf("message = %q, want the id-restart caveat", msg)
		}
		if _, present := body["newest_retained"]; !present {
			t.Error("newest_retained missing when a range exists")
		}
	})

	t.Run("older than retained says so", func(t *testing.T) {
		manager, app := cyclesRig(t)
		// Tier-less cycles complete immediately, so overflowing the ring is
		// cheap and needs no test seam.
		for i := 0; i < compaction.CycleHistoryLimit+5; i++ {
			if _, err := manager.RunCompactionCycleForTiers(t.Context(), nil); err != nil {
				t.Fatalf("seed cycle %d: %v", i, err)
			}
		}
		status, body := getJSON(t, app, "/api/v1/compaction/cycles/1")
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 for a trimmed cycle", status)
		}
		if msg, _ := body["message"].(string); !strings.Contains(msg, "older than") {
			t.Errorf("message = %q, want it to say the cycle was trimmed", msg)
		}
		oldest, _ := body["oldest_retained"].(float64)
		if oldest <= 1 {
			t.Errorf("oldest_retained = %v, want it past the trimmed id", body["oldest_retained"])
		}
	})
}

// A non-numeric or non-positive id is a bad request, not a trimmed cycle.
func TestGetCycleRejectsBadIdsIssue1162(t *testing.T) {
	_, app := cyclesRig(t)
	for _, id := range []string{"abc", "0", "-3", "1.5", ""} {
		status, _ := getJSON(t, app, "/api/v1/compaction/cycles/"+id)
		if id == "" {
			// Fiber routes an empty trailing segment to the list route.
			if status != http.StatusOK {
				t.Errorf("GET /cycles/ = %d, want the list route 200", status)
			}
			continue
		}
		if status != http.StatusBadRequest {
			t.Errorf("GET /cycles/%s = %d, want 400", id, status)
		}
	}
}

// The list is newest-first, honours and clamps limit, and its
// running_cycle_id can never contradict the records beside it.
func TestListCyclesOrdersAndClampsIssue1162(t *testing.T) {
	manager, app := cyclesRig(t)
	var ids []int64
	for i := 0; i < 4; i++ {
		id, err := manager.RunCompactionCycleForTiers(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}

	status, body := getJSON(t, app, "/api/v1/compaction/cycles?limit=2")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	cycles, _ := body["cycles"].([]interface{})
	if len(cycles) != 2 {
		t.Fatalf("returned %d cycles, want 2", len(cycles))
	}
	first, _ := cycles[0].(map[string]interface{})
	if got := first["cycle_id"]; got != float64(ids[3]) {
		t.Errorf("first cycle_id = %v, want %d (newest first)", got, ids[3])
	}
	if got := body["retained"]; got != float64(4) {
		t.Errorf("retained = %v, want 4", got)
	}
	// Nothing is running, so the key must be absent rather than 0.
	if _, present := body["running_cycle_id"]; present {
		t.Errorf("running_cycle_id present with no cycle running: %v", body["running_cycle_id"])
	}

	for _, q := range []string{"?limit=0", "?limit=-1", "?limit=garbage", ""} {
		status, body := getJSON(t, app, "/api/v1/compaction/cycles"+q)
		if status != http.StatusOK {
			t.Errorf("GET /cycles%s = %d, want 200", q, status)
		}
		if cycles, _ := body["cycles"].([]interface{}); len(cycles) != 4 {
			t.Errorf("GET /cycles%s returned %d cycles, want the default page of 4", q, len(cycles))
		}
	}

	status, body = getJSON(t, app, "/api/v1/compaction/cycles?limit=999999")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if cycles, _ := body["cycles"].([]interface{}); len(cycles) != 4 {
		t.Errorf("clamped limit returned %d cycles, want 4", len(cycles))
	}
}

// Both routes report 503 rather than panicking when compaction is not wired.
func TestCycleRoutesWithoutAManagerIssue1162(t *testing.T) {
	handler := NewCompactionHandler(nil, nil, nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)

	for _, path := range []string{"/api/v1/compaction/cycles", "/api/v1/compaction/cycles/1"} {
		status, body := getJSON(t, app, path)
		if status != http.StatusServiceUnavailable {
			t.Errorf("GET %s = %d, want 503 (body %v)", path, status, body)
		}
	}
}
