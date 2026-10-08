package api

// GET /api/v1/compaction/history gains a ?cycle_id= filter and a limit that
// works above 10 (#1162 follow-up). These cover the handler's contract: the
// filtering itself is covered in internal/compaction, and the end-to-end
// proof is a live binary run, because the only thing that writes job history
// is CompactPartition, which runs a real compaction subprocess.

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

func historyRig(t *testing.T) *fiber.App {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend, CycleTimeout: time.Minute, Logger: zerolog.Nop(),
	})
	app := fiber.New()
	NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop()).RegisterRoutes(app)
	return app
}

// An invalid cycle_id is rejected, never treated as "no filter". Returning
// every job for a malformed id would read as "this cycle touched everything",
// which is the most misleading answer available.
func TestHistoryRejectsInvalidCycleIdIssue1162(t *testing.T) {
	app := historyRig(t)

	for _, raw := range []string{"0", "-1", "abc", "1.5", "9e9"} {
		status, body := getJSON(t, app, "/api/v1/compaction/history?cycle_id="+raw)
		if status != http.StatusBadRequest {
			t.Errorf("cycle_id=%s -> %d, want 400 (body %v)", raw, status, body)
		}
	}
}

// An absent cycle_id means no filter, and must not 400.
func TestHistoryWithoutACycleIdIsUnfilteredIssue1162(t *testing.T) {
	app := historyRig(t)

	status, body := getJSON(t, app, "/api/v1/compaction/history")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %v)", status, body)
	}
	if _, present := body["cycle_id"]; present {
		t.Errorf("cycle_id echoed on an unfiltered request: %v", body["cycle_id"])
	}
}

// The response carries what an operator needs to know the page is partial.
func TestHistoryResponseCarriesMatchAndTruncationIssue1162(t *testing.T) {
	app := historyRig(t)

	status, body := getJSON(t, app, "/api/v1/compaction/history?cycle_id=412&limit=25")
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	for _, key := range []string{"total_jobs", "recent_jobs", "matched", "page_size",
		"truncated", "retained", "capacity", "unit"} {
		if _, present := body[key]; !present {
			t.Errorf("response is missing %q", key)
		}
	}
	// Echoed so a client can tell which cycle a page describes.
	if got := body["cycle_id"]; got != float64(412) {
		t.Errorf("cycle_id = %v, want 412 echoed back", got)
	}
	if got := body["matched"]; got != float64(0) {
		t.Errorf("matched = %v, want 0 on an empty history", got)
	}
	if got := body["truncated"]; got != false {
		t.Errorf("truncated = %v, want false when nothing matched and the ring is not full", got)
	}
	if got := body["capacity"]; got != float64(compaction.JobHistoryLimit) {
		t.Errorf("capacity = %v, want the ring bound %d", got, compaction.JobHistoryLimit)
	}
	// Records are attempts, not batches: the splitter retries a failed batch at
	// half size and every attempt writes its own record. Saying so in the
	// payload is the difference between a count an operator can act on and one
	// they will misread.
	if got := body["unit"]; got != "attempts" {
		t.Errorf("unit = %v, want attempts", got)
	}

	// Cycle 412 is not retained here, and that MUST be distinguishable from a
	// cycle that failed nothing -- the most expensive wrong answer available.
	if got := body["cycle_retained"]; got != false {
		t.Errorf("cycle_retained = %v, want false for an unretained cycle", got)
	}
	if msg, _ := body["message"].(string); msg == "" {
		t.Error("no message explaining that an empty result does not mean the cycle failed nothing")
	}
}

// limit is clamped to the ring bound and falls back on garbage, so a caller
// cannot ask for more than exists or crash the handler.
func TestHistoryLimitClampsAndFallsBackIssue1162(t *testing.T) {
	app := historyRig(t)

	for _, q := range []string{"", "?limit=1", "?limit=0", "?limit=-5", "?limit=garbage",
		fmt.Sprintf("?limit=%d", compaction.JobHistoryLimit+5000)} {
		status, body := getJSON(t, app, "/api/v1/compaction/history"+q)
		if status != http.StatusOK {
			t.Errorf("GET /history%s -> %d, want 200 (body %v)", q, status, body)
		}
		if _, ok := body["recent_jobs"].([]interface{}); !ok {
			t.Errorf("GET /history%s: recent_jobs is not an array: %v", q, body["recent_jobs"])
		}
	}
}

// Still 503 rather than a panic when compaction is not wired.
func TestHistoryWithoutAManagerIssue1162(t *testing.T) {
	app := fiber.New()
	NewCompactionHandler(nil, nil, nil, nil, nil, zerolog.Nop()).RegisterRoutes(app)

	status, _ := getJSON(t, app, "/api/v1/compaction/history?cycle_id=5")
	if status != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", status)
	}
}

// The headline wire contract, asserted directly on the renderer.
//
// cycleResponse is unexported but takes an exported compaction.CycleRecord with
// exported fields, so this needs no rig at all -- and without it, deleting
// failed_partitions, failed_partition_count and failed_partitions_truncated
// from the response left the whole api package green. That is the deliverable
// of this change, so it has to be pinned at the layer that serialises it.
func TestCycleResponseRendersFailedPartitionsIssue1162(t *testing.T) {
	rec := compaction.CycleRecord{
		CycleID: 412, Status: "failed", Source: "api",
		StartedAt: time.Now().UTC().Add(-time.Hour), FinishedAt: time.Now().UTC(),
		Failed: 21,
		FailedPartitions: map[string]int{
			"db_d/wide_table/2026/09/14/08": 19,
			"db_d/wide_table/2026/09/21/16": 2,
		},
	}

	out := cycleResponse(rec)

	fp, ok := out["failed_partitions"].(map[string]int)
	if !ok {
		t.Fatalf("failed_partitions missing or wrong type: %#v", out["failed_partitions"])
	}
	if fp["db_d/wide_table/2026/09/14/08"] != 19 || fp["db_d/wide_table/2026/09/21/16"] != 2 {
		t.Errorf("failed_partitions = %v, want the two partitions with counts 19 and 2", fp)
	}
	if got := out["failed_partition_count"]; got != 2 {
		t.Errorf("failed_partition_count = %v, want 2", got)
	}
	// Counts must reconcile with failed_batches -- the one exact rule.
	sum := 0
	for _, n := range fp {
		sum += n
	}
	if int64(sum) != rec.Failed {
		t.Errorf("sum(failed_partitions) = %d, want failed_batches = %d", sum, rec.Failed)
	}
	if _, present := out["failed_partitions_truncated"]; present {
		t.Error("truncation flagged on an untruncated record")
	}

	// Truncated records say so.
	rec.FailedPartitionsTruncated = true
	if got := cycleResponse(rec)["failed_partitions_truncated"]; got != true {
		t.Errorf("failed_partitions_truncated = %v, want true", got)
	}

	// A clean cycle carries neither key.
	clean := cycleResponse(compaction.CycleRecord{CycleID: 1, Status: "completed", StartedAt: time.Now(), FinishedAt: time.Now()})
	if _, present := clean["failed_partitions"]; present {
		t.Error("failed_partitions present on a cycle that failed nothing")
	}
	if _, present := clean["failed_partition_count"]; present {
		t.Error("failed_partition_count present on a cycle that failed nothing")
	}
}

// The LIST omits the full map but keeps the count: 500 retained cycles x 200
// partitions would be ~100,000 entries in one buffered response.
func TestListCyclesOmitsTheFullPartitionMapIssue1162(t *testing.T) {
	rec := compaction.CycleRecord{
		CycleID: 7, Status: "failed", StartedAt: time.Now(), FinishedAt: time.Now(),
		Failed:           3,
		FailedPartitions: map[string]int{"a": 1, "b": 2},
	}

	listed := cycleResponseWithPartitions(rec, false)
	if _, present := listed["failed_partitions"]; present {
		t.Error("the list embedded the full partition map; at 500 cycles that is megabytes per response")
	}
	if got := listed["failed_partition_count"]; got != 2 {
		t.Errorf("failed_partition_count = %v, want 2 kept on the list", got)
	}

	// The detail view still carries it.
	if _, present := cycleResponseWithPartitions(rec, true)["failed_partitions"]; !present {
		t.Error("the detail view dropped the partition map")
	}
}

// A cycle-filtered history response carries that cycle's authoritative
// counters. Those come from the cycle ring, which the api package CAN seed
// through the exported cycle entry points -- unlike job history, whose only
// writer spawns a compaction subprocess.
func TestHistoryCarriesTheCycleCrossCheckIssue1162(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend, CycleTimeout: time.Minute, Logger: zerolog.Nop(),
	})
	app := fiber.New()
	NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop()).RegisterRoutes(app)

	// A real, retained cycle (tier-less, so it completes immediately).
	cycleID, err := manager.RunCompactionCycleForTiers(t.Context(), nil)
	if err != nil {
		t.Fatalf("seed cycle: %v", err)
	}

	status, body := getJSON(t, app, fmt.Sprintf("/api/v1/compaction/history?cycle_id=%d", cycleID))
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %v)", status, body)
	}
	// The cycle IS retained, so the absence signal must be gone and the
	// authoritative counters present.
	if _, present := body["cycle_retained"]; present {
		t.Errorf("cycle_retained present for a retained cycle: %v", body["cycle_retained"])
	}
	if got := body["cycle_status"]; got != "completed" {
		t.Errorf("cycle_status = %v, want completed", got)
	}
	for _, key := range []string{"cycle_started_batches", "cycle_failed_batches"} {
		if _, present := body[key]; !present {
			t.Errorf("%s missing from a cycle-filtered response", key)
		}
	}

	// A different, unretained id still gets the absence signal plus the range.
	_, missing := getJSON(t, app, fmt.Sprintf("/api/v1/compaction/history?cycle_id=%d", cycleID+500))
	if got := missing["cycle_retained"]; got != false {
		t.Errorf("cycle_retained = %v, want false for an unretained id", got)
	}
	if got := missing["newest_retained_cycle"]; got != float64(cycleID) {
		t.Errorf("newest_retained_cycle = %v, want %d", got, cycleID)
	}
}
