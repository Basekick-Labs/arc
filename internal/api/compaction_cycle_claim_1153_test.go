package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// newClaimTestHandler builds a handler whose cycle can be held open, so the
// claim can be observed while it is held.
//
// A partition must exist on disk or the cycle never reaches the tier at all:
// listDatabases returns nothing and FindCandidates is not called.
func newClaimTestHandler(t *testing.T) (*compaction.Manager, *fiber.App, blockingTierIssue1152, func()) {
	t.Helper()
	root := t.TempDir()
	partition := filepath.Join(root, "rig", "cpu", "2026", "10", "08", "12")
	if err := os.MkdirAll(partition, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partition, "a.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })

	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend, CycleTimeout: time.Minute, Logger: zerolog.Nop(),
	})
	tier := blockingTierIssue1152{entered: make(chan struct{}, 1), release: make(chan struct{})}
	manager.Tiers = []compaction.Tier{tier}
	// One idempotent releaser, shared by the test and the cleanup: a test
	// that lets the cycle finish and the cleanup would otherwise both close
	// the channel.
	var once sync.Once
	releaseCycle := func() { once.Do(func() { close(tier.release) }) }
	t.Cleanup(releaseCycle)

	handler := NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)
	return manager, app, tier, releaseCycle
}

// Concurrent triggers produce exactly one winner.
//
// This is the regression test for #1153, and it is the only shape that
// distinguishes the fix. The obvious test — hold a cycle open, then trigger
// again and expect 409 — passes BEFORE the fix too, because waiting for the
// cycle to reach the tier means the CompareAndSwap has already run.
//
// The real defect is the window between the handler's old IsCycleRunning()
// read and that CAS, inside the goroutine. It is wide: the goroutine is
// scheduled lazily, so handlers routinely answered 200 before any goroutine
// claimed. Measured on the unfixed code, 3 to 16 of 32 concurrent triggers
// were accepted depending on machine and scheduler, with and without -race --
// never 1. Post-fix the claim is taken in the handler, so exactly one can win.
func TestCompactionTriggerAdmitsExactlyOneCycleIssue1153(t *testing.T) {
	_, app, tier, _ := newClaimTestHandler(t)

	const triggers = 32
	var wg sync.WaitGroup
	codes := make([]int, triggers)
	start := make(chan struct{})

	for i := 0; i < triggers; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			<-start // release them together, to overlap in the handler
			resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/api/v1/compaction/trigger", nil), -1)
			if err != nil {
				codes[slot] = -1
				return
			}
			resp.Body.Close()
			codes[slot] = resp.StatusCode
		}(i)
	}
	close(start)
	wg.Wait()

	accepted, conflicted, other := 0, 0, 0
	for _, code := range codes {
		switch code {
		case http.StatusOK:
			accepted++
		case http.StatusConflict:
			conflicted++
		default:
			other++
		}
	}
	if other != 0 {
		t.Fatalf("unexpected statuses: %v", codes)
	}
	if accepted != 1 {
		t.Fatalf("accepted = %d of %d, want exactly 1; every extra acceptance is a caller told it owns a cycle that another trigger is running", accepted, triggers)
	}
	if conflicted != triggers-1 {
		t.Fatalf("conflicted = %d, want %d", conflicted, triggers-1)
	}

	// Precondition, asserted last because the winner reaches the tier only
	// after it is admitted. The 409 count is deterministic ONLY because the
	// winning cycle is still held open in FindCandidates; if the fixture ever
	// stops reaching the tier, cycles finish in microseconds, the claim is
	// released between requests, and later triggers legitimately win. That
	// turns this into a flake whose message blames the fix.
	//
	// A BOUNDED WAIT, not the non-blocking select this originally used.
	// Post-#1153 the claim is taken synchronously in the handler, so the
	// accepted==1 assertion above already proves exclusivity by the time we
	// get here -- the winner's cycle goroutine, which is what reaches the
	// tier, is still scheduled lazily and routinely had not run yet. Measured
	// on this branch: 13 of 30 runs failed here against correct code, with a
	// message blaming exclusivity. The comment above predicted exactly that
	// and the original code polled anyway.
	select {
	case <-tier.entered:
	case <-time.After(30 * time.Second):
		t.Fatal("precondition failed: the winning cycle never reached the blocking tier, so these 409s do not demonstrate exclusivity")
	}
}

// The reported id is the id the cycle claimed, asserted with no waiting.
//
// This is a POST-FIX PIN, not a pre-fix detector: by the time app.Test returns
// the response, the old lazy goroutine had reliably claimed, so this passes
// against the unfixed handler too (measured 5/5). It pins that the id in the
// body, the manager's running id, and the id the cycle finally records are all
// the same number. The pre-fix detector is
// TestCompactionTriggerAdmitsExactlyOneCycleIssue1153.
func TestCompactionTriggerReportsTheClaimedIDIssue1153(t *testing.T) {
	manager, app, tier, releaseCycle := newClaimTestHandler(t)

	status, body := triggerCompactionFor(t, app)
	if status != http.StatusOK {
		t.Fatalf("status = %d (%v), want 200", status, body)
	}
	reported, ok := body["cycle_id"].(float64)
	if !ok {
		t.Fatalf("cycle_id missing or not a number: %v", body)
	}

	// No sleep: the claim is synchronous, so this must already hold.
	if got := manager.RunningCycleID(); got != int64(reported) {
		t.Fatalf("RunningCycleID() = %d immediately after the response, but the body reported %d; the id was not claimed before responding", got, int64(reported))
	}
	if !manager.IsCycleRunning() {
		t.Fatal("IsCycleRunning() is false immediately after a 200; the cycle was not claimed before responding")
	}

	// And the id the cycle finally records is the one the caller was given.
	select {
	case <-tier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the cycle never reached the tier")
	}
	releaseCycle()
	deadline := time.Now().Add(5 * time.Second)
	for manager.IsCycleRunning() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	stats := manager.Stats()
	last, _ := stats["last_cycle"].(map[string]interface{})
	if last == nil {
		t.Fatalf("no last_cycle in stats: %v", stats)
	}
	if last["cycle_id"] != int64(reported) {
		t.Fatalf("last_cycle.cycle_id = %v, want the reported %d; an operator matching on cycle_id would read the wrong cycle", last["cycle_id"], int64(reported))
	}
}

// The 409 names the running cycle, never 0.
//
// arcli maps any 409 on this route to "a compaction cycle is already running
// (cycle N)" reading only cycle_id, so a missing or stale id is what the
// operator is shown. RunningCycleID shares the claim's mutex, so it cannot
// report the previous finished cycle in the instant between a claim and its
// id assignment.
func TestCompactionTriggerConflictNamesTheRunningCycleIssue1153(t *testing.T) {
	manager, app, tier, _ := newClaimTestHandler(t)

	status, first := triggerCompactionFor(t, app)
	if status != http.StatusOK {
		t.Fatalf("first status = %d (%v), want 200", status, first)
	}
	select {
	case <-tier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the cycle never reached the tier")
	}

	status, second := triggerCompactionFor(t, app)
	if status != http.StatusConflict {
		t.Fatalf("second status = %d (%v), want 409", status, second)
	}
	if second["cycle_id"] == float64(0) || second["cycle_id"] == nil {
		t.Fatalf("409 cycle_id = %v; arcli prints this verbatim, so 0 or absent reads as \"cycle 0\"", second["cycle_id"])
	}
	if second["cycle_id"] != first["cycle_id"] {
		t.Fatalf("409 cycle_id = %v, want the running cycle %v", second["cycle_id"], first["cycle_id"])
	}
	if second["is_running"] != true {
		t.Fatalf("409 body = %v, want is_running true", second)
	}
	if got := manager.RunningCycleID(); got != int64(first["cycle_id"].(float64)) {
		t.Fatalf("RunningCycleID() = %d, want %v", got, first["cycle_id"])
	}
}

// Releasing a claim twice must not clear a later cycle's flag.
//
// A stale claim calling Release again after another cycle has taken the claim
// would set cycleRunning false under a running cycle — admitting a second
// concurrent cycle, which is worse than the bug CycleClaim fixes.
func TestCycleClaimReleaseIsIdempotentIssue1153(t *testing.T) {
	manager := compaction.NewManager(&compaction.ManagerConfig{CycleTimeout: time.Minute, Logger: zerolog.Nop()})

	first, err := manager.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	if !manager.IsCycleRunning() {
		t.Fatal("IsCycleRunning() false while a claim is held")
	}
	first.Release()
	if manager.IsCycleRunning() {
		t.Fatal("IsCycleRunning() true after the claim was released")
	}

	// A second claim, as a later cycle would take.
	second, err := manager.ClaimCycle()
	if err != nil {
		t.Fatal(err)
	}
	if second.ID <= first.ID {
		t.Fatalf("second claim id = %d, want greater than %d", second.ID, first.ID)
	}

	// The stale claim releases again; the live one must be unaffected.
	first.Release()
	if !manager.IsCycleRunning() {
		t.Fatal("a stale claim's second Release cleared cycleRunning under a live claim")
	}
	if _, err := manager.ClaimCycle(); err == nil {
		t.Fatal("ClaimCycle succeeded while another claim is held")
	}

	second.Release()
	if manager.IsCycleRunning() {
		t.Fatal("IsCycleRunning() true after the live claim was released")
	}
}
