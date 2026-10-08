package tiering

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// --- the budget accessor ---

// TestScanBudgetFallsBackWithoutConfig: m.config is a pointer and nil is a
// documented supported shape, and a zero ScanTimeout would make
// context.WithTimeout return an already-expired context that truncates every
// scan on its first object. Both shapes appear throughout this package's tests.
func TestScanBudgetFallsBackWithoutConfig(t *testing.T) {
	for _, tc := range []struct {
		name string
		m    *Manager
	}{
		{"nil config", &Manager{}},
		{"zero ScanTimeout", &Manager{config: &config.TieredStorageConfig{}}},
		{"negative ScanTimeout", &Manager{config: &config.TieredStorageConfig{ScanTimeout: -time.Second}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.m.scanBudget(); got != defaultScanTimeout {
				t.Fatalf("scanBudget() = %s, want the %s fallback; a non-positive budget expires before the first object", got, defaultScanTimeout)
			}
			// The fallback must produce a context with time left on it.
			ctx, cancel := context.WithTimeout(context.Background(), tc.m.scanBudget())
			defer cancel()
			if err := ctx.Err(); err != nil {
				t.Fatalf("context from scanBudget() is already dead: %v", err)
			}
		})
	}
}

// TestDefaultScanBudgetMatchesTheConfigDefault pins the two numbers together.
// The comment on defaultScanTimeout claims it matches
// tiered_storage.scan_timeout rather than restating it, and nothing else makes
// that true: divergence is harmless in production, because config.Load always
// validates and sets the field, but the comment would become a lie silently.
func TestDefaultScanBudgetMatchesTheConfigDefault(t *testing.T) {
	t.Chdir(t.TempDir())
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TieredStorage.ScanTimeout != defaultScanTimeout {
		t.Fatalf("tiered_storage.scan_timeout defaults to %s but defaultScanTimeout is %s; the comment on defaultScanTimeout says they match",
			cfg.TieredStorage.ScanTimeout, defaultScanTimeout)
	}
}

func TestScanBudgetPrefersTheConfiguredValue(t *testing.T) {
	m := &Manager{config: &config.TieredStorageConfig{ScanTimeout: 7 * time.Minute}}
	if got := m.scanBudget(); got != 7*time.Minute {
		t.Fatalf("scanBudget() = %s, want 7m", got)
	}
	if got := m.ScanBudget(); got != 7*time.Minute {
		t.Fatalf("ScanBudget() = %s, want 7m; the API handler reads the budget through this", got)
	}
}

// --- truncation reporting ---

// TestTruncatedWalkReportsPartialCountsAndFlag covers the exit inside the
// object loop. scanTiers used to drop the partial result on the error path, so
// a walk over tens of thousands of files reported files_scanned 0 — the counts
// an operator needs to see how far it got.
//
// The cancel fires only once the walk has provably written a row, so the
// assertion on non-zero counts cannot pass by accident on a scan that never
// started.
func TestTruncatedWalkReportsPartialCountsAndFlag(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()

	ctx := context.Background()
	const files = 2000
	for i := 0; i < files; i++ {
		if err := hot.Write(ctx, scanFixtureKeyN(i), []byte("rows")); err != nil {
			t.Fatal(err)
		}
	}

	scanCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for {
			rows, err := m.metadata.GetFilesInTier(ctx, TierHot)
			if err == nil && len(rows) > 0 {
				cancel()
				return
			}
			select {
			case <-scanCtx.Done():
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	result, _, err := m.scanTiers(scanCtx)
	if result == nil {
		t.Fatal("the result is never nil, even on truncation")
	}
	if err == nil {
		t.Skipf("the walk finished all %d files before the cancel landed; nothing was truncated, so this run proves nothing", files)
	}

	// The counts are the point: they survive the error return.
	if result.FilesScanned == 0 {
		t.Error("FilesScanned = 0 after a walk that provably wrote a row; the partial result was discarded")
	}
	if result.FilesRegistered == 0 {
		t.Error("FilesRegistered = 0 after a walk that provably wrote a row; the partial result was discarded")
	}
	if result.FilesScanned >= files {
		t.Errorf("FilesScanned = %d of %d, so the scan was not actually truncated", result.FilesScanned, files)
	}
	if !result.Truncated {
		t.Error("Truncated is not set, so the caller cannot tell a short scan from a complete one")
	}
}

// TestTruncationInRetireTailIsReported covers the retire tail, which returns a
// COUNT and no error. Its first act is to load the hot rows, so a budget that
// is already gone fails there: without the flag the scan reports Errors++ and
// still returns nil, reading as a complete scan that had a hiccup rather than
// one that retired nothing.
func TestTruncationInRetireTailIsReported(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()

	dead, cancel := context.WithCancel(context.Background())
	cancel()

	result := &ScanResult{}
	if retired := m.retireVanishedHotRows(dead, nil, time.Now(), result); retired != 0 {
		t.Fatalf("retired = %d on a dead context, want 0", retired)
	}
	if !result.Truncated {
		t.Fatal("the retire tail returns no error of its own, so without this flag a truncated scan reports success")
	}
	if result.Errors == 0 {
		t.Error("precondition: the load must have failed, or this test is not on the path it names")
	}
}

// TestScanTiersCopiesHotRowsWritten pins a value every consumer read as zero:
// the migration-complete notifications and the startup cache invalidation all
// key on it.
func TestScanTiersCopiesHotRowsWritten(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := hot.Write(ctx, scanFixtureKey(i), []byte("rows")); err != nil {
			t.Fatal(err)
		}
	}

	result, _, err := m.scanTiers(ctx)
	if err != nil {
		t.Fatalf("scanTiers: %v", err)
	}
	if result.FilesRegistered == 0 {
		t.Fatal("precondition: the scan must have registered files, or HotRowsWritten would be legitimately zero")
	}
	if result.HotRowsWritten == 0 {
		t.Fatalf("HotRowsWritten = 0 after registering %d files; the caches keyed on it never invalidate", result.FilesRegistered)
	}
}

// TestNilErrorTruncationIsNotReportedAsComplete: the retirement pass returns a
// count and no error, so the scan reaches its own logging with err nil. Saying
// "File scan completed" there is the success-with-the-wrong-answer shape the
// issue is about -- the walk ran, but stale hot rows were left behind.
func TestNilErrorTruncationIsNotReportedAsComplete(t *testing.T) {
	for _, tc := range []struct {
		name      string
		truncated bool
		wantMsg   string
		wantLevel string
		absent    string
	}{
		{"complete", false, "File scan completed", "info", "ran out of budget"},
		{"truncated", true, "ran out of budget", "warn", "File scan completed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			m := &Manager{logger: zerolog.New(&logs)}
			m.logScanOutcome(&ScanResult{FilesScanned: 10, Truncated: tc.truncated})

			out := logs.String()
			if !strings.Contains(out, tc.wantMsg) {
				t.Errorf("log does not contain %q: %s", tc.wantMsg, out)
			}
			if strings.Contains(out, tc.absent) {
				t.Errorf("log should not contain %q: %s", tc.absent, out)
			}
			if !strings.Contains(out, `"level":"`+tc.wantLevel+`"`) {
				t.Errorf("log level is not %s: %s", tc.wantLevel, out)
			}
		})
	}
}

// --- the in-cycle child context ---

// blockingListBackend stalls ListObjects until the context it is given is
// done, which is what a storage root too large for the scan budget looks like
// from inside the walk.
type blockingListBackend struct {
	storage.Backend
	entered chan struct{}
	once    sync.Once
}

func (b *blockingListBackend) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestInCycleScanIsBoundedByItsOwnBudgetNotTheCycle drives the real runCycle.
//
// The scan budget is 20ms and the cycle budget is an hour, so if the scan ran
// on the cycle's context -- which is what the code did before #1154 -- this
// would block for an hour. The three assertions are the whole mechanism: the
// scan is bounded by its own budget, the CYCLE still has its time left
// afterwards (migration is the step that moves files), and the truncation is
// recorded.
func TestInCycleScanIsBoundedByItsOwnBudgetNotTheCycle(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()

	blocker := &blockingListBackend{Backend: hot, entered: make(chan struct{})}
	m.hotBackend = blocker
	m.config.ScanTimeout = 20 * time.Millisecond

	cycleCtx, cancelCycle := context.WithTimeout(context.Background(), time.Hour)
	defer cancelCycle()

	done := make(chan error, 1)
	go func() { done <- m.runCycle(cycleCtx) }()

	select {
	case <-blocker.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the scan never reached ListObjects")
	}

	select {
	case <-done:
		// runCycle returned, which is the point: the scan gave up on its own
		// budget rather than holding the cycle's.
	case <-time.After(5 * time.Second):
		t.Fatal("runCycle did not return within 5s on a 20ms scan budget; the scan is running on the CYCLE context, so it would block for the full hour")
	}

	// Migration keeps the remainder. This is the claim that makes the child
	// context the right shape rather than replacing the cycle budget.
	if cycleCtx.Err() != nil {
		t.Fatalf("the cycle context died with the scan (%v); migration would lose its remaining budget", cycleCtx.Err())
	}
	if deadline, ok := cycleCtx.Deadline(); !ok || time.Until(deadline) < 30*time.Minute {
		t.Fatalf("the cycle has %s left, want most of its hour", time.Until(deadline))
	}

	// And the truncation is observable afterwards.
	last, _ := m.LastScan()
	if last == nil {
		t.Fatal("the truncated in-cycle scan was not recorded")
	}
	if !last.Truncated {
		t.Error("the recorded scan is not marked truncated")
	}
}

// TestInCycleScanIsCappedByTheCycleWhenItIsShorter is the other direction: a
// scan budget above the cycle's must not extend the cycle.
func TestInCycleScanIsCappedByTheCycleWhenItIsShorter(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()

	blocker := &blockingListBackend{Backend: hot, entered: make(chan struct{})}
	m.hotBackend = blocker
	m.config.ScanTimeout = 4 * time.Hour

	cycleCtx, cancelCycle := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelCycle()

	done := make(chan error, 1)
	go func() { done <- m.runCycle(cycleCtx) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runCycle did not return; a 4h scan budget must still be capped by the 50ms cycle")
	}
	if cycleCtx.Err() == nil {
		t.Fatal("the cycle should be the expired one, which is how runCycle attributes the stop to the cycle rather than the scan budget")
	}
}

// --- serialisation ---

// TestConcurrentScanIsRefused: the endpoint has no handler deadline and
// fasthttp does not cancel on client disconnect, so an operator who gives up
// and retries would otherwise start a second full scan over the same storage
// root against the same SQLite handle. The latch is driven directly rather
// than by blocking a real scan, so the contract is asserted deterministically.
func TestConcurrentScanIsRefused(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()
	if err := hot.Write(context.Background(), scanFixtureKey(0), []byte("rows")); err != nil {
		t.Fatal(err)
	}

	// Stand in for a scan already in flight.
	if !m.scanRunning.CompareAndSwap(false, true) {
		t.Fatal("precondition: the latch should start clear")
	}
	result, err := m.ScanTiers(context.Background())
	if !errors.Is(err, ErrScanRunning) {
		t.Fatalf("ScanTiers while one is running returned %v, want ErrScanRunning", err)
	}
	if result == nil {
		t.Error("the result is never nil, even when refused")
	}

	// Releasing the latch lets the next caller through, and the latch is
	// cleared again afterwards rather than staying latched.
	m.scanRunning.Store(false)
	if _, err := m.ScanTiers(context.Background()); err != nil {
		t.Fatalf("ScanTiers after the latch cleared: %v", err)
	}
	if m.scanRunning.Load() {
		t.Error("ScanTiers left the latch set; every later scan would be refused")
	}
}

// TestInCycleScanIsNotRefusedByARunningApiScan: the CAS guards the exported
// ScanTiers only. Guarding the inner scanTiers would make an 02:00 cycle that
// lands on a running API scan skip its own scan and migrate on stale rows.
func TestInCycleScanIsNotRefusedByARunningApiScan(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()
	if err := hot.Write(context.Background(), scanFixtureKey(0), []byte("rows")); err != nil {
		t.Fatal(err)
	}

	if !m.scanRunning.CompareAndSwap(false, true) {
		t.Fatal("precondition: the latch should start clear")
	}
	defer m.scanRunning.Store(false)

	if _, _, err := m.scanTiers(context.Background()); errors.Is(err, ErrScanRunning) {
		t.Fatal("the in-cycle scan was refused while an API scan held the latch; the cycle would migrate on stale rows")
	}
}

// --- the recorded last scan ---

func TestLastScanIsRecordedIncludingTruncation(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()
	if err := hot.Write(context.Background(), scanFixtureKey(0), []byte("rows")); err != nil {
		t.Fatal(err)
	}

	if last, _ := m.LastScan(); last != nil {
		t.Fatal("precondition: nothing recorded before the first scan")
	}

	if _, _, err := m.scanTiers(context.Background()); err != nil {
		t.Fatal(err)
	}
	last, at := m.LastScan()
	if last == nil {
		t.Fatal("a completed scan was not recorded; the startup scan has no other way to be observed")
	}
	if at.IsZero() {
		t.Error("LastScanAt is zero")
	}
	if last.Truncated {
		t.Error("a complete scan is marked truncated")
	}

	// The stored value is a COPY of what the scan returned. The caller keeps
	// that pointer and hands it to HTTP responses and callbacks, so storing
	// it rather than a copy would let any later annotation rewrite what this
	// node reports as its last scan.
	scanned, _, err := m.scanTiers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	before, _ := m.LastScan()
	scanned.FilesRegistered = 999999
	after, _ := m.LastScan()
	if after.FilesRegistered == 999999 {
		t.Error("recordScan stored the caller's pointer; mutating the returned result rewrote recorded history")
	}
	if after.FilesRegistered != before.FilesRegistered {
		t.Errorf("recorded FilesRegistered changed from %d to %d without a new scan", before.FilesRegistered, after.FilesRegistered)
	}

	// Truncation is recorded too — it is the case the field exists for.
	dead, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)
	if _, _, err := m.scanTiers(dead); err == nil {
		t.Fatal("expected the expired scan to fail")
	}
	if truncatedLast, _ := m.LastScan(); truncatedLast == nil || !truncatedLast.Truncated {
		t.Fatalf("a truncated scan was not recorded as truncated: %+v", truncatedLast)
	}
}

func TestLastScanIsRaceFree(t *testing.T) {
	m, hot, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()
	if err := hot.Write(context.Background(), scanFixtureKey(0), []byte("rows")); err != nil {
		t.Fatal(err)
	}

	// Four callers can scan concurrently while the status handler reads.
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _, _ = m.scanTiers(context.Background())
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_, _ = m.LastScan()
			}
		}()
	}
	wg.Wait()
}

func scanFixtureKey(i int) string {
	return "db/cpu/2026/01/02/03/f" + strings.Repeat("x", i) + ".parquet"
}

func scanFixtureKeyN(i int) string {
	return "db/cpu/2026/01/02/03/f" + strconv.Itoa(i) + ".parquet"
}
