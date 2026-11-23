package tiering

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// #1830: a migration whose budget ends mid-run must still release the hot
// copies of the files it migrated, and report what it left.

// ctxManifest refuses a context that has already ended, as the coordinator
// does before proposing (coordinator.go manifestContext + ctx.Err check).
type ctxManifest struct{ *fakeManifest }

func (c ctxManifest) DeleteFilesFromManifest(ctx context.Context, paths []string, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.fakeManifest.DeleteFilesFromManifest(ctx, paths, reason)
}

// endAfterWrite ends the migration's context as soon as a cold write
// completes: the copy finished, the budget did not.
type endAfterWrite struct {
	*coldLister
	end func()
}

func (e *endAfterWrite) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	err := e.coldLister.WriteReader(ctx, path, r, size)
	e.end()
	return err
}

func TestReleaseOutlivesTheMigrationBudget1830(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	gate.primary.Store(true)
	m.migrator.maxConcurrent = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.coldBackend = &endAfterWrite{coldLister: cold, end: cancel}
	fake := &fakeManifest{hot: hot, entries: map[string]int64{gateDailyA: 7, gateDailyB: 7}}
	m.manifest = ctxManifest{fake}
	for _, p := range []string{gateDailyA, gateDailyB} {
		mustWrite(t, hot, p)
		recordHotRow(t, m, p, manifestPartition)
	}

	b := m.migrator.migrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})

	if b.migrated != 1 || b.kept != 0 || b.notAttempted != 1 || b.failed != 0 || b.releaseErr != nil {
		t.Fatalf("batch = %+v, want 1 migrated and released, 1 not attempted, nothing kept or failed", b)
	}
	if ok, _ := hot.Exists(context.Background(), gateDailyA); ok {
		t.Fatal("the migrated file kept its hot copy: the release ran on the ended context")
	}
	if _, ok := fake.entries[gateDailyA]; ok {
		t.Fatal("the migrated file is still in the manifest")
	}
	if ok, _ := hot.Exists(context.Background(), gateDailyB); !ok {
		t.Fatal("the file not attempted lost its hot copy")
	}
	if _, ok := fake.entries[gateDailyB]; !ok {
		t.Fatal("the file not attempted left the manifest")
	}
}

// A manifest that genuinely fails still keeps the hot copies, and the batch
// says how many.
func TestKeptHotCopiesAreCounted1830(t *testing.T) {
	m, hot, _, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	gate.primary.Store(true)
	fake := &fakeManifest{hot: hot, entries: map[string]int64{gateDailyA: 7, gateDailyB: 7}, err: errors.New("raft: manifest apply failed")}
	m.manifest = fake
	for _, p := range []string{gateDailyA, gateDailyB} {
		mustWrite(t, hot, p)
		recordHotRow(t, m, p, manifestPartition)
	}
	b := m.migrator.migrateBatch(context.Background(), []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})
	if b.migrated != 2 || b.kept != 2 || b.releaseErr == nil {
		t.Fatalf("batch = %+v, want 2 migrated, 2 kept and the release error", b)
	}
	for _, p := range []string{gateDailyA, gateDailyB} {
		if ok, _ := hot.Exists(context.Background(), p); !ok {
			t.Fatalf("%s lost its hot copy although its manifest entry stayed", p)
		}
	}
}

// The release context ends releaseDrain after the migration's does, or at
// once when the manager stops.
func TestReleaseContextDrainBound1830(t *testing.T) {
	old := releaseDrain
	releaseDrain = 50 * time.Millisecond
	t.Cleanup(func() { releaseDrain = old })
	m, _, _, _, cleanup := setupGatedTest(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	rel, cancelRel := m.migrator.releaseContext(ctx)
	defer cancelRel()
	cancel()
	if rel.Err() != nil {
		t.Fatal("the release context ended with the migration's")
	}
	select {
	case <-rel.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the release context outlived its drain")
	}

	m.stopCh = make(chan struct{})
	rel2, cancel2 := m.migrator.releaseContext(context.Background())
	defer cancel2()
	close(m.stopCh)
	select {
	case <-rel2.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the release context outlived the manager")
	}
}

// Files kept by a failed release are finished by Reconcile: manifest entry
// and hot copy go, the cold copy stays.
func TestReconcileFinishesKeptCopies1830(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	gate.primary.Store(true)
	ctx := context.Background()
	failing := &fakeManifest{hot: hot, entries: map[string]int64{gateDailyA: 7}, err: errors.New("raft: manifest apply failed")}
	m.manifest = failing
	mustWrite(t, hot, gateDailyA)
	recordHotRow(t, m, gateDailyA, manifestPartition)
	if b := m.migrator.migrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA)}); b.kept != 1 {
		t.Fatalf("setup: batch = %+v, want the file kept", b)
	}

	working := &fakeManifest{hot: hot, entries: failing.entries}
	m.manifest = working
	r, err := m.Reconcile(ctx, 0)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if r.OrphansDeleted != 1 {
		t.Fatalf("result = %+v, want the kept copy deleted", r)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); ok {
		t.Fatal("hot copy still there")
	}
	if _, ok := working.entries[gateDailyA]; ok {
		t.Fatal("manifest entry still there")
	}
	if ok, _ := cold.Exists(ctx, gateDailyA); !ok {
		t.Fatal("the cold copy went too")
	}
	if len(working.calls) == 0 || !working.calls[0].hotPresent[gateDailyA] {
		t.Fatal("the manifest entry was not removed before the hot copy")
	}
}

func TestReconcileGates1830(t *testing.T) {
	m, _, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()

	gate.primary.Store(false)
	if _, err := m.Reconcile(ctx, 0); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("not primary: err = %v", err)
	}
	gate.primary.Store(true)

	m.cycleRunning.Store(true)
	if _, err := m.Reconcile(ctx, 0); !errors.Is(err, ErrMigrationCycleRunning) {
		t.Fatalf("cycle running: err = %v", err)
	}
	m.cycleRunning.Store(false)

	cold.listErr = errors.New("AccessDenied: list")
	if _, err := m.Reconcile(ctx, 0); !errors.Is(err, ErrColdSyncFailed) {
		t.Fatalf("cold listing failed: err = %v", err)
	}
	if m.cycleRunning.Load() {
		t.Fatal("Reconcile left the cycle guard held")
	}
}

// After a failover this node's row for a file the old primary migrated still
// says hot while the cold object exists. Reconcile syncs first, which flips
// the row; the orphan sweep then finds the hot copy when the copy is within
// the window, and window_hours reaches an older one.
func TestReconcileAfterFailoverSyncsFirst1830(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	gate.primary.Store(true)
	ctx := context.Background()
	cold.lastModified = time.Now().Add(-72 * time.Hour) // copied three days ago
	mustWrite(t, hot, gateDailyA)
	recordHotRow(t, m, gateDailyA, manifestPartition)
	mustWrite(t, cold.mockBackend, gateDailyA) // the old primary's cold copy
	fake := &fakeManifest{hot: hot, entries: map[string]int64{gateDailyA: 7}}
	m.manifest = fake

	// Default 48 h window: the row flips to cold and the settled manifest
	// entry goes (on a node-local cluster that unlinks every replica), but
	// the sweep does not reach a three-day-old copy.
	r, err := m.Reconcile(ctx, 0)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if r.ColdSynced != 1 || r.ManifestEntriesRemoved != 1 || r.OrphansDeleted != 0 {
		t.Fatalf("default window: %+v", r)
	}
	if f, _ := m.metadata.GetFile(ctx, gateDailyA); f == nil || f.Tier != TierCold {
		t.Fatalf("row after sync: %+v, want cold", f)
	}
	// A wider window reaches it.
	r, err = m.Reconcile(ctx, 100*time.Hour)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if r.OrphansDeleted != 1 {
		t.Fatalf("100 h window: %+v, want the hot copy deleted", r)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); ok {
		t.Fatal("hot copy still there")
	}
	if ok, _ := cold.Exists(ctx, gateDailyA); !ok {
		t.Fatal("the cold copy went too")
	}
}

// Batches stop once the budget has ended.
func TestMigrationStopsBatchingAtTheBudget1830(t *testing.T) {
	m, hot, _, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	gate.primary.Store(true)
	m.migrator.batchSize = 1
	for _, p := range []string{gateDailyA, gateDailyB, gateDailyC} {
		mustWrite(t, hot, p)
		recordHotRow(t, m, p, manifestPartition)
	}
	m.migrator.maxConcurrent = 1
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cold := m.coldBackend.(*coldLister)
	m.coldBackend = &endAfterWrite{coldLister: cold, end: cancel}
	r := m.migrator.MigrateTierFilteredResult(ctx, TierHot, TierCold, "", "")
	if r.Candidates != 3 {
		t.Fatalf("setup: %d candidates, want 3", r.Candidates)
	}
	if r.Migrated != 1 || r.Failed != 0 || r.NotAttempted != 2 || !r.TimedOut || r.Kept != 0 {
		t.Fatalf("result = %+v, want 1 migrated, 2 not attempted, timed out, none failed or kept", r)
	}
	if got := cold.writes.Load(); got != 1 {
		t.Fatalf("%d cold writes, want 1: a batch started after the budget ended", got)
	}
}

// abortWrite ends the migration's context during a cold write, which fails
// with it: the copy was cut off, not refused.
type abortWrite struct {
	*coldLister
	end func()
}

func (a *abortWrite) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	a.end()
	return ctx.Err()
}

// A copy the budget cut off is not attempted, not failed: the file stays
// hot with no cold row, and the answer must not report it as an error.
func TestCopyCutOffByTheBudgetIsNotAFailure1830(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	gate.primary.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.coldBackend = &abortWrite{coldLister: cold, end: cancel}
	mustWrite(t, hot, gateDailyA)
	recordHotRow(t, m, gateDailyA, manifestPartition)
	b := m.migrator.migrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA)})
	if b.notAttempted != 1 || b.failed != 0 || b.migrated != 0 {
		t.Fatalf("batch = %+v, want the cut-off copy counted as not attempted", b)
	}
	if f, _ := m.metadata.GetFile(context.Background(), gateDailyA); f == nil || f.Tier != TierHot {
		t.Fatalf("row = %+v, want it still hot", f)
	}
}
