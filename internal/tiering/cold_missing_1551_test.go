package tiering

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

// #1551: cold rows whose object is gone from cold storage are counted at the
// end of a cold sync, checked again at the moment of the check.

const (
	missA = "db1/cpu/2024/03/15/cpu_a_daily.parquet"
	missB = "db1/cpu/2024/03/15/cpu_b_daily.parquet"
)

// syncListing runs a cold sync whose listing returns paths.
func syncListing(t *testing.T, m *Manager, cold *mockBackend, paths ...string) {
	t.Helper()
	m.coldBackend = &listedColdObjects{mockBackend: cold, objects: coldObjects(paths...)}
	if _, err := m.syncColdTierMetadata(context.Background()); err != nil {
		t.Fatalf("sync: %v", err)
	}
}

func missingRig(t *testing.T) (*Manager, *mockBackend, *mockBackend) {
	t.Helper()
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	t.Cleanup(cleanup)
	t.Cleanup(func() { metrics.Get().SetTieringColdRowsMissingObjects(0, 0) })
	syncListing(t, m, cold, missA, missB) // records both cold rows
	return m, hot, cold
}

func TestColdMissingObjects_LostAndHotCopySplit(t *testing.T) {
	m, hot, cold := missingRig(t)
	if st := m.ColdMissingObjects(); st == nil || st.Lost != 0 || st.HotCopy != 0 {
		t.Fatalf("healthy store: %+v, want a zero result", st)
	}
	hot.seedRaw(missB, []byte("x")) // B still has a hot copy

	syncListing(t, m, cold) // both objects gone from cold
	st := m.ColdMissingObjects()
	if st == nil || st.Lost != 1 || st.HotCopy != 1 || !slices.Equal(st.Sample, []string{missA}) || st.CheckedAt.IsZero() {
		t.Fatalf("status = %+v, want lost 1 (A), hot copy 1", st)
	}
	snap := metrics.Get().Snapshot()
	if snap["tiering_cold_rows_missing_objects"] != int64(1) || snap["tiering_cold_rows_missing_objects_hot_copy"] != int64(1) {
		t.Fatalf("metrics = %v / %v, want 1 / 1", snap["tiering_cold_rows_missing_objects"], snap["tiering_cold_rows_missing_objects_hot_copy"])
	}

	// Restoring the object clears it on the next sync.
	cold.seedRaw(missA, []byte("x"))
	cold.seedRaw(missB, []byte("x"))
	syncListing(t, m, cold, missA, missB)
	if st := m.ColdMissingObjects(); st.Lost != 0 || st.HotCopy != 0 {
		t.Fatalf("after restoring the objects: %+v", st)
	}
	if metrics.Get().Snapshot()["tiering_cold_rows_missing_objects"] != int64(0) {
		t.Fatal("gauge not cleared")
	}
}

// A row retired since the listing is not counted.
func TestColdMissingObjects_RetiredRowIsNotCounted(t *testing.T) {
	m, _, cold := missingRig(t)
	if _, err := m.RetireColdRows(context.Background(), []string{missA, missB}); err != nil {
		t.Fatal(err)
	}
	if lost, _, ok := m.recheckColdRow(context.Background(), missA); lost || !ok {
		t.Fatalf("retired row: lost %v ok %v", lost, ok)
	}
	syncListing(t, m, cold)
	if st := m.ColdMissingObjects(); st.Lost != 0 {
		t.Fatalf("%+v", st)
	}
}

// A row no longer cold (reverted to hot) is not counted.
func TestColdMissingObjects_RevertedRowIsNotCounted(t *testing.T) {
	m, _, _ := missingRig(t)
	if err := m.RevertColdRowToHot(context.Background(), missA); err != nil {
		t.Fatal(err)
	}
	if lost, hotCopy, ok := m.recheckColdRow(context.Background(), missA); lost || hotCopy || !ok {
		t.Fatalf("reverted row: lost %v hot copy %v ok %v", lost, hotCopy, ok)
	}
}

// An object the listing missed but that exists now (written after the
// listing) is not counted.
func TestColdMissingObjects_ObjectWrittenAfterTheListing(t *testing.T) {
	m, _, cold := missingRig(t)
	cold.seedRaw(missA, []byte("x"))
	cold.seedRaw(missB, []byte("x"))
	syncListing(t, m, cold) // listing returns nothing, objects exist at check time
	if st := m.ColdMissingObjects(); st.Lost != 0 || st.HotCopy != 0 {
		t.Fatalf("%+v, want nothing counted", st)
	}
}

// A cluster reader keeps rows the primary deleted (#1563): nothing is
// reported there, and a node that becomes a reader drops its last result.
func TestColdMissingObjects_ReaderReportsNothing(t *testing.T) {
	m, _, cold := missingRig(t)
	syncListing(t, m, cold)
	if m.ColdMissingObjects() == nil {
		t.Fatal("primary/standalone: no result")
	}
	m.clusterGate = &mockGate{role: "reader"}
	syncListing(t, m, cold)
	if st := m.ColdMissingObjects(); st != nil {
		t.Fatalf("reader: %+v, want nil", st)
	}
	if metrics.Get().Snapshot()["tiering_cold_rows_missing_objects"] != int64(0) {
		t.Fatal("reader left the gauge set")
	}
}

// existsHook runs a callback inside the check's cold stat.
type existsHook struct {
	*listedColdObjects
	onExists func()
}

func (e *existsHook) Exists(ctx context.Context, path string) (bool, error) {
	if e.onExists != nil {
		e.onExists()
	}
	return e.listedColdObjects.Exists(ctx, path)
}

// A cold DELETE or retention that runs during the check, or is running when
// it starts, keeps the previous result.
func TestColdMissingObjects_RemovalOverlapKeepsThePreviousResult(t *testing.T) {
	m, _, cold := missingRig(t)
	prev := m.ColdMissingObjects()

	m.BeginColdRemoval()
	syncListing(t, m, cold)
	if m.ColdMissingObjects() != prev {
		t.Fatal("a check during a removal published a result")
	}
	m.EndColdRemoval()

	m.coldBackend = &existsHook{
		listedColdObjects: &listedColdObjects{mockBackend: cold},
		onExists:          func() { m.BeginColdRemoval(); m.EndColdRemoval() },
	}
	if _, err := m.syncColdTierMetadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	if m.ColdMissingObjects() != prev {
		t.Fatal("a removal that ran during the check did not keep the previous result")
	}

	syncListing(t, m, cold)
	if st := m.ColdMissingObjects(); st == prev || st.Lost != 2 {
		t.Fatalf("after the removals: %+v, want a fresh result with 2 lost", st)
	}
}

// Over the per-sync limit, the rest are reported unchecked.
func TestColdMissingObjects_CheckLimit(t *testing.T) {
	m, _, cold, cleanup := setupIntegrationTest(t, true)
	t.Cleanup(cleanup)
	t.Cleanup(func() { metrics.Get().SetTieringColdRowsMissingObjects(0, 0) })
	paths := make([]string, 0, coldMissingCheckLimit+5)
	for i := 0; i < coldMissingCheckLimit+5; i++ {
		paths = append(paths, fmt.Sprintf("db1/cpu/2024/03/15/cpu_%04d_daily.parquet", i))
	}
	syncListing(t, m, cold, paths...)
	syncListing(t, m, cold)
	st := m.ColdMissingObjects()
	if st.Lost != coldMissingCheckLimit || st.Unchecked != 5 {
		t.Fatalf("lost %d unchecked %d, want %d and 5", st.Lost, st.Unchecked, coldMissingCheckLimit)
	}
}

// A failed stat is unchecked, not lost.
func TestColdMissingObjects_FailedStatIsUnchecked(t *testing.T) {
	m, hot, cold := missingRig(t)
	m.hotBackend = &hotOnlyExistsErrorBackend{mockBackend: hot}
	syncListing(t, m, cold)
	if st := m.ColdMissingObjects(); st.Lost != 0 || st.Unchecked != 2 {
		t.Fatalf("%+v, want 2 unchecked", st)
	}
}

func TestColdMissingObjects_NilAndNoColdTier(t *testing.T) {
	var m *Manager
	if m.ColdMissingObjects() != nil {
		t.Fatal("nil manager")
	}
	m.BeginColdRemoval()
	m.EndColdRemoval()
}

// An incomplete check never lowers the lost count; a complete one does.
func TestColdMissingObjects_IncompleteCheckDoesNotClear(t *testing.T) {
	m, hot, cold := missingRig(t)
	syncListing(t, m, cold) // both lost
	if st := m.ColdMissingObjects(); st.Lost != 2 {
		t.Fatalf("%+v", st)
	}
	m.hotBackend = &hotOnlyExistsErrorBackend{mockBackend: hot}
	syncListing(t, m, cold) // every hot stat fails: 0 lost, 2 unchecked
	if st := m.ColdMissingObjects(); st.Lost != 2 || st.Unchecked != 0 {
		t.Fatalf("an incomplete check replaced the result: %+v", st)
	}
	if metrics.Get().Snapshot()["tiering_cold_rows_missing_objects"] != int64(2) {
		t.Fatal("an incomplete check cleared the gauge")
	}
	m.hotBackend = hot
	cold.seedRaw(missA, []byte("x"))
	cold.seedRaw(missB, []byte("x"))
	syncListing(t, m, cold, missA, missB)
	if st := m.ColdMissingObjects(); st.Lost != 0 {
		t.Fatalf("a complete check did not clear: %+v", st)
	}
}

// A check that started earlier never overwrites one that started later.
func TestColdMissingObjects_OlderCheckDoesNotOverwrite(t *testing.T) {
	m, _, _ := missingRig(t)
	now := time.Now()
	newer := &ColdMissingObjectsStatus{Lost: 3}
	if !m.publishColdMissing(newer, now) {
		t.Fatal("newer not published")
	}
	if m.publishColdMissing(&ColdMissingObjectsStatus{}, now.Add(-time.Second)) || m.ColdMissingObjects() != newer {
		t.Fatal("an older check overwrote a newer one")
	}
}

// A primary demoted to reader stops reporting at once, before its next sync.
func TestColdMissingObjects_DemotedPrimaryStopsReporting(t *testing.T) {
	m, _, cold := missingRig(t)
	syncListing(t, m, cold)
	gate := &mockGate{role: "writer"}
	gate.primary.Store(true)
	m.clusterGate = gate
	if m.ColdMissingObjects() == nil {
		t.Fatal("primary: no result")
	}
	gate.primary.Store(false)
	if m.ColdMissingObjects() != nil {
		t.Fatal("demoted primary still reports")
	}
}

// Database delete removes cold objects before their rows: bracketed.
func TestColdMissingObjects_DatabaseDeleteIsBracketed(t *testing.T) {
	m, _, cold := missingRig(t)
	cold.seedRaw(missA, []byte("x"))
	cold.seedRaw(missB, []byte("x"))
	var during []int32
	m.coldBackend = &deleteSpy{listedColdObjects: &listedColdObjects{mockBackend: cold}, onDelete: func() { during = append(during, m.coldRemovals.Load()) }}
	m.CleanupDatabaseDelete(context.Background(), "db1", nil, nil)
	if len(during) == 0 || during[0] != 1 || m.coldRemovals.Load() != 0 {
		t.Fatalf("removals during deletes %v, after %d: want 1 and 0", during, m.coldRemovals.Load())
	}
}

type deleteSpy struct {
	*listedColdObjects
	onDelete func()
}

func (d *deleteSpy) Delete(ctx context.Context, path string) error {
	d.onDelete()
	return d.listedColdObjects.Delete(ctx, path)
}

// dropTakenOver deletes the cold copy while the row is still cold: bracketed.
func TestColdMissingObjects_DropTakenOverIsBracketed(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	t.Cleanup(cleanup)
	m.manifest = &ownedManifest{fakeManifest: &fakeManifest{hot: hot, entries: map[string]int64{}}, ownedFrom: 1}
	cold.seedRaw(missA, []byte("x"))
	var during []int32
	m.coldBackend = &deleteSpy{listedColdObjects: &listedColdObjects{mockBackend: cold}, onDelete: func() { during = append(during, m.coldRemovals.Load()) }}
	if kept := m.migrator.dropTakenOver(context.Background(), []MigrationCandidate{candidateFor(missA)}); len(kept) != 0 {
		t.Fatalf("kept %v", kept)
	}
	if len(during) != 1 || during[0] != 1 || m.coldRemovals.Load() != 0 {
		t.Fatalf("removals during the delete %v, after %d: want [1] and 0", during, m.coldRemovals.Load())
	}
}
