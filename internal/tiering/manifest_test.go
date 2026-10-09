package tiering

// Tiering and the cluster file manifest: every hot copy tiering removes
// leaves the manifest first, in bounded, paced chunks; a manifest failure
// keeps the hot copies; and the sweep removes entries for files already in
// cold only once they are settled and verified.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
)

type manifestCall struct {
	paths      []string
	reason     string
	hotPresent map[string]bool // the call's own paths, as seen in hot storage at call time
	probeInHot bool            // fakeManifest.probe, as seen at call time
	at         time.Time
}

// fakeManifest records each proposal together with what hot storage held
// at that moment, so tests can assert manifest-before-storage rather than
// take it on trust.
type fakeManifest struct {
	hot        storage.Backend
	entries    map[string]int64
	calls      []manifestCall
	err        error
	failOnCall int // 1-based: fail exactly this call
	probe      string
	seq        *[]string
	byDatabase map[string][]string
}

func (f *fakeManifest) DeleteFilesFromManifest(ctx context.Context, paths []string, reason string) error {
	call := manifestCall{paths: append([]string(nil), paths...), reason: reason, hotPresent: map[string]bool{}, at: time.Now()}
	for _, p := range paths {
		ok, _ := f.hot.Exists(ctx, p)
		call.hotPresent[p] = ok
	}
	if f.probe != "" {
		call.probeInHot, _ = f.hot.Exists(ctx, f.probe)
	}
	f.calls = append(f.calls, call)
	if f.seq != nil {
		*f.seq = append(*f.seq, "manifest")
	}
	if f.err != nil {
		return f.err
	}
	if f.failOnCall > 0 && len(f.calls) == f.failOnCall {
		return errors.New("raft: manifest apply failed")
	}
	for _, p := range paths {
		delete(f.entries, p)
	}
	return nil
}

func (f *fakeManifest) ManifestEntry(path string) (int64, bool) {
	n, ok := f.entries[path]
	return n, ok
}

// ManifestEntriesByDatabase models the real FSM, which indexes entries by the
// manifest entry's Database FIELD (ClusterFSM.filesByDB), NOT by the first
// path segment. byDatabase lets a test register the mismatch edge-sync
// actually produces -- Path "{spoke}/{db}/..." under Database "{db}" -- which
// a path-prefix fake cannot express, and which is what made the unfiltered
// union delete a spoke's files.
func (f *fakeManifest) ManifestEntriesByDatabase(database string) []string {
	if f.byDatabase != nil {
		return append([]string(nil), f.byDatabase[database]...)
	}
	var paths []string
	for path := range f.entries {
		if strings.HasPrefix(path, database+"/") {
			paths = append(paths, path)
		}
	}
	return paths
}

var manifestPartition = time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)

func candidateFor(path string) MigrationCandidate {
	return MigrationCandidate{
		Path: path, Database: "db1", Measurement: "cpu", PartitionTime: manifestPartition,
		SizeBytes: 7, CurrentTier: TierHot, TargetTier: TierCold,
	}
}

func coldRowFor(path string) *FileMetadata {
	return &FileMetadata{Path: path, Database: "db1", Measurement: "cpu", PartitionTime: manifestPartition, SizeBytes: 7}
}

func TestMigrateBatch_ManifestBeforeHotDelete(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	fake := &fakeManifest{hot: hot, entries: map[string]int64{}}
	m.manifest = fake
	for _, p := range []string{gateDailyA, gateDailyB} {
		mustWrite(t, hot, p)
		recordHotRow(t, m, p, manifestPartition)
		fake.entries[p] = 7
	}

	migrated, failed := m.migrator.MigrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})
	if migrated != 2 || failed != 0 {
		t.Fatalf("MigrateBatch = (%d migrated, %d failed), want (2, 0)", migrated, failed)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("manifest proposals = %d, want exactly one for the batch", len(fake.calls))
	}
	call := fake.calls[0]
	if call.reason != manifestReasonMigrated || len(call.paths) != 2 {
		t.Fatalf("proposal = %+v, want both paths with reason %q", call, manifestReasonMigrated)
	}
	for _, p := range call.paths {
		if !call.hotPresent[p] {
			t.Fatalf("%s was deleted from hot before the manifest proposal", p)
		}
	}
	for _, p := range []string{gateDailyA, gateDailyB} {
		if ok, _ := hot.Exists(ctx, p); ok {
			t.Fatalf("%s still in hot after the batch", p)
		}
		if ok, _ := cold.Exists(ctx, p); !ok {
			t.Fatalf("%s missing from cold", p)
		}
		if got := fileMeta(t, m, p).Tier; got != TierCold {
			t.Fatalf("%s tier = %s, want cold", p, got)
		}
		if _, still := fake.entries[p]; still {
			t.Fatalf("%s still in the manifest", p)
		}
	}
}

// A manifest failure after the copies leaves every hot copy in place: the
// files are migrated (cold is canonical, rows say cold) and reconciliation
// finishes the cleanup. It counts once for the cycle.
func TestMigrateBatch_ManifestFailureKeepsHotCopies(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	fake := &fakeManifest{hot: hot, entries: map[string]int64{}, err: errors.New("raft: manifest apply failed")}
	m.manifest = fake
	for _, p := range []string{gateDailyA, gateDailyB} {
		mustWrite(t, hot, p)
		recordHotRow(t, m, p, manifestPartition)
		fake.entries[p] = 7
	}

	migrated, failed := m.migrator.MigrateBatch(ctx, []MigrationCandidate{candidateFor(gateDailyA), candidateFor(gateDailyB)})
	if migrated != 2 || failed != 1 {
		t.Fatalf("MigrateBatch = (%d migrated, %d failed), want (2, 1)", migrated, failed)
	}
	for _, p := range []string{gateDailyA, gateDailyB} {
		if ok, _ := hot.Exists(ctx, p); !ok {
			t.Fatalf("%s deleted from hot although the manifest proposal failed", p)
		}
		if ok, _ := cold.Exists(ctx, p); !ok {
			t.Fatalf("%s missing from cold", p)
		}
		if got := fileMeta(t, m, p).Tier; got != TierCold {
			t.Fatalf("%s tier = %s, want cold", p, got)
		}
	}
}

func TestMigrateFile_ManifestBeforeHotDelete(t *testing.T) {
	m, hot, _, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	fake := &fakeManifest{hot: hot, entries: map[string]int64{gateDailyA: 7}}
	m.manifest = fake
	mustWrite(t, hot, gateDailyA)
	recordHotRow(t, m, gateDailyA, manifestPartition)

	if err := m.migrator.MigrateFile(ctx, candidateFor(gateDailyA)); err != nil {
		t.Fatalf("MigrateFile: %v", err)
	}
	if len(fake.calls) != 1 || !fake.calls[0].hotPresent[gateDailyA] {
		t.Fatalf("calls = %+v, want one proposal made while the hot copy still existed", fake.calls)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); ok {
		t.Fatal("hot copy still present")
	}
}

// Every node applies a proposal synchronously and queues its unlinks into a
// bounded queue, so proposals stay small and spaced out; each chunk's hot
// copies go before the next proposal.
func TestReleaseHotCopies_ChunksAndPaces(t *testing.T) {
	m, hot, _, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	const n = 2*manifestChunk + 50
	files := make([]MigrationCandidate, n)
	for i := range files {
		p := fmt.Sprintf("db1/cpu/2024/03/15/cpu_%03d_daily.parquet", i)
		mustWrite(t, hot, p)
		files[i] = candidateFor(p)
	}
	fake := &fakeManifest{hot: hot, entries: map[string]int64{}, probe: files[0].Path}
	m.manifest = fake

	if err := m.migrator.releaseHotCopies(ctx, files, manifestReasonMigrated); err != nil {
		t.Fatalf("releaseHotCopies: %v", err)
	}
	if len(fake.calls) != 3 {
		t.Fatalf("proposals = %d, want 3 (%d, %d, 50)", len(fake.calls), manifestChunk, manifestChunk)
	}
	if got := []int{len(fake.calls[0].paths), len(fake.calls[1].paths), len(fake.calls[2].paths)}; got[0] != manifestChunk || got[1] != manifestChunk || got[2] != 50 {
		t.Fatalf("chunk sizes = %v, want [%d %d 50]", got, manifestChunk, manifestChunk)
	}
	if !fake.calls[0].probeInHot || fake.calls[1].probeInHot {
		t.Fatalf("first chunk's hot copies must be gone before the second proposal (probe in hot: %v then %v)",
			fake.calls[0].probeInHot, fake.calls[1].probeInHot)
	}
	if gap := fake.calls[1].at.Sub(fake.calls[0].at); gap < manifestChunkPause {
		t.Fatalf("proposals %v apart, want at least %v", gap, manifestChunkPause)
	}
	for _, c := range files {
		if ok, _ := hot.Exists(ctx, c.Path); ok {
			t.Fatalf("%s still in hot", c.Path)
		}
	}
}

// A manifest failure part-way through a batch keeps every later chunk's hot
// copies: chunk 1 is released, chunks 2–3 wait for reconciliation.
func TestReleaseHotCopies_PartialFailureKeepsLaterChunks(t *testing.T) {
	m, hot, _, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	const n = 2*manifestChunk + 50
	files := make([]MigrationCandidate, n)
	for i := range files {
		p := fmt.Sprintf("db1/cpu/2024/03/15/cpu_%03d_daily.parquet", i)
		mustWrite(t, hot, p)
		files[i] = candidateFor(p)
	}
	fake := &fakeManifest{hot: hot, entries: map[string]int64{}, failOnCall: 2}
	m.manifest = fake

	if err := m.migrator.releaseHotCopies(ctx, files, manifestReasonMigrated); err == nil {
		t.Fatal("releaseHotCopies succeeded although the second proposal failed")
	}
	if len(fake.calls) != 2 {
		t.Fatalf("proposals = %d, want 2 (the batch stops at the failing chunk)", len(fake.calls))
	}
	for i, c := range files {
		ok, _ := hot.Exists(ctx, c.Path)
		if i < manifestChunk && ok {
			t.Fatalf("%s (chunk 1) still in hot after its proposal succeeded", c.Path)
		}
		if i >= manifestChunk && !ok {
			t.Fatalf("%s (chunk %d) deleted from hot although its proposal never succeeded", c.Path, i/manifestChunk+1)
		}
	}
}

// The primary's cycle sweeps a settled pre-fix cold row through scanTiers →
// ReconcileManifest, and leaves a row the sync just recorded alone.
func TestRunCycle_SweepsSettledRowNotJustListedOne(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(true)
	fake := &fakeManifest{hot: hot, entries: map[string]int64{gateDailyA: 7, gateDailyB: 7}}
	m.manifest = fake

	// A: migrated by this node before the manifest was kept in step.
	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(gateDailyA), time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// B: first seen by this cycle's cold listing, stamped with the object's
	// (recent) timestamp.
	cold.lastModified = time.Now()
	mustWrite(t, cold, gateDailyB)

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}
	var swept []string
	for _, c := range fake.calls {
		if c.reason == manifestReasonSweep {
			swept = append(swept, c.paths...)
		}
	}
	if len(swept) != 1 || swept[0] != gateDailyA {
		t.Fatalf("swept = %v, want exactly the settled pre-fix row %s", swept, gateDailyA)
	}
	if _, still := fake.entries[gateDailyB]; !still {
		t.Fatal("the just-listed row was swept before it settled")
	}
	if got := fileMeta(t, m, gateDailyB).Tier; got != TierCold {
		t.Fatalf("B tier = %s, want cold (recorded by the sync)", got)
	}
}

// Reconciliation marks receipts for every orphan first, then removes them
// from the manifest, then from hot; a manifest failure keeps the hot copies.
func TestReconcileOrphans_ReceiptsThenManifestThenHot(t *testing.T) {
	for _, tc := range []struct {
		name        string
		manifestErr error
	}{
		{"manifest ok", nil},
		{"manifest fails", errors.New("raft: manifest apply failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, hot, cold, _, cleanup := setupGatedTest(t)
			defer cleanup()
			ctx := context.Background()
			var seq []string
			m.SetOnHotFilesRemoved(func(paths []string) error {
				seq = append(seq, fmt.Sprintf("receipts:%d", len(paths)))
				return nil
			})
			fake := &fakeManifest{hot: hot, entries: map[string]int64{}, err: tc.manifestErr, seq: &seq}
			m.manifest = fake
			for _, p := range []string{gateDailyA, gateDailyB} {
				mustWrite(t, hot, p)
				mustWrite(t, cold, p)
				if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(p), time.Now()); err != nil {
					t.Fatal(err)
				}
				fake.entries[p] = 7
			}

			found, deleted, failed := m.migrator.ReconcileOrphanedFiles(ctx)
			if found != 2 {
				t.Fatalf("orphans found = %d, want 2", found)
			}
			if len(seq) != 2 || seq[0] != "receipts:2" || seq[1] != "manifest" {
				t.Fatalf("order = %v, want receipts for both, then one manifest proposal", seq)
			}
			for _, p := range fake.calls[0].paths {
				if !fake.calls[0].hotPresent[p] {
					t.Fatalf("%s deleted from hot before the manifest proposal", p)
				}
			}
			if tc.manifestErr == nil {
				if deleted != 2 || failed != 0 {
					t.Fatalf("deleted=%d failed=%d, want 2, 0", deleted, failed)
				}
				for _, p := range []string{gateDailyA, gateDailyB} {
					if ok, _ := hot.Exists(ctx, p); ok {
						t.Fatalf("%s still in hot", p)
					}
				}
			} else {
				if deleted != 0 || failed != 2 {
					t.Fatalf("deleted=%d failed=%d, want 0, 2", deleted, failed)
				}
				for _, p := range []string{gateDailyA, gateDailyB} {
					if ok, _ := hot.Exists(ctx, p); !ok {
						t.Fatalf("%s deleted from hot although the manifest proposal failed", p)
					}
				}
			}
		})
	}
}

// The sweep removes manifest entries only for settled cold rows whose cold
// object exists with the recorded size.
func TestReconcileManifest_SweepsOnlySettledVerifiedRows(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	fake := &fakeManifest{hot: hot, entries: map[string]int64{}}
	m.manifest = fake
	settled := time.Now().Add(-2 * time.Hour)
	const (
		swept       = "db1/cpu/2024/03/15/cpu_swept_daily.parquet"
		wrongSize   = "db1/cpu/2024/03/15/cpu_wrongsize_daily.parquet"
		recent      = "db1/cpu/2024/03/15/cpu_recent_daily.parquet"
		notInMan    = "db1/cpu/2024/03/15/cpu_notinmanifest_daily.parquet"
		noObject    = "db1/cpu/2024/03/15/cpu_noobject_daily.parquet"
		quarantined = "db1/cpu/2024/03/15/cpu_quarantined_daily.parquet"
	)
	for _, p := range []string{swept, wrongSize, recent, notInMan, quarantined} {
		mustWrite(t, cold, p)
	}
	for _, p := range []string{swept, wrongSize, notInMan, noObject, quarantined} {
		if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(p), settled); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(recent), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := m.metadata.QuarantineFile(ctx, quarantined, quarantineReasonInvalidPath); err != nil {
		t.Fatal(err)
	}
	fake.entries[swept] = 7
	fake.entries[wrongSize] = 99
	fake.entries[recent] = 7
	fake.entries[noObject] = 7
	fake.entries[quarantined] = 7

	rows, err := m.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := m.migrator.ReconcileManifest(ctx, rows)
	if err != nil {
		t.Fatalf("ReconcileManifest: %v", err)
	}
	if removed != 1 || len(fake.calls) != 1 || len(fake.calls[0].paths) != 1 || fake.calls[0].paths[0] != swept {
		t.Fatalf("removed=%d calls=%+v, want exactly %s swept", removed, fake.calls, swept)
	}
	if fake.calls[0].reason != manifestReasonSweep {
		t.Fatalf("reason = %q, want %q", fake.calls[0].reason, manifestReasonSweep)
	}
	for _, kept := range []string{wrongSize, recent, noObject, quarantined} {
		if _, ok := fake.entries[kept]; !ok {
			t.Fatalf("%s was removed from the manifest", kept)
		}
	}

	m.manifest = nil
	if removed, err := m.migrator.ReconcileManifest(ctx, rows); err != nil || removed != 0 || len(fake.calls) != 1 {
		t.Fatalf("without a manifest: removed=%d err=%v calls=%d, want nothing", removed, err, len(fake.calls))
	}
}

// A copy that cannot be flipped leaves its cold object in place: another
// node may already have recorded it, and deleting it could leave no copy.
func TestCopyAndFlip_KeepsColdObjectWhenFlipFails(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	mustWrite(t, hot, gateDailyA)
	recordHotRow(t, m, gateDailyA, manifestPartition)
	// Every metadata write fails from here on, including the tier flip.
	_ = m.metadata.db.Close()

	if err := m.migrator.copyAndFlip(ctx, candidateFor(gateDailyA)); err == nil {
		t.Fatal("copyAndFlip succeeded with a closed metadata store")
	}
	if ok, _ := cold.Exists(ctx, gateDailyA); !ok {
		t.Fatal("the cold copy was deleted on rollback")
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Fatal("the hot copy is gone")
	}
}

func TestRetryTransient(t *testing.T) {
	transient := errors.New("no leader")
	permanent := errors.New("bad payload")
	isTransient := func(err error) bool { return errors.Is(err, transient) }

	calls := 0
	err := RetryTransient(context.Background(), 3, time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return transient
		}
		return nil
	}, isTransient)
	if err != nil || calls != 3 {
		t.Fatalf("transient twice then ok: err=%v calls=%d, want nil after 3", err, calls)
	}

	calls = 0
	err = RetryTransient(context.Background(), 3, time.Millisecond, func() error { calls++; return permanent }, isTransient)
	if !errors.Is(err, permanent) || calls != 1 {
		t.Fatalf("permanent: err=%v calls=%d, want the error after 1 call", err, calls)
	}

	calls = 0
	err = RetryTransient(context.Background(), 3, time.Millisecond, func() error { calls++; return transient }, isTransient)
	if !errors.Is(err, transient) || calls != 3 {
		t.Fatalf("always transient: err=%v calls=%d, want the error after 3 calls", err, calls)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls = 0
	err = RetryTransient(ctx, 3, time.Hour, func() error { calls++; return transient }, isTransient)
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancelled ctx: err=%v calls=%d, want the context error after 1 call with no wait", err, calls)
	}
}
