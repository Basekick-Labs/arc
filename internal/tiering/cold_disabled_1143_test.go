package tiering

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1143, and read the reachability note before judging the severity.
//
// Orphan reconciliation deletes a hot copy once it has confirmed the cold
// object exists, and it used to confirm that through a GetBackendForTier that
// ignored cold.enabled — while the query path omits the cold glob on exactly
// that flag. So on a node holding a cold BACKEND with the flag OFF it would
// delete the copy reads were being served from in favour of one nothing would
// read.
//
// THAT STATE IS NOT REACHABLE FROM CONFIGURATION TODAY. cmd/arc/main.go builds
// the cold backend inside "if cold.Enabled", nothing assigns Cold.Enabled after
// load, and there is no config reload — so a disabled cold tier means a NIL
// backend, which the sweep already handled by keeping the hot file. The
// asymmetry was a latent invariant violation, not a live data path, and this
// test pins the invariant so a refactor that moves construction out of that
// "if" cannot quietly make it live.
//
// What WAS live on a disabled node is the subject of
// TestMigrationCycleSkipsReconciliationWhenColdIsDisabled: the sweep ran every
// cycle and logged an error per orphan row for work it could never do.
func TestReconcileKeepsTheHotCopyWhenColdIsDisabled(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	row := &FileMetadata{Path: gateDailyA, Database: "db1", Measurement: "cpu", PartitionTime: partition, SizeBytes: 7}

	// The orphan: a row that says cold, with BOTH copies present. That is the
	// state in which the sweep deletes the hot one.
	mustWrite(t, hot, gateDailyA)
	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, row, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Precondition: with cold ENABLED this is exactly the case that deletes.
	// Asserted in TestReconcileStillDeletesOrphansWhenColdIsEnabled, so that a
	// change which simply stopped reconciling could not pass both.
	if !m.coldTierUsable() {
		t.Fatal("the rig does not start with a usable cold tier, so nothing below is the disabled case")
	}

	m.config.Cold.Enabled = false

	found, deleted, failed := m.migrator.ReconcileOrphanedFiles(ctx)
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Error("reconciliation deleted the hot copy from a node whose cold tier is disabled; it would be keeping only the cold copy, which the query path refuses to read because it omits the cold glob on that same flag")
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 with cold disabled", deleted)
	}
	// The row is left exactly as it was: flipping it to hot would make every
	// disabled window re-migrate the file when cold comes back.
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Errorf("tier = %s, want cold (untouched)", got)
	}
	t.Logf("direct call with cold disabled: found=%d deleted=%d failed=%d", found, deleted, failed)
}

// The other half of the pair: with cold enabled the sweep must still do its
// job. Without this, "fixed" could mean "reconciliation no longer runs".
func TestReconcileStillDeletesOrphansWhenColdIsEnabled(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	row := &FileMetadata{Path: gateDailyA, Database: "db1", Measurement: "cpu", PartitionTime: partition, SizeBytes: 7}

	mustWrite(t, hot, gateDailyA)
	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, row, time.Now()); err != nil {
		t.Fatal(err)
	}

	found, deleted, failed := m.migrator.ReconcileOrphanedFiles(ctx)
	if found != 1 || deleted != 1 || failed != 0 {
		t.Fatalf("reconcile = (found %d, deleted %d, failed %d), want (1, 1, 0)", found, deleted, failed)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); ok {
		t.Error("the orphan hot copy was kept although the cold copy exists and cold is enabled")
	}
}

// Step 4 of the fix, which a direct migrator call cannot see: the CYCLE skips
// the sweep entirely on a disabled cold tier, so a node does not log an error
// and count a failure per orphan every cycle for work it was never going to
// do. The accessor fix alone leaves the sweep safe but noisy.
func TestMigrationCycleSkipsReconciliationWhenColdIsDisabled(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	// The primary is the only node that reconciles, so a gated one would skip
	// the sweep for a reason that has nothing to do with this fix.
	gate.primary.Store(true)
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	row := &FileMetadata{Path: gateDailyA, Database: "db1", Measurement: "cpu", PartitionTime: partition, SizeBytes: 7}

	mustWrite(t, hot, gateDailyA)
	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, row, time.Now()); err != nil {
		t.Fatal(err)
	}
	m.config.Cold.Enabled = false

	// The ONLY thing this gate changes is whether the sweep runs at all: with
	// the accessor alone the sweep is already safe (its nil-backend branch
	// keeps the hot file), so asserting the file survived cannot distinguish
	// the two. What distinguishes them is the sweep's per-orphan error, which
	// a gated cycle never emits — so the log is the observable.
	// Reassigning m.logger is safe HERE only because this test enqueues no
	// tier events: the drainer goroutine NewManager started also reads that
	// field. Do not copy this into a test that calls RecordReplicatedFile or
	// RecordUnlinkedFile.
	var logs bytes.Buffer
	m.logger = zerolog.New(&logs)
	m.migrator.logger = m.logger

	// runCycle, not RunMigrationCycle: the in-package test Manager cannot pass
	// the license check the exported one does first (see the cluster_gate_test
	// header). Everything under test is in runCycle.
	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle with cold disabled: %v", err)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Error("a migration cycle with cold disabled deleted the hot copy")
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Errorf("tier = %s, want cold (untouched)", got)
	}
	if out := logs.String(); strings.Contains(out, "Cold backend not available") {
		t.Error("the cycle ran orphan reconciliation on a disabled cold tier: it reports an error per orphan, every cycle, for work it was never going to do")
	}
	if out := logs.String(); strings.Contains(out, "Orphaned hot file reconciliation completed") {
		t.Error("the cycle reported orphan reconciliation results on a disabled cold tier")
	}
}

// The manifest sweep is the second ungated call in that block, and it removes
// manifest entries on the strength of a cold object it stats through the same
// accessor. With cold disabled it must remove nothing.
//
// The row has to be genuinely SWEEPABLE for this to mean anything — settled
// migrated_at, in the manifest, cold object present at the recorded size — or
// the loop skips it long before the cold stat and the test passes against any
// mutation at all. It does with cold enabled, which is asserted first.
func TestReconcileManifestDoesNothingWhenColdIsDisabled(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	fake := &fakeManifest{hot: hot, entries: map[string]int64{}}
	m.manifest = fake
	settled := time.Now().Add(-2 * time.Hour)

	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(gateDailyA), settled); err != nil {
		t.Fatal(err)
	}
	fake.entries[gateDailyA] = 7

	rows, err := m.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		t.Fatal(err)
	}

	// Precondition: this row IS swept when cold is usable.
	removed, err := m.migrator.ReconcileManifest(ctx, rows)
	if err != nil {
		t.Fatalf("ReconcileManifest with cold enabled: %v", err)
	}
	if removed != 1 {
		t.Fatalf("removed %d with cold enabled, want 1 — the row is not sweepable, so the disabled case below proves nothing", removed)
	}

	// Same row, same manifest entry, cold disabled.
	fake.entries[gateDailyA] = 7
	if _, err := m.metadata.RecordColdFile(ctx, coldRowFor(gateDailyA), settled); err != nil {
		t.Fatal(err)
	}
	m.config.Cold.Enabled = false
	rows, err = m.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		t.Fatal(err)
	}
	removed, err = m.migrator.ReconcileManifest(ctx, rows)
	if err != nil {
		t.Fatalf("ReconcileManifest with cold disabled: %v", err)
	}
	if removed != 0 {
		t.Errorf("removed %d manifest entries with cold disabled, want 0", removed)
	}
}

// The cycle must still reconcile when cold IS usable. Without this, gating the
// sweep could be "fixed" by never reconciling at all — and the direct-call
// test cannot see a cycle-level change.
func TestMigrationCycleStillReconcilesWhenColdIsEnabled(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(true)
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	row := &FileMetadata{Path: gateDailyA, Database: "db1", Measurement: "cpu", PartitionTime: partition, SizeBytes: 7}

	mustWrite(t, hot, gateDailyA)
	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, row, time.Now()); err != nil {
		t.Fatal(err)
	}

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle: %v", err)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); ok {
		t.Error("the cycle did not reconcile the orphan hot copy although cold is enabled; gating the sweep must not switch it off")
	}
}

// The query router must route reads at exactly the tiers the writers consider
// usable, or a read is sent at a tier nothing maintains. Same predicate since
// #1143.
func TestQueryGlobsAndColdAccessorsAgree(t *testing.T) {
	m, _, _, _, cleanup := setupGatedTest(t)
	defer cleanup()
	router := NewRouter(m, m.logger)

	for _, enabled := range []bool{true, false} {
		m.config.Cold.Enabled = enabled
		paths := router.GetGlobPathsForQuery("db1", "cpu")
		_, hasCold := paths[TierCold]
		if hasCold != enabled {
			t.Errorf("cold glob present = %v with cold.enabled = %v", hasCold, enabled)
		}
		if (m.ColdBackend() != nil) != enabled {
			t.Errorf("ColdBackend non-nil = %v with cold.enabled = %v", m.ColdBackend() != nil, enabled)
		}
		if (m.GetBackendForTier(TierCold) != nil) != enabled {
			t.Errorf("GetBackendForTier non-nil = %v with cold.enabled = %v", m.GetBackendForTier(TierCold) != nil, enabled)
		}
		if hasCold != (m.GetBackendForTier(TierCold) != nil) {
			t.Error("the query glob and the cold accessor disagree; a read would be routed at a tier the writers will not touch")
		}
	}
}

// The two drainer paths dropped their own `!m.config.Cold.Enabled` halves when
// GetBackendForTier took on the flag (#1143), so this pins the behaviour those
// halves were there for: on a node with cold configured but DISABLED, a
// migration-reason unlink costs no cold probe and writes no cold row. A row
// saying cold here would make the measurement claim a tier the query path will
// not read.
func TestUnlinkWritesNoColdRowWhenColdIsDisabled(t *testing.T) {
	cold := newMockBackend("s3")
	const path = "db1/cpu/2026/10/03/a_daily.parquet"
	cold.seedRaw(path, []byte("cold copy"))

	// coldEnabled=false: the backend is configured, the flag is off.
	m := newTierEventManager(t, cold, false)
	m.RecordReplicatedFile(path, 9)
	m.RecordUnlinkedFile(path, "tiering:migrated", 9)
	waitTierEvents(t, m, 2)

	// NO COLD ROW is the whole claim, and no row at all satisfies it
	// legitimately: the file was unlinked, so nothing on this node claims it,
	// which is what actually happens here. Asserting "the row is hot" instead
	// would fail against correct behaviour — measured, not assumed.
	if tier, ok := rowTier(t, m, path); ok && tier == string(TierCold) {
		t.Error("a cold row was written with cold DISABLED; the query path omits the cold glob on that flag, so the measurement would claim a tier nothing reads")
	}
}

// M3 from review: every test above constructs the UNREACHABLE state — a
// Manager holding a cold backend with the flag off — because that is the
// invariant being pinned. This one covers the shape an operator actually runs:
// cold off means no backend at all, so coldBackend is nil.
//
// The pre-fix code reached the sweep in exactly this state and logged
// "Cold backend not available" once per orphan, every cycle. That is the
// defect, and this is the configuration it occurred in.
func TestMigrationCycleIsQuietWithNoColdBackendAtAll(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(true)
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	row := &FileMetadata{Path: gateDailyA, Database: "db1", Measurement: "cpu", PartitionTime: partition, SizeBytes: 7}

	// Seed the orphan while the cold backend is still there, then take it away
	// — the state cmd/arc/main.go produces when cold.enabled is false.
	mustWrite(t, hot, gateDailyA)
	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, row, time.Now()); err != nil {
		t.Fatal(err)
	}
	m.coldBackend = nil
	m.config.Cold.Enabled = false

	var logs bytes.Buffer
	m.logger = zerolog.New(&logs)
	m.migrator.logger = m.logger

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle with no cold backend: %v", err)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Error("the cycle deleted the hot copy on a node with no cold backend")
	}
	if out := logs.String(); strings.Contains(out, "Cold backend not available") {
		t.Error("the cycle ran orphan reconciliation with no cold backend: that is one error line per orphan, every cycle, for work it could never do")
	}
}
