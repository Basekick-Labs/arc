package tiering

// The cluster gate: only the primary writer migrates, but EVERY node keeps its
// tier metadata in sync by listing cold storage — standalone included since
// #1179 — because the query layer routes each node from its own metadata.
// These tests drive runCycle (RunMigrationCycle after the license check, which
// the in-package test Manager cannot pass) with a gate that flips between
// cycles, and one of them with no gate at all.

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// mockGate implements ClusterGate. primary flips between cycles to prove the
// check happens on every cycle, not once at construction.
type mockGate struct {
	primary atomic.Bool
	role    string
}

func (g *mockGate) IsPrimaryWriter() bool { return g.primary.Load() }
func (g *mockGate) Role() string          { return g.role }

// coldLister wraps the cold mockBackend to count listings and writes, and to
// control the LastModified the sync stamps migrated_at from (the base mock
// reports "now", which would hide a stamp of time.Now()).
type coldLister struct {
	*mockBackend
	lists        atomic.Int32
	writes       atomic.Int32
	lastModified time.Time
	// block, when set, holds every listing until it is closed — to keep a
	// cycle open. listErr, when set, fails every listing.
	block   chan struct{}
	listErr error
}

func (c *coldLister) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	c.lists.Add(1)
	if c.block != nil {
		<-c.block
	}
	if c.listErr != nil {
		return nil, c.listErr
	}
	objs, err := c.mockBackend.ListObjects(ctx, prefix)
	for i := range objs {
		objs[i].LastModified = c.lastModified
	}
	return objs, err
}

func (c *coldLister) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	c.writes.Add(1)
	return c.mockBackend.WriteReader(ctx, path, r, size)
}

const (
	gateDailyA = "db1/cpu/2024/03/15/cpu_20240315_daily.parquet"
	gateDailyB = "db1/cpu/2024/03/16/cpu_20240316_daily.parquet"
	gateDailyC = "db1/cpu/2024/03/17/cpu_20240317_daily.parquet"
)

var gateColdModTime = time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)

func setupGatedTest(t *testing.T) (*Manager, *mockBackend, *coldLister, *mockGate, func()) {
	t.Helper()
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	cl := &coldLister{mockBackend: cold, lastModified: gateColdModTime}
	m.coldBackend = cl
	gate := &mockGate{role: "writer"}
	m.clusterGate = gate
	return m, hot, cl, gate, cleanup
}

func mustWrite(t *testing.T, b storage.Backend, path string) {
	t.Helper()
	if err := b.Write(context.Background(), path, []byte("parquet")); err != nil {
		t.Fatal(err)
	}
}

func recordHotRow(t *testing.T, m *Manager, path string, partition time.Time) {
	t.Helper()
	err := m.metadata.RecordFile(context.Background(), &FileMetadata{
		Path: path, Database: "db1", Measurement: "cpu",
		PartitionTime: partition, Tier: TierHot, SizeBytes: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func fileMeta(t *testing.T, m *Manager, path string) *FileMetadata {
	t.Helper()
	meta, err := m.metadata.GetFile(context.Background(), path)
	if err != nil || meta == nil {
		t.Fatalf("GetFile(%s) = (%v, %v), want a row", path, meta, err)
	}
	return meta
}

func cacheGen(m *Manager) uint64 {
	m.metadata.tierCacheMu.Lock()
	defer m.metadata.tierCacheMu.Unlock()
	return m.metadata.tierCacheGen
}

func TestRunCycle_GatedNodeSyncsButNeverMigrates(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	mustWrite(t, hot, gateDailyA) // old enough to be a candidate
	gate.primary.Store(false)

	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("runCycle on a gated node = %v, want ErrMigrationRoleGated", err)
	}
	// The hot scan still ran: the node knows about the file.
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierHot {
		t.Fatalf("tier after gated cycle = %s, want hot (registered, not moved)", got)
	}
	// Nothing was moved or deleted.
	if ok, _ := cold.Exists(ctx, gateDailyA); ok {
		t.Fatal("gated node copied a file to cold")
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Fatal("gated node deleted a file from hot")
	}
	if n := cold.lists.Load(); n != 1 {
		t.Fatalf("cold listings = %d, want exactly 1 (the metadata sync)", n)
	}
}

func TestRunCycle_PrimaryMigrates(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	mustWrite(t, hot, gateDailyA)
	gate.primary.Store(true)

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle on the primary: %v", err)
	}
	if ok, _ := cold.Exists(ctx, gateDailyA); !ok {
		t.Fatal("primary did not migrate to cold")
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); ok {
		t.Fatal("primary left the source in hot")
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold", got)
	}
}

// A standalone node with a usable cold tier syncs metadata before migrating,
// even though it has no cluster gate.
func TestRunCycle_NilGateSyncsColdAndMigrates(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	m.clusterGate = nil
	mustWrite(t, hot, gateDailyA)

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle without a gate: %v", err)
	}
	if ok, _ := cold.Exists(ctx, gateDailyA); !ok {
		t.Fatal("ungated node did not migrate")
	}
	if n := cold.lists.Load(); n != 1 {
		t.Fatalf("cold listings = %d, want one metadata sync before migration", n)
	}
	if gated, role := m.MigrationGate(); gated || role != "" {
		t.Fatalf("MigrationGate without a gate = (%v, %q), want (false, \"\")", gated, role)
	}
}

// Leader changes must take effect on the next cycle, with no restart.
func TestRunCycle_GateCheckedEveryCycle(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()

	mustWrite(t, hot, gateDailyA)
	gate.primary.Store(true)
	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("cycle 1 (primary): %v", err)
	}
	if ok, _ := cold.Exists(ctx, gateDailyA); !ok {
		t.Fatal("cycle 1 did not migrate A")
	}

	mustWrite(t, hot, gateDailyB)
	gate.primary.Store(false)
	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("cycle 2 (demoted) = %v, want ErrMigrationRoleGated", err)
	}
	if ok, _ := cold.Exists(ctx, gateDailyB); ok {
		t.Fatal("cycle 2 migrated B while demoted")
	}

	gate.primary.Store(true)
	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("cycle 3 (re-elected): %v", err)
	}
	if ok, _ := cold.Exists(ctx, gateDailyB); !ok {
		t.Fatal("cycle 3 did not migrate B after re-election")
	}
}

func TestSchedulerStatus_RoleGated(t *testing.T) {
	m, _, _, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	s := NewScheduler(&SchedulerConfig{Manager: m, Logger: zerolog.Nop()})

	gate.primary.Store(false)
	if !s.Status().RoleGated {
		t.Fatal("role_gated = false on a non-primary, want true")
	}
	gate.primary.Store(true)
	if s.Status().RoleGated {
		t.Fatal("role_gated = true on the primary, want false")
	}
	m.clusterGate = nil
	if s.Status().RoleGated {
		t.Fatal("role_gated = true without a gate, want false")
	}
}

// A manual trigger on a gated node answers immediately: no listing, no
// registration. The scheduled cycle owns convergence.
func TestTriggerMigration_GatedRefusesWithoutWork(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	mustWrite(t, hot, gateDailyA)
	gate.primary.Store(false)

	if err := m.TriggerMigration(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("TriggerMigration on a gated node = %v, want ErrMigrationRoleGated", err)
	}
	if n := cold.lists.Load(); n != 0 {
		t.Fatalf("cold listings = %d, want 0 (manual trigger must not sync)", n)
	}
	rows, err := m.metadata.GetFilesInTier(ctx, TierHot)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("hot rows = %d, want 0 (manual trigger must not scan)", len(rows))
	}
	if gated, role := m.MigrationGate(); !gated || role != "writer" {
		t.Fatalf("MigrationGate = (%v, %q), want (true, writer)", gated, role)
	}
}

// The sync flips hot rows whose object is in cold, inserts rows for cold
// objects it never heard of, leaves rows that are already cold alone, and
// stamps migrated_at from the object rather than from the clock.
func TestSync_FlipsInsertsPreservesAndStamps(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(false)
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)

	// A: this node believes hot; the primary moved it (object in cold and,
	// as after a crash between copy and delete, still in hot).
	mustWrite(t, hot, gateDailyA)
	mustWrite(t, cold, gateDailyA)
	recordHotRow(t, m, gateDailyA, partition)
	// B: moved before this node ever saw it.
	mustWrite(t, cold, gateDailyB)
	// C: already known cold, with a migration time that must survive.
	earlier := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	mustWrite(t, cold, gateDailyC)
	if _, err := m.metadata.RecordColdFile(ctx, &FileMetadata{
		Path: gateDailyC, Database: "db1", Measurement: "cpu", PartitionTime: partition, SizeBytes: 7,
	}, earlier); err != nil {
		t.Fatal(err)
	}

	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("runCycle = %v, want ErrMigrationRoleGated", err)
	}

	for _, path := range []string{gateDailyA, gateDailyB} {
		meta := fileMeta(t, m, path)
		if meta.Tier != TierCold {
			t.Fatalf("%s tier = %s, want cold", path, meta.Tier)
		}
		if meta.MigratedAt == nil || !meta.MigratedAt.Equal(gateColdModTime) {
			t.Fatalf("%s migrated_at = %v, want the cold object's LastModified %v", path, meta.MigratedAt, gateColdModTime)
		}
	}
	if meta := fileMeta(t, m, gateDailyC); meta.MigratedAt == nil || !meta.MigratedAt.Equal(earlier) {
		t.Fatalf("already-cold row's migrated_at = %v, want preserved %v", meta.MigratedAt, earlier)
	}
	// A is in hot with a cold row: the hot scan must not downgrade it (#683),
	// and a gated node must not reconcile it away (that is the primary's job).
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Fatal("gated node deleted the orphan hot copy; reconciliation is leader-only")
	}

	// Steady state: nothing to change, so nothing is written — every write
	// would invalidate the tier cache a query-serving reader is using.
	before := cacheGen(m)
	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("second runCycle = %v", err)
	}
	if after := cacheGen(m); after != before {
		t.Fatalf("tier cache generation moved %d -> %d on a no-op sync", before, after)
	}
	if n := cold.writes.Load(); n != 0 {
		t.Fatalf("cold writes = %d, want 0 on a gated node", n)
	}
}

// A newly elected primary holds rows that say hot for files its predecessor
// moved. The sync flips them before candidate selection, so it neither
// errors on the missing source nor re-uploads the file.
func TestRunCycle_NewLeaderAdoptsPredecessorsMigrations(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	recordHotRow(t, m, gateDailyA, partition)
	mustWrite(t, cold, gateDailyA)
	gate.primary.Store(true)

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle as the new leader: %v", err)
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold (adopted)", got)
	}
	if n := cold.writes.Load(); n != 0 {
		t.Fatalf("cold writes = %d, want 0 (already migrated by the predecessor)", n)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); ok {
		t.Fatal("hot copy appeared out of nowhere")
	}
}

// A fresh node (empty metadata) learns that a measurement has cold data from
// one gated cycle, which is what puts the cold tier into its query routing.
func TestSync_FreshReaderRoutesToCold(t *testing.T) {
	m, _, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	mustWrite(t, cold, gateDailyA)
	gate.primary.Store(false)

	tiers, err := m.metadata.GetTiersForMeasurement(ctx, "db1", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if tiers[TierCold] {
		t.Fatal("precondition: fresh node already routes to cold")
	}
	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("runCycle = %v", err)
	}
	tiers, err = m.metadata.GetTiersForMeasurement(ctx, "db1", "cpu")
	if err != nil {
		t.Fatal(err)
	}
	if !tiers[TierCold] {
		t.Fatalf("tiers after one gated cycle = %v, want cold included", tiers)
	}
}

// The sync never reverts a cold row whose object is gone (#683 applies in
// this direction too); it reports it and moves on.
func TestSync_ColdRowWithoutObjectIsLeftAlone(t *testing.T) {
	m, _, _, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(false)
	if _, err := m.metadata.RecordColdFile(ctx, &FileMetadata{
		Path: gateDailyA, Database: "db1", Measurement: "cpu",
		PartitionTime: time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC), SizeBytes: 7,
	}, gateColdModTime); err != nil {
		t.Fatal(err)
	}

	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("runCycle = %v", err)
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierCold {
		t.Fatalf("tier = %s, want cold (never reverted)", got)
	}
}

// Objects the sync cannot place — reserved roots, non-parquet, paths that
// are not Arc's layout — are skipped without being recorded.
func TestSync_SkipsForeignObjects(t *testing.T) {
	m, _, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(false)
	for _, p := range []string{
		"_schema/db1/cpu/anchor.parquet",
		"warehouse/metadata/v1.metadata.json",
		"db1/cpu/notes.parquet",
		"db1/cpu/2024/03/xx/f_daily.parquet",
	} {
		cold.seedRaw(p, []byte("x"))
	}

	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("runCycle = %v", err)
	}
	rows, err := m.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("cold rows = %+v, want none recorded from foreign objects", rows)
	}
}

// Two cycles must never overlap on one node: the second one's sync could
// observe the first one's half-finished copy.
func TestRunCycle_RefusesOverlap(t *testing.T) {
	m, _, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(false)
	cold.block = make(chan struct{})

	first := make(chan error, 1)
	go func() { first <- m.runCycle(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for cold.lists.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("first cycle never reached the cold listing")
		}
		time.Sleep(time.Millisecond)
	}

	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationCycleRunning) {
		t.Fatalf("overlapping runCycle = %v, want ErrMigrationCycleRunning", err)
	}

	close(cold.block)
	if err := <-first; !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("first cycle = %v, want ErrMigrationRoleGated", err)
	}
	// The slot is released once the first cycle returns.
	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("cycle after release = %v, want ErrMigrationRoleGated", err)
	}
}

// cold_synced counts flips and inserts, and a gated cycle that changed rows
// invalidates the query caches exactly once; steady state stays quiet.
func TestScanTiers_ColdSyncedCountsAndNotifies(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(false)
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	fired := 0
	m.SetOnMigrationComplete(func() { fired++ })

	mustWrite(t, hot, gateDailyA)
	mustWrite(t, cold, gateDailyA)
	recordHotRow(t, m, gateDailyA, partition) // flip
	mustWrite(t, cold, gateDailyB)            // insert

	res, err := m.ScanTiers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.ColdSynced != 2 || res.ColdSyncFailed {
		t.Fatalf("ScanTiers = %+v, want cold_synced 2 (one flip, one insert) and no failure", res)
	}
	res, err = m.ScanTiers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.ColdSynced != 0 {
		t.Fatalf("steady-state cold_synced = %d, want 0", res.ColdSynced)
	}

	mustWrite(t, cold, gateDailyC)
	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("runCycle = %v", err)
	}
	if fired != 1 {
		t.Fatalf("cache invalidation fired %d times after a cycle that learned a cold file, want 1", fired)
	}
	if err := m.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("runCycle = %v", err)
	}
	if fired != 1 {
		t.Fatalf("cache invalidation fired %d times, want still 1 after a no-op cycle", fired)
	}
}

// A primary whose cold listing failed does not migrate that cycle: its rows
// for files other nodes moved are stale, and it would select them. Since
// #1179 an ungated node skips for the same reason — see
// TestRunCycle_StandaloneSkipsMigrationWhenColdSyncFails, where there are no
// other nodes and the stale view is this node's own.
func TestRunCycle_LeaderSkipsMigrationWhenColdListingFails(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	gate.primary.Store(true)
	cold.listErr = errors.New("cold tier unreachable")
	mustWrite(t, hot, gateDailyA)

	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("runCycle = %v, want nil (skipped, not failed)", err)
	}
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Fatal("file left hot storage although the cold tier could not be listed")
	}
	if n := cold.writes.Load(); n != 0 {
		t.Fatalf("cold writes = %d, want 0", n)
	}
	// The hot scan still ran.
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierHot {
		t.Fatalf("tier = %s, want hot", got)
	}
}

// A cold row the sync recorded from a listing that caught a copy the primary
// then rolled back points at nothing. Reconciliation must not delete the hot
// copy on such a row — it is the only copy — and it reverts the row to hot
// so the file is migrated again.
func TestReconcile_KeepsOnlyCopyWhenColdObjectIsGone(t *testing.T) {
	m, hot, cold, _, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	row := &FileMetadata{Path: gateDailyA, Database: "db1", Measurement: "cpu", PartitionTime: partition, SizeBytes: 7}

	mustWrite(t, hot, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, row, time.Now()); err != nil {
		t.Fatal(err)
	}
	found, deleted, failed := m.migrator.ReconcileOrphanedFiles(ctx)
	if ok, _ := hot.Exists(ctx, gateDailyA); !ok {
		t.Fatal("reconciliation deleted the only copy of the file")
	}
	if found != 1 || deleted != 0 || failed != 1 {
		t.Fatalf("reconcile = (found %d, deleted %d, failed %d), want (1, 0, 1)", found, deleted, failed)
	}
	if got := fileMeta(t, m, gateDailyA).Tier; got != TierHot {
		t.Fatalf("tier = %s, want hot (reverted so the primary migrates it again)", got)
	}

	// With the cold copy present the orphan hot copy is removed as before.
	mustWrite(t, cold, gateDailyA)
	if _, err := m.metadata.RecordColdFile(ctx, row, time.Now()); err != nil {
		t.Fatal(err)
	}
	found, deleted, failed = m.migrator.ReconcileOrphanedFiles(ctx)
	if ok, _ := hot.Exists(ctx, gateDailyA); ok {
		t.Fatal("orphan hot copy kept although the cold copy exists")
	}
	if found != 1 || deleted != 1 || failed != 0 {
		t.Fatalf("reconcile = (found %d, deleted %d, failed %d), want (1, 1, 0)", found, deleted, failed)
	}
}

// prefixedCold stores everything under a key prefix and lists keys with it
// stripped, the contract every object-store backend honours
// (internal/storage/s3.go ListObjects). Rows must carry the logical path.
type prefixedCold struct {
	*mockBackend
	prefix string
}

func (p *prefixedCold) key(path string) string { return p.prefix + path }
func (p *prefixedCold) Read(ctx context.Context, path string) ([]byte, error) {
	return p.mockBackend.Read(ctx, p.key(path))
}
func (p *prefixedCold) ReadTo(ctx context.Context, path string, w io.Writer) error {
	return p.mockBackend.ReadTo(ctx, p.key(path), w)
}
func (p *prefixedCold) Write(ctx context.Context, path string, data []byte) error {
	return p.mockBackend.Write(ctx, p.key(path), data)
}
func (p *prefixedCold) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	return p.mockBackend.WriteReader(ctx, p.key(path), r, size)
}
func (p *prefixedCold) Delete(ctx context.Context, path string) error {
	return p.mockBackend.Delete(ctx, p.key(path))
}
func (p *prefixedCold) Exists(ctx context.Context, path string) (bool, error) {
	return p.mockBackend.Exists(ctx, p.key(path))
}
func (p *prefixedCold) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	objs, err := p.mockBackend.ListObjects(ctx, p.key(prefix))
	for i := range objs {
		objs[i].Path = strings.TrimPrefix(objs[i].Path, p.prefix)
		objs[i].LastModified = gateColdModTime
	}
	return objs, err
}

func TestSync_ColdPrefixNeverReachesRows(t *testing.T) {
	m, hot, cold, gate, cleanup := setupGatedTest(t)
	defer cleanup()
	ctx := context.Background()
	pc := &prefixedCold{mockBackend: cold.mockBackend, prefix: "archive/"}
	m.coldBackend = pc

	// The primary migrates: the object lands under the prefix, the row does not.
	mustWrite(t, hot, gateDailyA)
	gate.primary.Store(true)
	if err := m.runCycle(ctx); err != nil {
		t.Fatalf("primary cycle: %v", err)
	}
	if ok, _ := cold.mockBackend.Exists(ctx, "archive/"+gateDailyA); !ok {
		t.Fatal("migrated object is not under the configured prefix")
	}
	if got := fileMeta(t, m, gateDailyA).Path; got != gateDailyA {
		t.Fatalf("row path = %q, want the logical path", got)
	}

	// A node that never migrated learns the same logical path from the sync.
	fresh, _, _, freshGate, freshCleanup := setupGatedTest(t)
	defer freshCleanup()
	fresh.coldBackend = pc
	freshGate.primary.Store(false)
	if err := fresh.runCycle(ctx); !errors.Is(err, ErrMigrationRoleGated) {
		t.Fatalf("fresh node cycle = %v", err)
	}
	rows, err := fresh.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Path != gateDailyA {
		t.Fatalf("fresh node cold rows = %+v, want exactly the logical path %q", rows, gateDailyA)
	}
}
