package tiering

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
)

// newTierEventManager builds a Manager with a live tier-event drainer and the
// real metadata store, the way the package's other tests build one directly:
// NewManager needs a real license.Client, which a unit test has no way to
// produce.
func newTierEventManager(t *testing.T, cold *mockBackend, coldEnabled bool) *Manager {
	t.Helper()

	tmpFile, err := os.CreateTemp("", "tiering_replicated_test_*.db")
	if err != nil {
		t.Fatalf("temp file: %v", err)
	}
	tmpFile.Close()
	t.Cleanup(func() { os.Remove(tmpFile.Name()) })

	db, err := sql.Open("sqlite3", tmpFile.Name())
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	logger := zerolog.Nop()
	metadata, err := NewMetadataStore(db, logger)
	if err != nil {
		t.Fatalf("metadata store: %v", err)
	}

	cfg := &config.TieredStorageConfig{Enabled: true}
	cfg.Cold.Enabled = coldEnabled

	m := &Manager{
		metadata:      metadata,
		config:        cfg,
		logger:        logger,
		stopCh:        make(chan struct{}),
		tierEvents:    newTierEventQueue(),
		tierEventStop: make(chan struct{}),
	}
	if cold != nil {
		m.coldBackend = cold
	}
	m.tierEventWG.Add(1)
	go m.tierEventLoop()
	t.Cleanup(func() {
		m.tierEventStopOnce.Do(func() { close(m.tierEventStop) })
		m.tierEventWG.Wait()
	})
	return m
}

// waitTierEvents waits for the drainer to finish n events on its STEADY-STATE
// path. Deliberately not a Stop: the stop drain takes a short deadline and
// skips the cold-tier existence probe, so draining that way would quietly
// test the shutdown branch instead of the one that runs in service.
func waitTierEvents(t *testing.T, m *Manager, n int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if m.tierEventsProcessed.Load() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("drainer processed %d events, want %d", m.tierEventsProcessed.Load(), n)
}

// stopTierEventLoop drains through Stop's path, which is what Stop guarantees:
// everything already queued is applied before it returns.
func stopTierEventLoop(t *testing.T, m *Manager) {
	t.Helper()
	m.tierEventStopOnce.Do(func() { close(m.tierEventStop) })
	m.tierEventWG.Wait()
}

func rowTier(t *testing.T, m *Manager, path string) (string, bool) {
	t.Helper()
	var tier string
	err := m.metadata.db.QueryRow(`SELECT tier FROM tier_files WHERE path = ?`, path).Scan(&tier)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("read tier: %v", err)
	}
	return tier, true
}

func TestRecordReplicatedFile_RegistersHotRow(t *testing.T) {
	m := newTierEventManager(t, nil, false)

	m.RecordReplicatedFile("db1/cpu/2026/10/03/14/a.parquet", 4096)
	waitTierEvents(t, m, 1)

	tier, ok := rowTier(t, m, "db1/cpu/2026/10/03/14/a.parquet")
	if !ok {
		t.Fatal("no row written for a pulled file")
	}
	if tier != string(TierHot) {
		t.Fatalf("tier = %q, want hot", tier)
	}

	var database, measurement string
	var size int64
	var partition time.Time
	if err := m.metadata.db.QueryRow(
		`SELECT database, measurement, size_bytes, partition_time FROM tier_files WHERE path = ?`,
		"db1/cpu/2026/10/03/14/a.parquet",
	).Scan(&database, &measurement, &size, &partition); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if database != "db1" || measurement != "cpu" {
		t.Fatalf("row = %q/%q, want db1/cpu", database, measurement)
	}
	if size != 4096 {
		t.Fatalf("size_bytes = %d, want 4096", size)
	}
	if want := time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC); !partition.UTC().Equal(want) {
		t.Fatalf("partition_time = %v, want %v", partition.UTC(), want)
	}

	applied, dropped, failed := m.TierEventStats()
	if applied != 1 || dropped != 0 || failed != 0 {
		t.Fatalf("stats = applied %d dropped %d failed %d, want 1/0/0", applied, dropped, failed)
	}
}

func TestRecordReplicatedFile_NilManagerAndSkippedPaths(t *testing.T) {
	// The cluster layer holds this as an interface, and an interface holding a
	// typed nil is not == nil (#713).
	var nilManager *Manager
	nilManager.RecordReplicatedFile("db1/cpu/2026/10/03/14/a.parquet", 1)
	nilManager.RecordUnlinkedFile("db1/cpu/2026/10/03/14/a.parquet", "tiering:migrated", 1)
	if applied, dropped, failed := nilManager.TierEventStats(); applied|dropped|failed != 0 {
		t.Fatal("a nil manager reported stats")
	}

	m := newTierEventManager(t, nil, false)
	for _, path := range []string{
		"_schema/db1/cpu.parquet",            // reserved root: Arc's own state
		"db1/cpu/2026/10/03/14/a.txt",        // not parquet
		"db1/cpu/2026/10/file.parquet",       // unparseable: too short
		"db1/cpu/2026/10/xx/f_daily.parquet", // unparseable: non-numeric day
	} {
		m.RecordReplicatedFile(path, 10)
	}
	waitTierEvents(t, m, 4)

	var rows int
	if err := m.metadata.db.QueryRow(`SELECT count(*) FROM tier_files`).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("%d rows written for paths the scan would also skip", rows)
	}
}

// parseFilePath maps {spoke}/{db}/{meas}/... to database=spoke,
// measurement=db, because that is the split the query layer produces for a
// spoke-namespaced path. The row a pull writes has to match the row the scan
// writes, not the one the flush path would.
func TestRecordReplicatedFile_SpokeNamespacedFollowsScanConvention(t *testing.T) {
	m := newTierEventManager(t, nil, false)

	m.RecordReplicatedFile("edge1/db1/cpu/2026/10/03/14/a.parquet", 64)
	waitTierEvents(t, m, 1)

	var database, measurement string
	if err := m.metadata.db.QueryRow(
		`SELECT database, measurement FROM tier_files WHERE path = ?`,
		"edge1/db1/cpu/2026/10/03/14/a.parquet",
	).Scan(&database, &measurement); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if database != "edge1" || measurement != "db1" {
		t.Fatalf("row = %q/%q, want edge1/db1 (the scan's convention)", database, measurement)
	}
}

func TestRecordUnlinkedFile_MigratedWithColdObjectFlipsToCold(t *testing.T) {
	cold := newMockBackend("s3")
	const path = "db1/cpu/2026/10/03/a_daily.parquet"
	cold.seedRaw(path, []byte("cold copy"))

	m := newTierEventManager(t, cold, true)
	m.RecordReplicatedFile(path, 9)
	m.RecordUnlinkedFile(path, "tiering:migrated", 9)
	waitTierEvents(t, m, 2)

	tier, ok := rowTier(t, m, path)
	if !ok {
		t.Fatal("no row: the file is readable in cold and this node must route to it")
	}
	if tier != string(TierCold) {
		t.Fatalf("tier = %q, want cold", tier)
	}
}

// A dropped registration leaves no hot row. An update-only flip would match
// nothing, and the file — unlinked locally, readable only in cold — would be
// invisible on this node until the next cold sync.
func TestRecordUnlinkedFile_MigratedWithNoHotRowStillInsertsCold(t *testing.T) {
	cold := newMockBackend("s3")
	const path = "db1/cpu/2026/10/03/a_daily.parquet"
	cold.seedRaw(path, []byte("cold copy"))

	m := newTierEventManager(t, cold, true)
	m.RecordUnlinkedFile(path, "tiering:migrated", 9)
	waitTierEvents(t, m, 1)

	tier, ok := rowTier(t, m, path)
	if !ok {
		t.Fatal("no row written when the hot row was missing")
	}
	if tier != string(TierCold) {
		t.Fatalf("tier = %q, want cold", tier)
	}
}

// The operator manifest-delete endpoint passes an arbitrary ?reason=, so a
// migration-shaped reason is not proof of a migration. Trusting it would
// insert a cold row for a path with no cold object — and nothing retires such
// a row, so the measurement would lose its hot glob permanently.
func TestRecordUnlinkedFile_MigrationReasonWithoutColdObjectRetiresInstead(t *testing.T) {
	cold := newMockBackend("s3") // empty: no cold object for the path
	const path = "db1/cpu/2026/10/03/14/a.parquet"

	m := newTierEventManager(t, cold, true)
	m.RecordReplicatedFile(path, 9)
	m.RecordUnlinkedFile(path, "tiering:migrated", 9)
	waitTierEvents(t, m, 2)

	if tier, ok := rowTier(t, m, path); ok {
		t.Fatalf("a cold row was invented for a path with no cold object (tier = %q)", tier)
	}
}

func TestRecordUnlinkedFile_OtherReasonsRetireWithoutConsultingCold(t *testing.T) {
	cold := newMockBackend("s3")
	const path = "db1/cpu/2026/10/03/14/a.parquet"
	// Present in cold, to prove the branch does not look: these reasons mean
	// the file is gone, not moved.
	cold.seedRaw(path, []byte("should not be consulted"))

	for _, reason := range []string{
		"compaction:job-7",
		"retention:policy-2",
		"delete",
		"reconcile-orphan-manifest",
		"operator",
	} {
		m := newTierEventManager(t, cold, true)
		m.RecordReplicatedFile(path, 9)
		m.RecordUnlinkedFile(path, reason, 9)
		waitTierEvents(t, m, 2)

		if tier, ok := rowTier(t, m, path); ok {
			t.Fatalf("reason %q left a row (tier = %q); the hot row should be retired", reason, tier)
		}
	}
}

// A node with no cold route must not claim a file is readable there. Keyed on
// the backend that was actually constructed: a cold backend whose construction
// failed leaves cold.enabled true in config.
func TestRecordUnlinkedFile_NoColdBackendRetiresEvenWhenEnabled(t *testing.T) {
	const path = "db1/cpu/2026/10/03/14/a.parquet"

	m := newTierEventManager(t, nil, true) // enabled in config, no backend built
	m.RecordReplicatedFile(path, 9)
	m.RecordUnlinkedFile(path, "tiering:migrated", 9)
	waitTierEvents(t, m, 2)

	if tier, ok := rowTier(t, m, path); ok {
		t.Fatalf("row left at tier %q; a node that cannot read cold must retire the row", tier)
	}
}

// errExistsBackend answers every existence check with a failure, the way an
// unreachable cold endpoint does.
type errExistsBackend struct {
	*mockBackend
}

func (b *errExistsBackend) Exists(context.Context, string) (bool, error) {
	return false, errors.New("RequestTimeout: cold endpoint unreachable")
}

// An unreachable cold tier leaves the node unable to tell "moved" from "gone".
// It does know the local copy is gone, so the row must stop saying hot: a row
// contradicting the disk would otherwise stand until the next tier scan.
func TestRecordUnlinkedFile_ColdCheckFailureRetiresTheHotRow(t *testing.T) {
	const path = "db1/cpu/2026/10/03/14/a.parquet"
	cold := &errExistsBackend{mockBackend: newMockBackend("s3")}

	m := newTierEventManager(t, nil, true)
	m.coldBackend = cold

	m.RecordReplicatedFile(path, 9)
	m.RecordUnlinkedFile(path, "tiering:migrated", 9)
	waitTierEvents(t, m, 2)

	if tier, ok := rowTier(t, m, path); ok {
		t.Fatalf("row left at tier %q; an unreachable cold tier must not leave a hot row for a file that is gone", tier)
	}
}

// The queue's cap is a memory bound for a drainer that has stopped making
// progress, not a throughput knob: at the bound a report is dropped and
// counted rather than blocking the pull worker that is making it.
func TestTierEventQueueDropsOnlyAtItsMemoryBoundAndNeverBlocks(t *testing.T) {
	restore := tierEventQueueMax
	tierEventQueueMax = 2
	t.Cleanup(func() { tierEventQueueMax = restore })

	m := &Manager{
		logger:        zerolog.Nop(),
		tierEvents:    newTierEventQueue(),
		tierEventStop: make(chan struct{}),
	}

	// No drainer running, so the queue reaches its bound and the rest must be
	// dropped rather than block the pull worker that is reporting.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			m.RecordReplicatedFile("db1/cpu/2026/10/03/14/a.parquet", 1)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RecordReplicatedFile blocked on a full queue")
	}

	if _, dropped, _ := m.TierEventStats(); dropped != 48 {
		t.Fatalf("dropped = %d, want 48", dropped)
	}
	if got := m.tierEvents.pending(); got != 2 {
		t.Fatalf("pending = %d, want the 2 the bound allows", got)
	}
}

// Stop must join the drainer even when Start was refused — an expired license
// or an unparseable migration schedule — or the goroutine outlives the shared
// SQLite handle the caller closes next.
func TestStopJoinsDrainerWhenNeverStarted(t *testing.T) {
	m := newTierEventManager(t, nil, false)
	if m.running.Load() {
		t.Fatal("setup: manager should not be running")
	}

	m.RecordReplicatedFile("db1/cpu/2026/10/03/14/a.parquet", 32)

	done := make(chan error, 1)
	go func() { done <- m.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Stop() did not return: the drainer was not joined")
	}

	// Queued work is applied before Stop returns, so the caller can close the
	// database immediately afterwards.
	if _, ok := rowTier(t, m, "db1/cpu/2026/10/03/14/a.parquet"); !ok {
		t.Fatal("Stop() returned without applying what was already queued")
	}
}

func TestApplyTierEventBatch_InvalidatesOncePerMeasurement(t *testing.T) {
	m := newTierEventManager(t, nil, false)
	ctx := context.Background()

	// Prime the cache for two measurements so a bump is observable.
	if _, err := m.metadata.GetTiersForMeasurement(ctx, "db1", "cpu"); err != nil {
		t.Fatalf("GetTiersForMeasurement() error = %v", err)
	}
	if _, err := m.metadata.GetTiersForMeasurement(ctx, "db1", "mem"); err != nil {
		t.Fatalf("GetTiersForMeasurement() error = %v", err)
	}

	m.metadata.tierCacheMu.RLock()
	before := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	// Eight files across two measurements, applied as one batch.
	batch := []tierEvent{}
	for _, meas := range []string{"cpu", "mem"} {
		for _, hour := range []string{"10", "11", "12", "13"} {
			batch = append(batch, tierEvent{
				kind:      tierEventPulled,
				path:      "db1/" + meas + "/2026/10/03/" + hour + "/a.parquet",
				sizeBytes: 16,
			})
		}
	}
	m.applyTierEventBatch(batch)

	m.metadata.tierCacheMu.RLock()
	after := m.metadata.tierCacheGen
	m.metadata.tierCacheMu.RUnlock()

	// Per-row invalidation would bump eight times. The generation is global —
	// every bump makes concurrent readers discard their cache fills — so a
	// catch-up burst has to cost one bump per measurement, not one per file.
	if got := after - before; got != 2 {
		t.Fatalf("tierCacheGen advanced by %d for 8 files across 2 measurements, want 2", got)
	}

	if applied, _, _ := m.TierEventStats(); applied != 8 {
		t.Fatalf("applied = %d, want 8", applied)
	}
}

// The hot scan is a third writer of these rows, and it runs concurrently with
// the drainer now: every node scans on the migration schedule, and the
// primary proposes its tiering:migrated deletes on the same schedule. The
// scan's cold-path set is a snapshot taken before its walk, so a row the
// drainer flips to cold mid-walk is not in it — and an unconditional upsert
// would flip that row back to hot. Nothing reverts a cold row, and
// retireVanishedHotRows skips any path in the same stale listing, so the
// downgrade would stand until the next cold sync: the file is gone from this
// node's disk, the row says hot, and the measurement loses the cold tier from
// its reads.
func TestScanDoesNotDowngradeARowTheDrainerFlippedToCold(t *testing.T) {
	const path = "db1/cpu/2026/10/03/a_daily.parquet"
	cold := newMockBackend("s3")
	cold.seedRaw(path, []byte("cold copy"))

	m := newTierEventManager(t, cold, true)
	hot := newMockBackend("local")
	hot.seedRaw(path, []byte("hot copy"))
	m.hotBackend = hot

	ctx := context.Background()

	// The row starts hot, as a pull would have left it.
	if _, err := m.metadata.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 9)); err != nil {
		t.Fatalf("seed hot row: %v", err)
	}

	// Take the scan's snapshots, then let the drainer flip the row to cold
	// before the scan's walk reaches the path — the real interleaving, driven
	// deterministically.
	objects, err := hot.ListObjects(ctx, "")
	if err != nil {
		t.Fatalf("ListObjects: %v", err)
	}
	coldRows, err := m.metadata.GetFilesInTier(ctx, TierCold)
	if err != nil {
		t.Fatalf("GetFilesInTier: %v", err)
	}
	if len(coldRows) != 0 {
		t.Fatalf("setup: expected no cold rows yet, got %d", len(coldRows))
	}

	m.RecordUnlinkedFile(path, "tiering:migrated", 9)
	waitTierEvents(t, m, 1)
	if tier, _ := rowTier(t, m, path); tier != string(TierCold) {
		t.Fatalf("setup: drainer left tier %q, want cold", tier)
	}

	// Now the scan's walk reaches the path with its pre-flip snapshot.
	if _, err := m.ScanAndRegisterFiles(ctx); err != nil {
		t.Fatalf("ScanAndRegisterFiles: %v", err)
	}
	_ = objects

	tier, ok := rowTier(t, m, path)
	if !ok {
		t.Fatal("the scan removed the cold row")
	}
	if tier != string(TierCold) {
		t.Fatalf("tier = %q, want cold — the scan downgraded a row the drainer had flipped (#683)", tier)
	}
}

// retireVanishedHotRows decides from a snapshot of hot rows and a storage
// listing, then deletes. Between the two the drainer can register a freshly
// pulled file under a path the snapshot says is stale — and re-registering
// keeps the original created_at, so the row looks just as old. Without an age
// condition on the delete, the decision taken about the old row removes the
// new one, leaving a file on disk with no hot row and its measurement reading
// cold-only.
func TestRetireDoesNotDeleteARowRefreshedAfterTheSnapshot(t *testing.T) {
	const path = "db1/cpu/2026/10/03/14/a.parquet"
	m := newTierEventManager(t, nil, false)
	ctx := context.Background()

	// An old hot row for a file the listing will not return.
	old := replicatedTestFile(path, 9)
	old.CreatedAt = time.Now().UTC().Add(-48 * time.Hour)
	if _, err := m.metadata.recordHotFileIfNotCold(ctx, old); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// The file IS on disk now — it was just pulled back — but the retire
	// decision was taken from a listing that did not include it.
	hot := newMockBackend("local")
	hot.seedRaw(path, []byte("pulled back"))
	m.hotBackend = hot

	result := &ScanResult{}
	retired := m.retireVanishedHotRows(ctx, nil /* empty listing */, time.Now().UTC(), result)

	if _, ok := rowTier(t, m, path); !ok {
		t.Fatalf("the row for a file on disk was retired (retired=%d); the measurement now reads cold-only", retired)
	}
}

// Stop's drain writes rows but spends no network probe: it runs on the
// shutdown timeout that every remaining hook and component shares, so a
// migration-shaped unlink retires the hot row there rather than waiting on a
// cold-tier round trip. The cold row is recorded by the cold-tier metadata
// sync on the next boot.
//
// Drives applyTierEventBatch directly rather than through the loop: handing
// the event to the running loop races it, and whichever branch wins is not
// the test's to decide.
func TestStopDrainRetiresWithoutProbingCold(t *testing.T) {
	const path = "db1/cpu/2026/10/03/a_daily.parquet"
	cold := &countingExistsBackend{mockBackend: newMockBackend("s3")}
	cold.seedRaw(path, []byte("cold copy"))

	m := newTierEventManager(t, nil, true)
	stopTierEventLoop(t, m) // park the loop; this test calls the batch itself
	m.coldBackend = cold
	hot := newMockBackend("local")
	hot.seedRaw(path, []byte("hot copy"))
	m.hotBackend = hot

	ctx := context.Background()
	if _, err := m.metadata.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 9)); err != nil {
		t.Fatalf("seed hot row: %v", err)
	}

	m.draining.Store(true)
	m.applyTierEventBatch([]tierEvent{{
		kind:      tierEventUnlinked,
		path:      path,
		reason:    "tiering:migrated",
		sizeBytes: 9,
	}})

	if n := cold.calls.Load(); n != 0 {
		t.Fatalf("cold Exists called %d times during the stop drain; shutdown must not wait on the network", n)
	}
	if tier, ok := rowTier(t, m, path); ok {
		t.Fatalf("row left at tier %q; the stop drain should retire the hot row", tier)
	}

	// Control: the same event outside the stop drain does probe, and flips.
	m.draining.Store(false)
	if _, err := m.metadata.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 9)); err != nil {
		t.Fatalf("re-seed hot row: %v", err)
	}
	m.applyTierEventBatch([]tierEvent{{
		kind:      tierEventUnlinked,
		path:      path,
		reason:    "tiering:migrated",
		sizeBytes: 9,
	}})
	if n := cold.calls.Load(); n != 1 {
		t.Fatalf("control: cold Exists called %d times, want 1", n)
	}
	if tier, _ := rowTier(t, m, path); tier != string(TierCold) {
		t.Fatalf("control: tier = %q, want cold", tier)
	}
}

type countingExistsBackend struct {
	*mockBackend
	calls atomic.Int64
}

func (b *countingExistsBackend) Exists(ctx context.Context, path string) (bool, error) {
	b.calls.Add(1)
	return b.mockBackend.Exists(ctx, path)
}
