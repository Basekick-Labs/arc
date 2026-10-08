package tiering

import (
	"context"
	"sync"
	"testing"
	"time"
)

func replicatedTestFile(path string, size int64) *FileMetadata {
	return &FileMetadata{
		Path:          path,
		Database:      "testdb",
		Measurement:   "cpu",
		PartitionTime: time.Date(2026, 10, 3, 14, 0, 0, 0, time.UTC),
		SizeBytes:     size,
		CreatedAt:     time.Date(2026, 10, 3, 15, 0, 0, 0, time.UTC),
	}
}

func tierOf(t *testing.T, store *MetadataStore, path string) string {
	t.Helper()
	var tier string
	if err := store.db.QueryRow(`SELECT tier FROM tier_files WHERE path = ?`, path).Scan(&tier); err != nil {
		t.Fatalf("read tier for %s: %v", path, err)
	}
	return tier
}

func TestRecordHotFileIfNotCold_InsertsWhenAbsent(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	wrote, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile("testdb/cpu/2026/10/03/14/a.parquet", 100))
	if err != nil {
		t.Fatalf("recordHotFileIfNotCold() error = %v", err)
	}
	if !wrote {
		t.Fatal("recordHotFileIfNotCold() reported no write for a path with no row")
	}
	if got := tierOf(t, store, "testdb/cpu/2026/10/03/14/a.parquet"); got != string(TierHot) {
		t.Fatalf("tier = %q, want hot", got)
	}
}

// The reason this helper exists instead of RecordFile: RecordFile's upsert sets
// tier = excluded.tier unconditionally, so registering a replicated file with
// it would drag a migrated file back to hot (#683).
func TestRecordHotFileIfNotCold_LeavesColdRowCold(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "testdb/cpu/2026/10/03/14/a.parquet"

	cold := replicatedTestFile(path, 100)
	cold.Tier = TierCold
	if err := store.RecordFile(ctx, cold); err != nil {
		t.Fatalf("RecordFile() setup error = %v", err)
	}

	wrote, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 200))
	if err != nil {
		t.Fatalf("recordHotFileIfNotCold() error = %v", err)
	}
	if wrote {
		t.Fatal("recordHotFileIfNotCold() wrote over a cold row")
	}
	if got := tierOf(t, store, path); got != string(TierCold) {
		t.Fatalf("tier = %q, want cold — the row was downgraded", got)
	}

	// Control: plain RecordFile is what this helper exists to avoid.
	hot := replicatedTestFile(path, 200)
	hot.Tier = TierHot
	if err := store.RecordFile(ctx, hot); err != nil {
		t.Fatalf("RecordFile() error = %v", err)
	}
	if got := tierOf(t, store, path); got != string(TierHot) {
		t.Fatalf("control: RecordFile left tier = %q, expected it to downgrade to hot", got)
	}
}

func TestRecordHotFileIfNotCold_LeavesQuarantinedRowAlone(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "testdb/cpu/2026/10/03/14/a.parquet"

	if _, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 100)); err != nil {
		t.Fatalf("setup error = %v", err)
	}
	if err := store.QuarantineFile(ctx, path, "unusable key"); err != nil {
		t.Fatalf("QuarantineFile() error = %v", err)
	}

	wrote, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 999))
	if err != nil {
		t.Fatalf("recordHotFileIfNotCold() error = %v", err)
	}
	if wrote {
		t.Fatal("recordHotFileIfNotCold() wrote to a quarantined row")
	}
	var size int64
	if err := store.db.QueryRow(`SELECT size_bytes FROM tier_files WHERE path = ?`, path).Scan(&size); err != nil {
		t.Fatalf("read size: %v", err)
	}
	if size != 100 {
		t.Fatalf("size_bytes = %d, want 100 — the quarantined row was modified", size)
	}
}

// The drainer only invalidates the tier cache for events that wrote, so a
// reconciliation walk over files this node already holds must report false.
func TestRecordHotFileIfNotCold_IdenticalRerecordReportsNoWrite(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "testdb/cpu/2026/10/03/14/a.parquet"

	if _, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 100)); err != nil {
		t.Fatalf("setup error = %v", err)
	}

	wrote, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 100))
	if err != nil {
		t.Fatalf("recordHotFileIfNotCold() error = %v", err)
	}
	if wrote {
		t.Fatal("an identical re-record reported a write; the WHERE clause on the DO UPDATE is not suppressing it")
	}

	wrote, err = store.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 250))
	if err != nil {
		t.Fatalf("recordHotFileIfNotCold() error = %v", err)
	}
	if !wrote {
		t.Fatal("a changed size reported no write")
	}
	var size int64
	if err := store.db.QueryRow(`SELECT size_bytes FROM tier_files WHERE path = ?`, path).Scan(&size); err != nil {
		t.Fatalf("read size: %v", err)
	}
	if size != 250 {
		t.Fatalf("size_bytes = %d, want 250", size)
	}
}

func TestMarkFileCold_InsertsWhenAbsent(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "testdb/cpu/2026/10/03/14/a.parquet"

	// The hot row was never written — a registration dropped under load. An
	// update-only flip would leave the file with no row at all: unlinked
	// locally and absent from this node's cold reads.
	wrote, err := store.markFileCold(ctx, replicatedTestFile(path, 100))
	if err != nil {
		t.Fatalf("markFileCold() error = %v", err)
	}
	if !wrote {
		t.Fatal("markFileCold() reported no write for a path with no row")
	}
	if got := tierOf(t, store, path); got != string(TierCold) {
		t.Fatalf("tier = %q, want cold", got)
	}
}

func TestMarkFileCold_FlipsHotAndLeavesMigratedAtNull(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "testdb/cpu/2026/10/03/14/a.parquet"

	if _, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 100)); err != nil {
		t.Fatalf("setup error = %v", err)
	}

	wrote, err := store.markFileCold(ctx, replicatedTestFile(path, 100))
	if err != nil {
		t.Fatalf("markFileCold() error = %v", err)
	}
	if !wrote {
		t.Fatal("markFileCold() reported no write for a hot row")
	}
	if got := tierOf(t, store, path); got != string(TierCold) {
		t.Fatalf("tier = %q, want cold", got)
	}

	// A stamp here would put every unlinked path into orphan reconciliation's
	// recently-migrated window, which HEADs each row. The cold sync supplies
	// the real stamp from the object's own timestamp.
	var migratedAt *time.Time
	if err := store.db.QueryRow(`SELECT migrated_at FROM tier_files WHERE path = ?`, path).Scan(&migratedAt); err != nil {
		t.Fatalf("read migrated_at: %v", err)
	}
	if migratedAt != nil {
		t.Fatalf("migrated_at = %v, want NULL", migratedAt)
	}
}

func TestMarkFileCold_NoOpOnColdAndQuarantined(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	// Already cold, with a migrated_at the cold sync stamped: the flip must
	// not overwrite it.
	const coldPath = "testdb/cpu/2026/10/03/14/cold.parquet"
	stamped := time.Date(2026, 10, 1, 2, 0, 0, 0, time.UTC)
	if _, err := store.RecordColdFile(ctx, replicatedTestFile(coldPath, 100), stamped); err != nil {
		t.Fatalf("RecordColdFile() setup error = %v", err)
	}
	wrote, err := store.markFileCold(ctx, replicatedTestFile(coldPath, 100))
	if err != nil {
		t.Fatalf("markFileCold() error = %v", err)
	}
	if wrote {
		t.Fatal("markFileCold() wrote to an already-cold row")
	}
	var migratedAt *time.Time
	if err := store.db.QueryRow(`SELECT migrated_at FROM tier_files WHERE path = ?`, coldPath).Scan(&migratedAt); err != nil {
		t.Fatalf("read migrated_at: %v", err)
	}
	if migratedAt == nil {
		t.Fatal("markFileCold() cleared the cold sync's migrated_at")
	}

	const quarantinedPath = "testdb/cpu/2026/10/03/14/q.parquet"
	if _, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile(quarantinedPath, 100)); err != nil {
		t.Fatalf("setup error = %v", err)
	}
	if err := store.QuarantineFile(ctx, quarantinedPath, "unusable key"); err != nil {
		t.Fatalf("QuarantineFile() error = %v", err)
	}
	wrote, err = store.markFileCold(ctx, replicatedTestFile(quarantinedPath, 100))
	if err != nil {
		t.Fatalf("markFileCold() error = %v", err)
	}
	if wrote {
		t.Fatal("markFileCold() wrote to a quarantined row")
	}
	if got := tierOf(t, store, quarantinedPath); got != string(TierHot) {
		t.Fatalf("quarantined row tier = %q, want hot (unchanged)", got)
	}
}

func TestRetireHotRow(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "testdb/cpu/2026/10/03/14/a.parquet"

	wrote, db, meas, err := store.retireHotRow(ctx, path)
	if err != nil {
		t.Fatalf("retireHotRow() error = %v", err)
	}
	if wrote {
		t.Fatal("retireHotRow() reported a removal for a path with no row")
	}

	if _, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile(path, 100)); err != nil {
		t.Fatalf("setup error = %v", err)
	}
	wrote, db, meas, err = store.retireHotRow(ctx, path)
	if err != nil {
		t.Fatalf("retireHotRow() error = %v", err)
	}
	if !wrote {
		t.Fatal("retireHotRow() did not remove a hot row")
	}
	if db != "testdb" || meas != "cpu" {
		t.Fatalf("retireHotRow() returned %q/%q, want testdb/cpu — the cache key is read off the row before the delete", db, meas)
	}

	// A row that reached cold is not this caller's to remove: the file is in
	// cold storage and the node reads it there.
	coldFile := replicatedTestFile(path, 100)
	coldFile.Tier = TierCold
	if err := store.RecordFile(ctx, coldFile); err != nil {
		t.Fatalf("RecordFile() setup error = %v", err)
	}
	wrote, _, _, err = store.retireHotRow(ctx, path)
	if err != nil {
		t.Fatalf("retireHotRow() error = %v", err)
	}
	if wrote {
		t.Fatal("retireHotRow() removed a cold row")
	}
	if got := tierOf(t, store, path); got != string(TierCold) {
		t.Fatalf("tier = %q, want cold", got)
	}
}

// A reader that ran its SELECT DISTINCT tier before a write must not be able
// to store the pre-write tier set. Without the generation bump it would serve
// a set missing hot for the full TTL, which for a measurement whose only other
// row is cold is the cold-only read this whole change exists to prevent.
func TestInvalidateTierCache_ConcurrentFillCannotStoreStaleSet(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	// Fill the cache while the measurement has only a cold row.
	coldFile := replicatedTestFile("testdb/cpu/2026/10/03/13/cold.parquet", 100)
	coldFile.Tier = TierCold
	if err := store.RecordFile(ctx, coldFile); err != nil {
		t.Fatalf("RecordFile() setup error = %v", err)
	}
	tiers, err := store.GetTiersForMeasurement(ctx, "testdb", "cpu")
	if err != nil {
		t.Fatalf("GetTiersForMeasurement() error = %v", err)
	}
	if tiers[TierHot] {
		t.Fatal("setup: did not expect a hot tier yet")
	}

	// Simulate the race directly: take the generation as a reader would, let
	// the write land, then try to store the stale set.
	store.tierCacheMu.RLock()
	staleGen := store.tierCacheGen
	store.tierCacheMu.RUnlock()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := store.recordHotFileIfNotCold(ctx, replicatedTestFile("testdb/cpu/2026/10/03/14/hot.parquet", 100)); err != nil {
			t.Errorf("recordHotFileIfNotCold() error = %v", err)
			return
		}
		store.invalidateTierCache("testdb", "cpu")
	}()
	wg.Wait()

	store.storeTierCacheIfUnchanged("testdb/cpu", []Tier{TierCold}, staleGen)

	tiers, err = store.GetTiersForMeasurement(ctx, "testdb", "cpu")
	if err != nil {
		t.Fatalf("GetTiersForMeasurement() error = %v", err)
	}
	if !tiers[TierHot] {
		t.Fatal("a stale cache fill was stored: the measurement reads as cold-only while a hot file is on disk")
	}
}
