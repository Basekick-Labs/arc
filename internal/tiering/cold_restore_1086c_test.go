package tiering

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// RecordColdFile reports whether it wrote, and leaves a QUARANTINED row alone.
// Before #1086 its ON CONFLICT had no WHERE at all, so it flipped any row to
// cold — including one tiering had established it can never act on (#758) —
// and re-did it on every sync cycle whose listing still returned the object.
func TestRecordColdFileLeavesAQuarantinedRowAlone(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "db1/cpu/2025/01/01/00/a.parquet"

	recordAt(t, store, path, "db1", TierHot)
	if err := store.QuarantineFile(ctx, path, "unusable key"); err != nil {
		t.Fatalf("QuarantineFile: %v", err)
	}

	wrote, err := store.RecordColdFile(ctx, &FileMetadata{
		Path: path, Database: "db1", Measurement: "cpu",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), SizeBytes: 10,
	}, time.Now())
	if err != nil {
		t.Fatalf("RecordColdFile: %v", err)
	}
	if wrote {
		t.Error("RecordColdFile reported a write for a quarantined row")
	}
	got, err := store.GetFile(ctx, path)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if got.Tier != TierHot {
		t.Errorf("tier = %q, want hot: a quarantined row must not be flipped to cold", got.Tier)
	}
	if got.QuarantinedAt == nil {
		t.Error("the quarantine was cleared")
	}
}

// A clean row is written and reported.
func TestRecordColdFileReportsAWrite(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "db1/cpu/2025/01/01/00/a.parquet"

	wrote, err := store.RecordColdFile(ctx, &FileMetadata{
		Path: path, Database: "db1", Measurement: "cpu",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), SizeBytes: 10,
	}, time.Now())
	if err != nil || !wrote {
		t.Fatalf("new row: wrote=%v err=%v, want true nil", wrote, err)
	}
	got, err := store.GetFile(ctx, path)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if got.Tier != TierCold {
		t.Errorf("tier = %q, want cold", got.Tier)
	}
}

// ColdRows is keyed by path, excludes quarantined rows, and reports only cold.
func TestColdRowsExcludesQuarantinedAndHot(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	recordAt(t, store, "db1/cpu/2025/01/01/00/cold.parquet", "db1", TierCold)
	recordAt(t, store, "db1/cpu/2025/01/01/00/hot.parquet", "db1", TierHot)
	recordAt(t, store, "db1/cpu/2025/01/01/00/quar.parquet", "db1", TierCold)
	if err := store.QuarantineFile(ctx, "db1/cpu/2025/01/01/00/quar.parquet", "unusable"); err != nil {
		t.Fatal(err)
	}

	m := &Manager{metadata: store}
	rows, err := m.ColdRows(ctx)
	if err != nil {
		t.Fatalf("ColdRows: %v", err)
	}
	if len(rows) != 1 {
		t.Errorf("rows = %v, want exactly the one clean cold row", rows)
	}
	if _, ok := rows["db1/cpu/2025/01/01/00/cold.parquet"]; !ok {
		t.Error("the clean cold row is missing")
	}
	if _, ok := rows["db1/cpu/2025/01/01/00/quar.parquet"]; ok {
		t.Error("a quarantined row is present; a backup must not try to carry a permanently unusable key")
	}
}

// Nil-receiver safety, like the three adapters beside these.
func TestColdSourceMethodsAreNilSafe(t *testing.T) {
	ctx := context.Background()
	var nilManager *Manager
	if b := nilManager.ColdBackend(); b != nil {
		t.Error("nil manager returned a cold backend")
	}
	if rows, err := nilManager.ColdRows(ctx); err != nil || rows != nil {
		t.Errorf("nil manager ColdRows = %v, %v, want nil nil", rows, err)
	}
	const nilPath = "db/m/2025/01/01/00/a.parquet"
	quarantined, failed, err := nilManager.RecordRestoredColdFiles(ctx, map[string]int64{nilPath: 1})
	if err != nil || len(quarantined) != 0 {
		t.Errorf("nil manager RecordRestoredColdFiles = %v, %v, %v, want no quarantine and no error", quarantined, failed, err)
	}
	// Reported FAILED, not quarantined: a quarantine is a fact tiering
	// recorded, and a manager with no store has established nothing.
	if len(failed) != 1 || failed[0] != nilPath {
		t.Errorf("failed = %v, want the one path back", failed)
	}
	if _, failed, err := nilManager.RecordRestoredHotFiles(ctx, map[string]int64{nilPath: 1}); err != nil || len(failed) != 1 {
		t.Errorf("nil manager RecordRestoredHotFiles failed = %v, err = %v, want the one path back", failed, err)
	}
	// A partly-built manager too: config is a pointer, so ColdBackend must
	// check it before reading Cold.Enabled.
	empty := &Manager{}
	if rows, err := empty.ColdRows(ctx); err != nil || rows != nil {
		t.Errorf("empty manager ColdRows = %v, %v, want nil nil", rows, err)
	}
	if b := empty.ColdBackend(); b != nil {
		t.Error("a manager with no config returned a cold backend")
	}
	withBackendNoConfig := &Manager{coldBackend: &storage.LocalBackend{}}
	if b := withBackendNoConfig.ColdBackend(); b != nil {
		t.Error("ColdBackend() with a nil config must be nil, not a panic")
	}
}

// ColdBackend ANDs the enabled flag, which GetBackendForTier does not. A
// backup that walked a disabled cold tier would carry objects the query path
// refuses to read, because the query router gates its cold glob on the same
// flag.
func TestColdBackendRespectsTheEnabledFlag(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	enabled := &config.TieredStorageConfig{}
	enabled.Cold.Enabled = true
	on := &Manager{metadata: store, coldBackend: backend, config: enabled}
	if on.ColdBackend() == nil {
		t.Error("ColdBackend() is nil with cold enabled")
	}
	disabled := &config.TieredStorageConfig{}
	disabled.Cold.Enabled = false
	off := &Manager{metadata: store, coldBackend: backend, config: disabled}
	if off.ColdBackend() != nil {
		t.Error("ColdBackend() returned a backend with cold DISABLED; a file written there would be unreadable, since the query router gates the cold glob on the same flag")
	}
	// GetBackendForTier deliberately does not check the flag, so the contrast
	// is the point of this test.
	if off.GetBackendForTier(TierCold) == nil {
		t.Error("GetBackendForTier stopped returning the configured backend; ColdBackend is the one that ANDs the flag")
	}
}

// RecordRestoredColdFiles parses each path with tiering's own rule, stamps the
// row now, and writes it as cold.
func TestRecordRestoredColdFilesWritesAColdRow(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	m := &Manager{metadata: store}
	const path = "db1/cpu/2025/01/01/00/a.parquet"

	before := time.Now().Add(-time.Second)
	quarantined, failed, err := m.RecordRestoredColdFiles(ctx, map[string]int64{path: 1234})
	if err != nil || len(quarantined) != 0 || len(failed) != 0 {
		t.Fatalf("RecordRestoredColdFiles = %v, %v, %v, want all empty", quarantined, failed, err)
	}
	got, err := store.GetFile(ctx, path)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	if got.Tier != TierCold {
		t.Errorf("tier = %q, want cold", got.Tier)
	}
	if got.Database != "db1" || got.Measurement != "cpu" {
		t.Errorf("parsed (%q, %q), want (db1, cpu)", got.Database, got.Measurement)
	}
	if got.SizeBytes != 1234 {
		t.Errorf("size = %d, want 1234", got.SizeBytes)
	}
	// Stamped NOW, not from the object: only a row inside the orphan
	// reconciliation window lets the stale hot copy at the same key be
	// cleaned up.
	if got.MigratedAt == nil || got.MigratedAt.Before(before) {
		t.Errorf("migrated_at = %v, want a stamp from this moment so the row lands inside the reconciliation window", got.MigratedAt)
	}
}

// A HOT row for the same path flips to cold: the restore is authoritative
// about what it just wrote, and leaving the row hot would make the restored
// cold object unreadable.
func TestRecordRestoredColdFilesFlipsAHotRow(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "db1/cpu/2025/01/01/00/a.parquet"
	recordAt(t, store, path, "db1", TierHot)

	m := &Manager{metadata: store}
	quarantined, failed, err := m.RecordRestoredColdFiles(ctx, map[string]int64{path: 99})
	if err != nil || len(quarantined) != 0 || len(failed) != 0 {
		t.Fatalf("RecordRestoredColdFiles = %v, %v, %v", quarantined, failed, err)
	}
	got, err := store.GetFile(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Tier != TierCold {
		t.Errorf("tier = %q, want cold", got.Tier)
	}
}

// The quarantine guard's effect on its PRE-EXISTING caller. The sync walks the
// cold listing and upserts a row for every object; before the guard it would
// set tier='cold' on a quarantined row every cycle the listing still returned
// the object. Now it reports no write, the caller skips it, and the row is
// left exactly as it was.
func TestSyncLeavesAQuarantinedRowAlone(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "db1/cpu/2025/01/01/00/a.parquet"

	recordAt(t, store, path, "db1", TierHot)
	if err := store.QuarantineFile(ctx, path, "unusable key"); err != nil {
		t.Fatalf("QuarantineFile: %v", err)
	}

	// What the sync does per listed object.
	wrote, err := store.RecordColdFile(ctx, &FileMetadata{
		Path: path, Database: "db1", Measurement: "cpu",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), SizeBytes: 10,
	}, time.Now())
	if err != nil {
		t.Fatalf("RecordColdFile: %v", err)
	}
	if wrote {
		t.Fatal("the sync's upsert reported a write for a quarantined row; the caller would count it as synced and append it to coldRows for the manifest sweep")
	}

	got, err := store.GetFile(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Tier != TierHot || got.QuarantinedAt == nil {
		t.Errorf("row = (tier %q, quarantined %v), want hot and still quarantined", got.Tier, got.QuarantinedAt)
	}
	// Idempotent: a second cycle does the same nothing.
	if wrote, _ := store.RecordColdFile(ctx, &FileMetadata{
		Path: path, Database: "db1", Measurement: "cpu",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), SizeBytes: 10,
	}, time.Now()); wrote {
		t.Error("a second sync cycle reported a write")
	}
}

// The forced hot recorder moves a COLD row to hot and clears migrated_at: the
// file is no longer a migrated copy, and a stamp would leave it looking like a
// cold-tier reconciliation candidate with no cold object to verify.
func TestRecordRestoredHotFileMovesAColdRow(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "db1/cpu/2025/01/01/00/a.parquet"

	if _, err := store.RecordColdFile(ctx, &FileMetadata{
		Path: path, Database: "db1", Measurement: "cpu",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), SizeBytes: 10,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}

	m := &Manager{metadata: store}
	quarantined, failed, err := m.RecordRestoredHotFiles(ctx, map[string]int64{path: 55})
	if err != nil || len(quarantined) != 0 || len(failed) != 0 {
		t.Fatalf("RecordRestoredHotFiles = %v, %v, %v, want all empty", quarantined, failed, err)
	}
	got, err := store.GetFile(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Tier != TierHot {
		t.Errorf("tier = %q, want hot: the ordinary hot report cannot move a cold row, which is why this exists", got.Tier)
	}
	if got.MigratedAt != nil {
		t.Errorf("migrated_at = %v, want cleared", got.MigratedAt)
	}
	if got.SizeBytes != 55 {
		t.Errorf("size = %d, want 55", got.SizeBytes)
	}
}

// And it leaves a quarantined row alone, like its cold counterpart.
func TestRecordRestoredHotFilesLeavesAQuarantinedRowAlone(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	const path = "db1/cpu/2025/01/01/00/a.parquet"
	recordAt(t, store, path, "db1", TierCold)
	if err := store.QuarantineFile(ctx, path, "unusable"); err != nil {
		t.Fatal(err)
	}

	m := &Manager{metadata: store}
	quarantined, _, err := m.RecordRestoredHotFiles(ctx, map[string]int64{path: 55})
	if err != nil {
		t.Fatalf("RecordRestoredHotFiles: %v", err)
	}
	if len(quarantined) != 1 || quarantined[0] != path {
		t.Errorf("quarantined = %v, want the one path reported rather than written", quarantined)
	}
	got, _ := store.GetFile(ctx, path)
	if got.Tier != TierCold {
		t.Errorf("tier = %q, want cold (untouched)", got.Tier)
	}
}
