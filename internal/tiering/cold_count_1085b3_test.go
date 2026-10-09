package tiering

import (
	"context"
	"testing"
	"time"
)

// recordAt is the shortest path to a row in a given tier for these tests.
func recordAt(t *testing.T, store *MetadataStore, path, database string, tier Tier) {
	t.Helper()
	if err := store.RecordFile(context.Background(), &FileMetadata{
		Path:          path,
		Database:      database,
		Measurement:   "cpu",
		PartitionTime: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		Tier:          tier,
		SizeBytes:     1024,
		CreatedAt:     time.Now().UTC(),
	}); err != nil {
		t.Fatalf("RecordFile %s: %v", path, err)
	}
}

// The grouped count returns one entry per database that HAS rows in the tier,
// and no entry at all for a database that has none. A hot-only database being
// absent rather than present-at-zero is what lets the caller skip discovering
// the database set first.
func TestCountFilesInTierByDatabase(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	if got, err := store.CountFilesInTierByDatabase(ctx, TierCold); err != nil || len(got) != 0 {
		t.Fatalf("empty store: got %v err %v, want an empty map and no error", got, err)
	}

	recordAt(t, store, "hotonly/cpu/2025/01/01/00/a.parquet", "hotonly", TierHot)
	recordAt(t, store, "mixed/cpu/2025/01/01/00/b.parquet", "mixed", TierHot)
	recordAt(t, store, "mixed/cpu/2025/01/01/00/c.parquet", "mixed", TierCold)
	recordAt(t, store, "mixed/cpu/2025/01/01/00/d.parquet", "mixed", TierCold)
	recordAt(t, store, "archive/cpu/2025/01/01/00/e.parquet", "archive", TierCold)

	counts, err := store.CountFilesInTierByDatabase(ctx, TierCold)
	if err != nil {
		t.Fatalf("CountFilesInTierByDatabase: %v", err)
	}
	if len(counts) != 2 {
		t.Errorf("counts = %v, want exactly 2 databases (mixed, archive)", counts)
	}
	if counts["mixed"] != 2 {
		t.Errorf("counts[mixed] = %d, want 2", counts["mixed"])
	}
	if counts["archive"] != 1 {
		t.Errorf("counts[archive] = %d, want 1", counts["archive"])
	}
	if n, ok := counts["hotonly"]; ok {
		t.Errorf("counts[hotonly] = %d and present; a database with no cold rows must be ABSENT, not zero", n)
	}

	// And the hot tier is countable by the same call, which is what makes the
	// tier a parameter rather than a hardcoded 'cold'.
	hot, err := store.CountFilesInTierByDatabase(ctx, TierHot)
	if err != nil {
		t.Fatalf("hot tier: %v", err)
	}
	if hot["hotonly"] != 1 || hot["mixed"] != 1 || len(hot) != 2 {
		t.Errorf("hot counts = %v, want {hotonly:1, mixed:1}", hot)
	}
}

// A quarantined row is excluded. Defensive today: the migrator only ever
// quarantines a row it is leaving in the HOT tier, so a quarantined COLD row
// cannot arise through migration — but QuarantineFile itself is tier-agnostic
// (WHERE path = ?), so this is reachable and will matter if that call site
// changes.
//
// Quarantined through QuarantineFile rather than a hand-written UPDATE, so the
// row is written exactly as production writes it — including the time domain.
// The count only asks IS NULL, so a test that wrote the column in the wrong
// domain would pass anyway; using the real writer means it cannot drift.
func TestCountFilesInTierByDatabaseExcludesQuarantined(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	recordAt(t, store, "db1/cpu/2025/01/01/00/a.parquet", "db1", TierCold)
	recordAt(t, store, "db1/cpu/2025/01/01/00/b.parquet", "db1", TierCold)
	if err := store.QuarantineFile(ctx, "db1/cpu/2025/01/01/00/b.parquet", "unusable key"); err != nil {
		t.Fatalf("QuarantineFile: %v", err)
	}

	counts, err := store.CountFilesInTierByDatabase(ctx, TierCold)
	if err != nil {
		t.Fatalf("CountFilesInTierByDatabase: %v", err)
	}
	if counts["db1"] != 1 {
		t.Errorf("counts[db1] = %d, want 1: the quarantined row must not be counted as a file a backup is missing", counts["db1"])
	}

	// A quarantined HOT row is irrelevant to a cold count either way.
	recordAt(t, store, "db2/cpu/2025/01/01/00/c.parquet", "db2", TierHot)
	if err := store.QuarantineFile(ctx, "db2/cpu/2025/01/01/00/c.parquet", "unusable key"); err != nil {
		t.Fatalf("QuarantineFile hot: %v", err)
	}
	counts, err = store.CountFilesInTierByDatabase(ctx, TierCold)
	if err != nil {
		t.Fatalf("CountFilesInTierByDatabase: %v", err)
	}
	if _, ok := counts["db2"]; ok {
		t.Errorf("counts names db2, which has only a quarantined HOT row: %v", counts)
	}
}

// A nil manager and a manager with no metadata store both answer "no rows",
// not a panic: the backup manager holds this as an interface and may be given
// a manager built before its store (#713).
func TestManagerCountColdFilesByDatabaseNilSafe(t *testing.T) {
	ctx := context.Background()
	var nilManager *Manager
	if got, err := nilManager.CountColdFilesByDatabase(ctx); err != nil || got != nil {
		t.Errorf("nil manager: got %v err %v, want nil nil", got, err)
	}
	empty := &Manager{}
	if got, err := empty.CountColdFilesByDatabase(ctx); err != nil || got != nil {
		t.Errorf("manager with no metadata store: got %v err %v, want nil nil", got, err)
	}
}

// The delegation reaches the store and reports the COLD tier, not the hot one.
func TestManagerCountColdFilesByDatabaseReportsCold(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	recordAt(t, store, "db1/cpu/2025/01/01/00/a.parquet", "db1", TierCold)
	recordAt(t, store, "db1/cpu/2025/01/01/00/b.parquet", "db1", TierHot)

	m := &Manager{metadata: store}
	counts, err := m.CountColdFilesByDatabase(ctx)
	if err != nil {
		t.Fatalf("CountColdFilesByDatabase: %v", err)
	}
	if counts["db1"] != 1 {
		t.Errorf("counts = %v, want {db1:1}: the hot row must not be counted", counts)
	}
}
