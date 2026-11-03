package tiering

import (
	"context"
	"testing"
)

func TestCountFilesInTierForDatabase(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()

	if got, err := store.CountFilesInTierForDatabase(ctx, "db1", TierCold); err != nil || got != 0 {
		t.Fatalf("empty store: got %d err %v, want zero and no error", got, err)
	}

	const quarantinedPath = "db1/cpu/2025/01/01/00/b.parquet"
	recordAt(t, store, "db1/cpu/2025/01/01/00/a.parquet", "db1", TierCold)
	recordAt(t, store, quarantinedPath, "db1", TierCold)
	recordAt(t, store, "db1/cpu/2025/01/01/00/c.parquet", "db1", TierHot)
	recordAt(t, store, "db2/cpu/2025/01/01/00/d.parquet", "db2", TierCold)
	if err := store.QuarantineFile(ctx, quarantinedPath, "unusable key"); err != nil {
		t.Fatalf("QuarantineFile: %v", err)
	}

	got, err := store.CountFilesInTierForDatabase(ctx, "db1", TierCold)
	if err != nil {
		t.Fatalf("CountFilesInTierForDatabase: %v", err)
	}
	if got != 1 {
		t.Fatalf("db1 cold count = %d, want 1 (excluding hot and quarantined rows)", got)
	}
	got, err = store.CountFilesInTierForDatabase(ctx, "missing", TierCold)
	if err != nil || got != 0 {
		t.Fatalf("missing database: got %d err %v, want zero and no error", got, err)
	}
}

func TestManagerCountColdFilesForDatabaseNilSafe(t *testing.T) {
	ctx := context.Background()
	var nilManager *Manager
	if got, err := nilManager.CountColdFilesForDatabase(ctx, "db1"); err != nil || got != 0 {
		t.Errorf("nil manager: got %d err %v, want zero and no error", got, err)
	}
	if got, err := (&Manager{}).CountColdFilesForDatabase(ctx, "db1"); err != nil || got != 0 {
		t.Errorf("manager without metadata: got %d err %v, want zero and no error", got, err)
	}
}

func TestManagerCountColdFilesForDatabaseReportsOnlyColdRowsForDatabase(t *testing.T) {
	store, cleanup := setupTestMetadataStore(t)
	defer cleanup()
	ctx := context.Background()
	recordAt(t, store, "db1/cpu/2025/01/01/00/a.parquet", "db1", TierCold)
	recordAt(t, store, "db1/cpu/2025/01/01/00/b.parquet", "db1", TierHot)
	recordAt(t, store, "db2/cpu/2025/01/01/00/c.parquet", "db2", TierCold)

	manager := &Manager{metadata: store}
	got, err := manager.CountColdFilesForDatabase(ctx, "db1")
	if err != nil {
		t.Fatalf("CountColdFilesForDatabase: %v", err)
	}
	if got != 1 {
		t.Fatalf("db1 cold count = %d, want 1", got)
	}
}
