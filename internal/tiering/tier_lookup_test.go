package tiering

import (
	"context"
	"testing"
	"time"
)

// DatabaseHasTierRows (#1084) is one indexed query over tier_files: true for
// a database with a row in any tier, false for one with none, and false with
// no error on a nil manager, like the other reports the backup manager holds
// as interfaces.
func TestManager_DatabaseHasTierRows(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, false)
	defer cleanup()
	ctx := context.Background()

	if err := m.metadata.RecordFile(ctx, &FileMetadata{
		Path:          "cold/cpu/2026/01/01/00/f.parquet",
		Database:      "cold",
		Measurement:   "cpu",
		PartitionTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Tier:          TierCold,
		SizeBytes:     1,
		CreatedAt:     time.Now().UTC(),
	}); err != nil {
		t.Fatalf("RecordFile: %v", err)
	}

	if has, err := m.DatabaseHasTierRows(ctx, "cold"); err != nil || !has {
		t.Errorf("cold (one cold row) = %v, %v; want true", has, err)
	}
	if has, err := m.DatabaseHasTierRows(ctx, "nope"); err != nil || has {
		t.Errorf("nope (no rows) = %v, %v; want false", has, err)
	}
	// The match is exact: a database whose name is a prefix of another is a
	// different database.
	if has, err := m.DatabaseHasTierRows(ctx, "col"); err != nil || has {
		t.Errorf("col (prefix of cold) = %v, %v; want false", has, err)
	}
	var none *Manager
	if has, err := none.DatabaseHasTierRows(ctx, "cold"); err != nil || has {
		t.Errorf("nil manager = %v, %v; want false, nil", has, err)
	}
}
