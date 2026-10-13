package tiering

import (
	"context"
	"sort"
	"testing"
	"time"
)

// Retention's cold candidates: one database's cold rows whose partition starts
// before the cutoff. Quarantined rows come back too (retention counts them and
// leaves them alone); hot rows, other databases and later partitions do not.
func TestGetColdFilesForRetention(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	cutoff := time.Now().UTC().Add(-7 * 24 * time.Hour)
	old := cutoff.Add(-24 * time.Hour)
	recent := cutoff.Add(24 * time.Hour)
	rows := []FileMetadata{
		{Path: "db1/cpu/old.parquet", Database: "db1", Measurement: "cpu", PartitionTime: old, Tier: TierCold},
		{Path: "db1/mem/old.parquet", Database: "db1", Measurement: "mem", PartitionTime: old, Tier: TierCold},
		{Path: "db1/mem/quarantined.parquet", Database: "db1", Measurement: "mem", PartitionTime: old, Tier: TierCold},
		{Path: "db1/cpu/hot.parquet", Database: "db1", Measurement: "cpu", PartitionTime: old, Tier: TierHot},
		{Path: "db2/cpu/old.parquet", Database: "db2", Measurement: "cpu", PartitionTime: old, Tier: TierCold},
		{Path: "db1/cpu/recent.parquet", Database: "db1", Measurement: "cpu", PartitionTime: recent, Tier: TierCold},
		// The bound is strict: a partition starting at the cutoff holds no
		// row older than it.
		{Path: "db1/cpu/at-cutoff.parquet", Database: "db1", Measurement: "cpu", PartitionTime: cutoff, Tier: TierCold},
	}
	for i := range rows {
		if err := m.metadata.RecordFile(ctx, &rows[i]); err != nil {
			t.Fatalf("RecordFile(%q): %v", rows[i].Path, err)
		}
	}
	if err := m.metadata.QuarantineFile(ctx, "db1/mem/quarantined.parquet", "test"); err != nil {
		t.Fatalf("QuarantineFile: %v", err)
	}

	got, err := m.GetColdFilesForRetention(ctx, "db1", cutoff)
	if err != nil {
		t.Fatalf("GetColdFilesForRetention: %v", err)
	}
	var paths []string
	quarantined := 0
	for _, f := range got {
		paths = append(paths, f.Path)
		if f.QuarantinedAt != nil {
			quarantined++
		}
	}
	sort.Strings(paths)
	want := []string{"db1/cpu/old.parquet", "db1/mem/old.parquet", "db1/mem/quarantined.parquet"}
	if len(paths) != len(want) {
		t.Fatalf("candidates = %v, want %v", paths, want)
	}
	for i := range want {
		if paths[i] != want[i] {
			t.Fatalf("candidates = %v, want %v", paths, want)
		}
	}
	if quarantined != 1 {
		t.Fatalf("quarantined candidates = %d, want 1", quarantined)
	}
}

// RetireColdRows removes cold rows only, and RevertColdRowToHot flips one back
// to hot. NotifyColdFilesRemoved reaches the query-cache callback.
func TestRetentionColdRowMaintenance(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	part := time.Now().UTC().Add(-30 * 24 * time.Hour)
	for _, f := range []FileMetadata{
		{Path: "db1/cpu/gone.parquet", Database: "db1", Measurement: "cpu", PartitionTime: part, Tier: TierCold},
		{Path: "db1/cpu/still-hot.parquet", Database: "db1", Measurement: "cpu", PartitionTime: part, Tier: TierHot},
		{Path: "db1/cpu/back-to-hot.parquet", Database: "db1", Measurement: "cpu", PartitionTime: part, Tier: TierCold},
	} {
		f := f
		if err := m.metadata.RecordFile(ctx, &f); err != nil {
			t.Fatalf("RecordFile(%q): %v", f.Path, err)
		}
	}

	removed, err := m.RetireColdRows(ctx, []string{"db1/cpu/gone.parquet", "db1/cpu/still-hot.parquet", "db1/cpu/absent.parquet"})
	if err != nil {
		t.Fatalf("RetireColdRows: %v", err)
	}
	if removed != 1 {
		t.Fatalf("RetireColdRows removed %d, want 1 (only the cold row)", removed)
	}
	if f, _ := m.metadata.GetFile(ctx, "db1/cpu/gone.parquet"); f != nil {
		t.Fatalf("cold row survived RetireColdRows")
	}
	if f, _ := m.metadata.GetFile(ctx, "db1/cpu/still-hot.parquet"); f == nil || f.Tier != TierHot {
		t.Fatalf("hot row = %+v, want it untouched", f)
	}

	if err := m.RevertColdRowToHot(ctx, "db1/cpu/back-to-hot.parquet"); err != nil {
		t.Fatalf("RevertColdRowToHot: %v", err)
	}
	if f, _ := m.metadata.GetFile(ctx, "db1/cpu/back-to-hot.parquet"); f == nil || f.Tier != TierHot {
		t.Fatalf("reverted row = %+v, want tier hot", f)
	}

	calls := 0
	m.SetOnMigrationComplete(func() { calls++ })
	m.NotifyColdFilesRemoved(2)
	m.NotifyColdFilesRemoved(0)
	if calls != 1 {
		t.Fatalf("query-cache callback ran %d times, want 1", calls)
	}
}
