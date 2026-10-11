package tiering

import (
	"context"
	"testing"
	"time"
)

func TestGetColdFilesForRetentionFiltersDatabaseMeasurementAndCutoff(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	cutoff := time.Now().UTC().Add(-7 * 24 * time.Hour)
	old := cutoff.Add(-24 * time.Hour)
	recent := cutoff.Add(24 * time.Hour)
	files := []FileMetadata{
		{Path: "db1/cpu/old.parquet", Database: "db1", Measurement: "cpu", PartitionTime: old, Tier: TierCold, SizeBytes: 100},
		{Path: "db1/mem/old.parquet", Database: "db1", Measurement: "mem", PartitionTime: old, Tier: TierCold, SizeBytes: 200},
		{Path: "db2/cpu/old.parquet", Database: "db2", Measurement: "cpu", PartitionTime: old, Tier: TierCold, SizeBytes: 300},
		{Path: "db1/cpu/recent.parquet", Database: "db1", Measurement: "cpu", PartitionTime: recent, Tier: TierCold, SizeBytes: 400},
	}
	for i := range files {
		if err := m.metadata.RecordFile(ctx, &files[i]); err != nil {
			t.Fatalf("RecordFile(%q): %v", files[i].Path, err)
		}
	}

	got, err := m.GetColdFilesForRetention(ctx, "db1", "cpu", cutoff)
	if err != nil {
		t.Fatalf("GetColdFilesForRetention: %v", err)
	}
	if len(got) != 1 || got[0].Path != "db1/cpu/old.parquet" {
		t.Fatalf("cold retention candidates = %+v, want only db1/cpu/old.parquet", got)
	}
}
