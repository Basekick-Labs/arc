package tiering

import (
	"context"
	"testing"
	"time"
)

func TestFindCandidatesFilteredScopesManualMigration(t *testing.T) {
	m, _, _, cleanup := setupIntegrationTest(t, true)
	defer cleanup()

	ctx := context.Background()
	old := time.Now().UTC().Add(-30 * 24 * time.Hour)
	files := []FileMetadata{
		{Path: "db1/cpu/2025/01/01/cpu_20250101_daily.parquet", Database: "db1", Measurement: "cpu", PartitionTime: old, Tier: TierHot, SizeBytes: 100},
		{Path: "db1/mem/2025/01/01/mem_20250101_daily.parquet", Database: "db1", Measurement: "mem", PartitionTime: old, Tier: TierHot, SizeBytes: 200},
		{Path: "db2/cpu/2025/01/01/cpu_20250101_daily.parquet", Database: "db2", Measurement: "cpu", PartitionTime: old, Tier: TierHot, SizeBytes: 300},
	}
	for i := range files {
		if err := m.metadata.RecordFile(ctx, &files[i]); err != nil {
			t.Fatalf("RecordFile(%q): %v", files[i].Path, err)
		}
	}

	got, err := m.migrator.FindCandidatesFiltered(ctx, TierHot, TierCold, "db1", "cpu")
	if err != nil {
		t.Fatalf("FindCandidatesFiltered: %v", err)
	}
	if len(got) != 1 || got[0].Database != "db1" || got[0].Measurement != "cpu" {
		t.Fatalf("filtered candidates = %+v, want only db1/cpu", got)
	}

	preview, err := m.PreviewMigration(ctx, TierHot, TierCold, "db1", "")
	if err != nil {
		t.Fatalf("PreviewMigration: %v", err)
	}
	if len(preview) != 2 {
		t.Fatalf("database preview returned %d candidates, want 2", len(preview))
	}

	for _, f := range files {
		meta, err := m.metadata.GetFile(ctx, f.Path)
		if err != nil {
			t.Fatalf("GetFile(%q): %v", f.Path, err)
		}
		if meta.Tier != TierHot {
			t.Fatalf("PreviewMigration changed %q tier to %q; want hot", f.Path, meta.Tier)
		}
	}
}
