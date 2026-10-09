package tiering

// The hot scan retires hot rows whose files are gone from hot storage — a
// stale hot row keeps an empty hot glob in every multi-tier read of its
// measurement — but never a cold row, a quarantined row, or a row young
// enough that the listing may simply not have seen its file yet.

import (
	"context"
	"testing"
	"time"
)

func TestScanRetiresHotRowsForVanishedFiles(t *testing.T) {
	m, hot, cold, cleanup := setupIntegrationTest(t, true)
	defer cleanup()
	ctx := context.Background()
	old := time.Now().Add(-time.Hour)
	partition := time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)
	row := func(path string, tier Tier, created time.Time) *FileMetadata {
		return &FileMetadata{Path: path, Database: "db1", Measurement: "cpu", PartitionTime: partition,
			Tier: tier, SizeBytes: 7, CreatedAt: created}
	}
	const (
		present      = "db1/cpu/2024/03/15/14/cpu_present.parquet"
		vanished     = "db1/cpu/2024/03/15/14/cpu_vanished.parquet"
		justSettled  = "db1/cpu/2024/03/15/14/cpu_just_settled.parquet"
		insideMargin = "db1/cpu/2024/03/15/14/cpu_inside_margin.parquet"
		young        = "db1/cpu/2024/03/15/14/cpu_young.parquet"
		coldOnly     = "db1/cpu/2024/03/15/cpu_cold_daily.parquet"
		quarantined  = "db1/cpu/2024/03/15/14/cpu_quarantined.parquet"
	)

	mustWrite(t, hot, present)
	mustWrite(t, cold, coldOnly)
	for _, r := range []*FileMetadata{
		row(present, TierHot, old),
		row(vanished, TierHot, old), // file gone: compaction consumed it
		row(justSettled, TierHot, time.Now().Add(-retireVanishedHotRowMargin-time.Minute)),
		row(insideMargin, TierHot, time.Now().Add(-retireVanishedHotRowMargin+time.Minute)),
		row(young, TierHot, time.Now()), // registered just now: the listing may predate it
		row(quarantined, TierHot, old),
	} {
		if err := m.metadata.RecordFile(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.metadata.RecordColdFile(ctx, row(coldOnly, TierCold, old), old); err != nil {
		t.Fatal(err)
	}
	if err := m.metadata.QuarantineFile(ctx, quarantined, quarantineReasonInvalidPath); err != nil {
		t.Fatal(err)
	}

	// Through ScanTiers, the path the API and the cycle use, so the count
	// reaches them.
	res, err := m.ScanTiers(ctx)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if res.HotRetired != 2 {
		t.Fatalf("hot_retired = %d, want 2 (the vanished row and the one just past the margin)", res.HotRetired)
	}
	for _, gone := range []string{vanished, justSettled} {
		if f, _ := m.metadata.GetFile(ctx, gone); f != nil {
			t.Fatalf("%s hot row still present: %+v", gone, f)
		}
	}
	for _, kept := range []string{present, insideMargin, young, coldOnly, quarantined} {
		if f, _ := m.metadata.GetFile(ctx, kept); f == nil {
			t.Fatalf("%s row was retired", kept)
		}
	}
	if got := fileMeta(t, m, coldOnly).Tier; got != TierCold {
		t.Fatalf("cold row tier = %s, want cold", got)
	}

	// Steady state: nothing more to retire.
	res, err = m.ScanTiers(ctx)
	if err != nil || res.HotRetired != 0 {
		t.Fatalf("second scan: hot_retired=%d err=%v, want 0", res.HotRetired, err)
	}
}
