package edgesync

// #687: tiering marks receipts (via MarkCompacted) before deleting the hot
// copy of a migrated spoke file. This pins the receipt lifecycle that makes
// that safe with the REAL index: a marked receipt is reported Compacted by
// Lookup, which (a) makes confirmPresent skip the existence check so the
// receipt survives the file's absence, and (b) makes a spoke's re-offer of
// the same path+sha answer already-present (pinned by the receive tests).

import (
	"context"
	"testing"
	"time"
)

func TestMigrationStyleMarkSurvivesFileAbsence(t *testing.T) {
	idx := newTestHubIndex(t)
	ctx := context.Background()

	// A legacy spoke-side compacted daily file, received and receipted.
	rec := &ReceivedRecord{
		SpokeID:    "rocket-01",
		SourcePath: "telemetry/engine_temp/2024/03/15/engine_temp_20240315_daily.parquet",
		HubPath:    "rocket-01/telemetry/engine_temp/2024/03/15/engine_temp_20240315_daily.parquet",
		SHA256:     "abc123",
		SizeBytes:  10,
		ReceivedAt: time.Now().UTC(),
	}
	if err := idx.Record(ctx, rec); err != nil {
		t.Fatal(err)
	}

	// The migration hook marks by hub-path-minus-namespace = source path.
	if err := idx.MarkCompacted(ctx, "rocket-01", []string{rec.SourcePath}); err != nil {
		t.Fatal(err)
	}

	held, err := idx.Lookup(ctx, "rocket-01", []string{rec.SourcePath})
	if err != nil {
		t.Fatal(err)
	}
	hf, ok := held[rec.SourcePath]
	if !ok || !hf.Compacted {
		t.Fatalf("held = %+v, want the receipt present and marked (confirmPresent then skips its existence check)", held)
	}
}
