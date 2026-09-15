package iceberg

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// TestScheduler_InPlaceRewriteDetected drives the real storage-walk source through runPass:
// after a file is rewritten in place (same path, fewer rows) the next pass must NOT be skipped
// by the fingerprint gate, and the manifest entry must follow the rewrite.
func TestScheduler_InPlaceRewriteDetected(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	rel := "mydb/cpu/2023/11/14/22/a.parquet"
	baseTS := int64(1_700_000_000_000_000)
	writeArcStyleParquet(t, filepath.Join(root, rel), baseTS, 100)

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	exp, err := NewExporter(db, backend, "file://"+root, "arc", 0, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: NewStorageWalkSource(backend, "arc", zerolog.Nop()), Logger: zerolog.Nop()})

	sched.runPass(ctx)
	uri := "file://" + filepath.Join(root, rel)
	if got := liveEntries(ctx, t, exp, "mydb", "cpu")[uri]; got.count != 100 {
		t.Fatalf("first pass: record_count = %d, want 100", got.count)
	}
	s1 := currentSnapshotID(ctx, t, exp)

	rewriteInPlace(t, filepath.Join(root, rel), baseTS, 60)
	sched.runPass(ctx)
	if s2 := currentSnapshotID(ctx, t, exp); s2 == s1 {
		t.Fatal("pass after the in-place rewrite was skipped as unchanged (fingerprint is path-only)")
	}
	if got := liveEntries(ctx, t, exp, "mydb", "cpu")[uri]; got.count != 60 {
		t.Fatalf("after rewrite: record_count = %d, want 60", got.count)
	}
	// Steady state again: no new snapshot.
	s2 := currentSnapshotID(ctx, t, exp)
	sched.runPass(ctx)
	if s3 := currentSnapshotID(ctx, t, exp); s3 != s2 {
		t.Errorf("unchanged pass after the rewrite created a new snapshot (%d -> %d)", s2, s3)
	}
}

func TestFingerprint_IncludesSize(t *testing.T) {
	a := fingerprint([]FileRef{{PhysicalPath: "file:///x/a.parquet", SizeBytes: 100}}, []string{"/x/a.parquet"})
	b := fingerprint([]FileRef{{PhysicalPath: "file:///x/a.parquet", SizeBytes: 60}}, []string{"/x/a.parquet"})
	if a == b {
		t.Fatal("fingerprint ignores file size: an in-place rewrite would be skipped")
	}
	if a != fingerprint([]FileRef{{PhysicalPath: "file:///x/a.parquet", SizeBytes: 100}}, []string{"/x/a.parquet"}) {
		t.Fatal("fingerprint is not deterministic")
	}
}
