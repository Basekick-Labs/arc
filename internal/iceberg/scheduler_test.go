package iceberg

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// TestStorageWalkSourceAndReconcile is the Phase-2 integration proof: lay out Arc-style
// Parquet files under a real LocalBackend, then drive the reconciler through the
// FileSetSource (storage walk) — no tiering, no cluster — and assert the Iceberg table
// converges to the on-disk file set, then re-converges after a file is added/removed.
func TestStorageWalkSourceAndReconcile(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()

	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	// Arc layout: {db}/{measurement}/{Y}/{M}/{D}/{H}/{file}.parquet, all within one UTC day.
	base := int64(1_700_000_000_000_000)
	relH14 := "mydb/cpu/2023/11/14/22/cpu_a.parquet"
	relH15 := "mydb/cpu/2023/11/14/23/cpu_b.parquet"
	writeArcStyleParquet(t, filepath.Join(root, relH14), base, 100)
	writeArcStyleParquet(t, filepath.Join(root, relH15), base+3600_000_000, 100)

	db, err := sql.Open("sqlite3", filepath.Join(root, "arc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	exp, err := NewExporter(db, "file://"+root+"/warehouse", "arc", zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := NewStorageWalkSource(backend)
	sched := NewScheduler(SchedulerConfig{Exporter: exp, Source: src, Logger: zerolog.Nop()})

	// Enumerate: should find exactly mydb/cpu.
	ms, err := src.Measurements(ctx)
	if err != nil {
		t.Fatalf("Measurements: %v", err)
	}
	if len(ms) != 1 || ms[0].Database != "mydb" || ms[0].Measurement != "cpu" {
		t.Fatalf("Measurements = %+v, want [mydb/cpu]", ms)
	}

	// One reconcile pass (gate nil = runs).
	sched.runPass(ctx)

	assertTableFiles := func(want ...string) {
		t.Helper()
		lt, err := exp.EnsureTable(ctx, "mydb", "cpu", ArcSchema{})
		if err != nil {
			t.Fatalf("load table: %v", err)
		}
		have, err := exp.tableDataFiles(ctx, lt)
		if err != nil {
			t.Fatalf("tableDataFiles: %v", err)
		}
		if len(have) != len(want) {
			t.Fatalf("table has %d files, want %d: %v", len(have), len(want), have)
		}
		for _, w := range want {
			if _, ok := have["file://"+filepath.Join(root, w)]; !ok {
				t.Errorf("table missing expected file %q; have %v", w, have)
			}
		}
	}
	assertTableFiles(relH14, relH15)

	// Add a third file, reconcile → table grows to 3.
	relH16 := "mydb/cpu/2023/11/14/21/cpu_c.parquet"
	writeArcStyleParquet(t, filepath.Join(root, relH16), base+7200_000_000, 50)
	sched.runPass(ctx)
	assertTableFiles(relH14, relH15, relH16)

	// Remove one file (simulate retention/compaction), reconcile → table shrinks to 2.
	if err := os.Remove(filepath.Join(root, relH14)); err != nil {
		t.Fatal(err)
	}
	sched.runPass(ctx)
	assertTableFiles(relH15, relH16)
}
