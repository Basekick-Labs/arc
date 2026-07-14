package iceberg

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// TestMeasurements_ExcludesWarehouseDirs is the H1 regression: the reconciler writes its
// Iceberg warehouse ("<nsPrefix>_<db>.db/…") under the same storage root as the data, so the
// storage walk MUST skip those dirs — otherwise it enumerates its own metadata as phantom
// databases/measurements.
func TestMeasurements_ExcludesWarehouseDirs(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	// A real user database + measurement with a parquet file.
	if err := backend.Write(ctx, "mydb/cpu/2026/07/14/15/f.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	// A warehouse namespace dir (what the exporter itself writes) with metadata.
	if err := backend.Write(ctx, "arc_mydb.db/cpu/metadata/v1.metadata.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "arc_mydb.db/cpu/metadata/version-hint.text", []byte("1")); err != nil {
		t.Fatal(err)
	}

	src := NewStorageWalkSource(backend, "arc")
	ms, err := src.Measurements(ctx)
	if err != nil {
		t.Fatalf("Measurements: %v", err)
	}

	// Must find exactly the real measurement, never the warehouse dir.
	if len(ms) != 1 {
		t.Fatalf("Measurements = %+v, want exactly [mydb/cpu]", ms)
	}
	if ms[0].Database != "mydb" || ms[0].Measurement != "cpu" {
		t.Errorf("Measurements[0] = %+v, want mydb/cpu", ms[0])
	}
	for _, m := range ms {
		if m.Database == "arc_mydb.db" {
			t.Errorf("warehouse dir %q enumerated as a database", m.Database)
		}
	}
}

// TestFiles_RecursesNestedPartitions verifies Files() returns parquet from the nested
// Y/M/D/H partition dirs (not just immediate children) — otherwise the reconciler would see
// zero files and wrongly empty the table.
func TestFiles_RecursesNestedPartitions(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, _ := storage.NewLocalBackend(root, zerolog.Nop())

	keys := []string{
		"mydb/cpu/2026/07/14/15/a.parquet",
		"mydb/cpu/2026/07/14/16/b.parquet",
		"mydb/cpu/2026/07/15/00/c.parquet",
	}
	for _, k := range keys {
		if err := backend.Write(ctx, k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}

	src := NewStorageWalkSource(backend, "arc")
	files, err := src.Files(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		t.Fatalf("Files: %v", err)
	}
	if len(files) != len(keys) {
		t.Fatalf("Files returned %d, want %d (nested partition dirs must be walked): %+v", len(files), len(keys), files)
	}
	// Paths must resolve to file:// URIs under the root.
	for _, f := range files {
		if want := "file://" + filepath.Join(root); f.PhysicalPath[:len(want)] != want {
			t.Errorf("file path %q not under root %q", f.PhysicalPath, want)
		}
	}
}
