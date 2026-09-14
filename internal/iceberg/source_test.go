package iceberg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

type failingDirectoryBackend struct {
	storage.Backend
	failedPrefix string
}

func (b *failingDirectoryBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	if prefix == b.failedPrefix {
		return nil, errors.New("injected database listing failure")
	}
	return b.Backend.(storage.DirectoryLister).ListDirectories(ctx, prefix)
}

func TestMeasurements_SkipsUnreadableDatabase(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "gooddb/cpu/2026/07/14/15/good.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "baddb/cpu/2026/07/14/15/bad.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	src := NewStorageWalkSource(&failingDirectoryBackend{
		Backend:      backend,
		failedPrefix: "baddb/",
	}, "arc", zerolog.New(&logs))
	ms, err := src.Measurements(ctx)
	if err != nil {
		t.Fatalf("Measurements: %v", err)
	}
	if len(ms) != 1 || ms[0].Database != "gooddb" || ms[0].Measurement != "cpu" {
		t.Fatalf("Measurements = %+v, want [gooddb/cpu]", ms)
	}
	if !strings.Contains(logs.String(), "baddb") || !strings.Contains(logs.String(), "skipping database") {
		t.Errorf("log = %q, want skipped database and name", logs.String())
	}
}

func TestMeasurements_ReturnsTopLevelEnumerationError(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	src := NewStorageWalkSource(&failingDirectoryBackend{
		Backend:      backend,
		failedPrefix: "",
	}, "arc", zerolog.Nop())
	if _, err := src.Measurements(ctx); err == nil {
		t.Fatal("expected top-level database enumeration error")
	} else if !strings.Contains(err.Error(), "list databases") {
		t.Errorf("Measurements() error = %v, want top-level database listing context", err)
	}
}

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

	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
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

	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
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

// TestIsDataFile covers the export predicate. The dotfile skip is load-bearing: Arc writes
// in-flight files as ".tmp.*", and registering a partially-written Parquet into an Iceberg
// table would hand external readers a corrupt file.
func TestIsDataFile(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{"mydb/cpu/2026/07/14/15/cpu_123.parquet", true},
		{"mydb/cpu/2026/07/14/15/cpu_123_compacted.parquet", true},
		// In-flight write — must never be registered.
		{"mydb/cpu/2026/07/14/15/.tmp.cpu_123.parquet", false},
		// Any other dotfile is skipped too (e.g. editor/OS cruft).
		{"mydb/cpu/2026/07/14/15/.hidden.parquet", false},
		{"mydb/cpu/2026/07/14/15/.DS_Store", false},
		// Non-parquet is not a data file.
		{"mydb/cpu/2026/07/14/15/notes.txt", false},
		{"arc_mydb.db/cpu/metadata/v1.metadata.json", false},
	}
	for _, tt := range tests {
		if got := isDataFile(tt.path); got != tt.want {
			t.Errorf("isDataFile(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

// TestMeasurementsExpandsSpokeNamespaces covers the edge-sync hub layout (#634).
//
// A hub stores received data one level deeper than local data:
// {spoke_id}/{db}/{meas}/{y}/{m}/{d}/{h}/*.parquet. Walked flat, {spoke}/{db}
// reads as (database, measurement), so every measurement under a spoke's
// database is unioned into one table with a franken-schema — or fails schema
// union outright on a cross-measurement type collision and logs an error every
// pass forever.
//
// With the expander wired, each real measurement becomes its own table under a
// {spoke}/{db} pseudo-database.
func TestMeasurementsExpandsSpokeNamespaces(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	// A local database, which must keep its normal two-level shape.
	if err := backend.Write(ctx, "localdb/cpu/2026/07/14/15/f.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	// A hub spoke namespace: rocket-01 holds database "factory" with two measurements.
	for _, p := range []string{
		"rocket-01/factory/cpu/2026/07/14/15/a.parquet",
		"rocket-01/factory/mem/2026/07/14/15/b.parquet",
	} {
		if err := backend.Write(ctx, p, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}

	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	src.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) {
		return map[string]struct{}{"rocket-01": {}}, nil
	})

	ms, err := src.Measurements(ctx)
	if err != nil {
		t.Fatalf("Measurements: %v", err)
	}

	got := map[string]bool{}
	for _, m := range ms {
		got[m.Database+"|"+m.Measurement] = true
	}

	// The spoke's real measurements, under a {spoke}/{db} pseudo-database.
	for _, want := range []string{"rocket-01/factory|cpu", "rocket-01/factory|mem"} {
		if !got[want] {
			t.Errorf("missing %q; got %+v (#634)", want, ms)
		}
	}
	// The local database is untouched by expansion.
	if !got["localdb|cpu"] {
		t.Errorf("local database lost by expansion; got %+v", ms)
	}
	// The un-expanded shape must NOT appear: that is the garbage table.
	if got["rocket-01|factory"] {
		t.Errorf("spoke namespace exported un-expanded as (rocket-01, factory) — every measurement would union into one table (#634)")
	}
}

// TestMeasurementsExpanderFailureSkipsSpokes pins the fail-safe direction: if
// the spoke lookup fails, spoke namespaces are skipped for the pass rather than
// exported un-expanded. Exporting them wrong mints catalog tables that have to
// be cleaned up by hand, so skipping is the cheaper failure (#634).
func TestMeasurementsExpanderFailureSkipsSpokes(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "localdb/cpu/2026/07/14/15/f.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}

	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	src.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) {
		return nil, fmt.Errorf("registry unavailable")
	})

	ms, err := src.Measurements(ctx)
	if err != nil {
		t.Fatalf("Measurements must not fail when the spoke lookup does: %v", err)
	}
	// A lookup failure must not lose ordinary databases.
	found := false
	for _, m := range ms {
		if m.Database == "localdb" && m.Measurement == "cpu" {
			found = true
		}
	}
	if !found {
		t.Errorf("local database dropped after an expander error; got %+v", ms)
	}
}
