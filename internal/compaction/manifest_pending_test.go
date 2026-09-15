package compaction

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

// TestPendingOutputsUnder (regression, #638): outputs of manifests whose inputs
// still exist are pending; a manifest whose inputs are all gone is committed
// for the exporter's purposes; other measurements' manifests are not returned;
// a manifest deleted between listing and read is treated as absent; an
// unreadable one fails the call.
func TestPendingOutputsUnder(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	mm := NewManifestManager(backend, zerolog.Nop())
	write := func(key string) {
		t.Helper()
		if err := backend.Write(ctx, key, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	// Measurement A: in flight (inputs present).
	write("db/cpu/2026/07/14/15/cpu_a.parquet")
	write("db/cpu/2026/07/14/15/cpu_b.parquet")
	write("db/cpu/2026/07/14/15/cpu_20260714_150000_1_b1_compacted.parquet")
	// Measurement A, another partition: inputs already deleted (delete succeeded, manifest lingered).
	write("db/cpu/2026/07/14/16/cpu_20260714_160000_1_b1_compacted.parquet")
	// Measurement B: in flight, must not leak into A's set.
	write("db/mem/2026/07/14/15/mem_a.parquet")
	write("db/mem/2026/07/14/15/mem_20260714_150000_1_b1_compacted.parquet")
	manifests := []*Manifest{
		{OutputPath: filepath.Join("db", "cpu", "2026", "07", "14", "15", "cpu_20260714_150000_1_b1_compacted.parquet"),
			InputFiles: []string{"db/cpu/2026/07/14/15/cpu_a.parquet", "db/cpu/2026/07/14/15/cpu_b.parquet"},
			Database:   "db", Measurement: "cpu", PartitionPath: "db/cpu/2026/07/14/15", Tier: "hourly",
			Status: ManifestStatusPending, CreatedAt: time.Now().UTC(), JobID: "job-a"},
		{OutputPath: "db/cpu/2026/07/14/16/cpu_20260714_160000_1_b1_compacted.parquet",
			InputFiles: []string{"db/cpu/2026/07/14/16/gone_a.parquet", "db/cpu/2026/07/14/16/gone_b.parquet"},
			Database:   "db", Measurement: "cpu", PartitionPath: "db/cpu/2026/07/14/16", Tier: "hourly",
			Status: ManifestStatusPending, CreatedAt: time.Now().UTC(), JobID: "job-b"},
		{OutputPath: "db/mem/2026/07/14/15/mem_20260714_150000_1_b1_compacted.parquet",
			InputFiles: []string{"db/mem/2026/07/14/15/mem_a.parquet"},
			Database:   "db", Measurement: "mem", PartitionPath: "db/mem/2026/07/14/15", Tier: "hourly",
			Status: ManifestStatusPending, CreatedAt: time.Now().UTC(), JobID: "job-c"},
	}
	var paths []string
	for _, m := range manifests {
		p, err := mm.WriteManifest(ctx, m)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}

	got, err := mm.PendingOutputsUnder(ctx, "db/cpu/")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("pending under db/cpu/ = %v, want only the in-flight partition's output", got)
	}
	if _, ok := got["db/cpu/2026/07/14/15/cpu_20260714_150000_1_b1_compacted.parquet"]; !ok {
		t.Fatalf("in-flight output missing from %v", got)
	}
	// Once every input is gone the output is committed even with the manifest present.
	for _, in := range manifests[0].InputFiles {
		if err := backend.Delete(ctx, in); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := mm.PendingOutputsUnder(ctx, "db/cpu/"); err != nil || len(got) != 0 {
		t.Fatalf("after inputs deleted: pending = %v err = %v, want empty", got, err)
	}
	// A manifest deleted after the listing is absent, not an error.
	if err := backend.Delete(ctx, paths[2]); err != nil {
		t.Fatal(err)
	}
	if got, err := mm.PendingOutputsUnder(ctx, "db/mem/"); err != nil || len(got) != 0 {
		t.Fatalf("deleted manifest: pending = %v err = %v, want empty", got, err)
	}
	// A manifest that exists but cannot be parsed (a zero-length file after a
	// crash, or garbage) is skipped: it names nothing, recovery deletes it,
	// and failing every measurement on every pass over it would be an outage.
	if err := backend.Write(ctx, filepath.ToSlash(filepath.Join(ManifestBasePath, "hourly", "db", "corrupt.json")), []byte("{not json")); err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, filepath.ToSlash(filepath.Join(ManifestBasePath, "hourly", "db", "empty.json")), []byte{}); err != nil {
		t.Fatal(err)
	}
	if got, err := mm.PendingOutputsUnder(ctx, "db/cpu/"); err != nil || len(got) != 0 {
		t.Fatalf("unparseable manifests must be skipped: got %v err %v", got, err)
	}
}
