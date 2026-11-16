package compaction

import (
	"context"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/iceberg"
	"github.com/rs/zerolog"
)

// The real pieces together: OpenJobs as the export's exclusions and the
// manifest manager's PendingOutputsUnder as its pending outputs. Through a
// legacy job's window the export lists the job's rows exactly once (#1722).
func TestIcebergSourceSeesAJobsRowsOnceThroughItsWindow(t *testing.T) {
	ctx := context.Background()
	o, b := openJobsRig(t)
	mm := NewManifestManager(b, zerolog.Nop())
	src := iceberg.NewStorageWalkSource(b, "arc", zerolog.Nop())
	src.SetPendingOutputs(mm.PendingOutputsUnder)
	src.SetExclusions(o)

	dir := "db/cpu/2026/10/20/10/"
	in1, in2, out := dir+"cpu_a.parquet", dir+"cpu_b.parquet", dir+"cpu_compacted.parquet"
	for _, k := range []string{in1, in2} {
		if err := b.Write(ctx, k, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
	}
	listed := func() string {
		t.Helper()
		files, _, err := src.FilesAndLocal(ctx, iceberg.Measurement{Database: "db", Measurement: "cpu"})
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, f := range files {
			names = append(names, filepath.Base(f.PhysicalPath))
		}
		sort.Strings(names)
		return strings.Join(names, ",")
	}

	// The job: its manifest written and the parent's entry registered, then
	// the output uploaded; the inputs still there (a failed delete, or the
	// moment before it).
	manPath, err := mm.WriteManifest(ctx, &Manifest{OutputPath: out, InputFiles: []string{in1, in2},
		Database: "db", Measurement: "cpu", Status: ManifestStatusPending})
	if err != nil {
		t.Fatal(err)
	}
	o.register(out, manPath, []string{in1, in2})
	if got := listed(); got != "cpu_a.parquet,cpu_b.parquet" {
		t.Fatalf("before the upload: %s, want the inputs", got)
	}
	if err := b.Write(ctx, out, []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	if got := listed(); got != "cpu_compacted.parquet" {
		t.Fatalf("output uploaded, inputs present: %s, want the output only", got)
	}
	// One input deleted, the other's delete failed: still the output only.
	if err := b.Delete(ctx, in1); err != nil {
		t.Fatal(err)
	}
	if got := listed(); got != "cpu_compacted.parquet" {
		t.Fatalf("delete failed part-way: %s, want the output only", got)
	}
}
