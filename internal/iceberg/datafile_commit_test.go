package iceberg

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	iceberg "github.com/apache/iceberg-go"
)

// #939 B1: the exporter commits DataFiles it builds itself instead of handing
// iceberg-go paths to open. The manifests must describe the files exactly as
// AddFiles did: same record counts, sizes, column stats and partition values.
func TestBuildDataFiles_MatchesAddFiles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	base := int64(1_700_000_000_000_000)
	var refs []FileRef
	var paths []string
	for i := 0; i < 3; i++ {
		p := filepath.Join(dir, fmt.Sprintf("f%d.parquet", i))
		writeArcStyleParquet(t, p, base+int64(i)*3_600_000_000, 10+i*7)
		refs = append(refs, refOf(t, p))
		paths = append(paths, fileURI(p))
	}
	sc, err := SchemaFromParquet(filepath.Join(dir, "f0.parquet"))
	if err != nil {
		t.Fatal(err)
	}

	// The exporter's path.
	exp := newTestExporter(t, dir, 0)
	if err := exp.ReconcileMeasurement(ctx, "db", "viaexporter", sc, refs); err != nil {
		t.Fatal(err)
	}
	got := liveDataFiles(t, exp, "viaexporter", sc)

	// iceberg-go's own AddFiles on a twin table with the same schema and spec.
	twin, err := exp.EnsureTable(ctx, "db", "viaaddfiles", sc)
	if err != nil {
		t.Fatal(err)
	}
	txn := twin.NewTransaction()
	if err := txn.AddFiles(ctx, paths, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	want := liveDataFiles(t, exp, "viaaddfiles", sc)

	if len(got) != len(want) || len(got) != 3 {
		t.Fatalf("exporter registered %d files, AddFiles %d, want 3 each", len(got), len(want))
	}
	for p, w := range want {
		g, ok := got[p]
		if !ok {
			t.Fatalf("%s: registered by AddFiles, not by the exporter", p)
		}
		if diff := dataFileDiff(g, w); diff != "" {
			t.Fatalf("%s: %s", p, diff)
		}
	}
}

func liveDataFiles(t *testing.T, exp *Exporter, measurement string, sc ArcSchema) map[string]iceberg.DataFile {
	t.Helper()
	ctx := context.Background()
	tbl, err := exp.EnsureTable(ctx, "db", measurement, sc)
	if err != nil {
		t.Fatal(err)
	}
	live, err := exp.tableLiveFiles(ctx, tbl)
	if err != nil {
		t.Fatal(err)
	}
	return live
}

// dataFileDiff names the first manifest field on which two DataFiles differ.
func dataFileDiff(g, w iceberg.DataFile) string {
	if g.Count() != w.Count() {
		return fmt.Sprintf("record count %d, AddFiles %d", g.Count(), w.Count())
	}
	if g.FileSizeBytes() != w.FileSizeBytes() {
		return fmt.Sprintf("size %d, AddFiles %d", g.FileSizeBytes(), w.FileSizeBytes())
	}
	if g.FileFormat() != w.FileFormat() || g.ContentType() != w.ContentType() {
		return "format or content type differs"
	}
	if fmt.Sprint(g.Partition()) != fmt.Sprint(w.Partition()) {
		return fmt.Sprintf("partition %v, AddFiles %v", g.Partition(), w.Partition())
	}
	for name, pair := range map[string][2]any{
		"column sizes":  {g.ColumnSizes(), w.ColumnSizes()},
		"value counts":  {g.ValueCounts(), w.ValueCounts()},
		"null counts":   {g.NullValueCounts(), w.NullValueCounts()},
		"lower bounds":  {g.LowerBoundValues(), w.LowerBoundValues()},
		"upper bounds":  {g.UpperBoundValues(), w.UpperBoundValues()},
		"split offsets": {g.SplitOffsets(), w.SplitOffsets()},
	} {
		if fmt.Sprint(pair[0]) != fmt.Sprint(pair[1]) {
			return fmt.Sprintf("%s %v, AddFiles %v", name, pair[0], pair[1])
		}
	}
	return ""
}

// A file whose footer disagrees with the size it was listed at (replaced
// since the listing) fails the pass instead of registering stats for bytes
// that are no longer there; the table keeps its previous snapshot.
func TestReconcile_FileChangedSinceListingFailsThePass(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	p := filepath.Join(dir, "f.parquet")
	writeArcStyleParquet(t, p, 1_700_000_000_000_000, 10)
	sc, err := SchemaFromParquet(p)
	if err != nil {
		t.Fatal(err)
	}
	exp := newTestExporter(t, dir, 0)
	ref := refOf(t, p)
	ref.SizeBytes += 100
	if err := exp.ReconcileMeasurement(ctx, "db", "cpu", sc, []FileRef{ref}); err == nil {
		t.Fatal("a size mismatch between the listing and the file was registered")
	}
	if live := liveDataFiles(t, exp, "cpu", sc); len(live) != 0 {
		t.Fatalf("table holds %d files after a failed pass, want 0", len(live))
	}
}
