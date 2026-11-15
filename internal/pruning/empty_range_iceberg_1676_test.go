package pruning

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// #1676: a colocated Iceberg table keeps its metadata in the measurement's
// own directory. That child must not turn off the empty-range proof, but a
// "metadata" child holding Parquet must, because `**/*.parquet` reads it.
func TestLayoutIsStandardSetsAsideAnIcebergMetadataDir(t *testing.T) {
	root := t.TempDir()
	meas := filepath.Join(root, "db", "cpu")
	if err := os.MkdirAll(filepath.Join(meas, "2026", "11", "13", "10"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(meas, "metadata"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"v1.metadata.json", "version-hint.text", "snap-1.avro"} {
		if err := os.WriteFile(filepath.Join(meas, "metadata", f), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	p := NewPartitionPruner(zerolog.Nop())
	if !p.layoutIsStandard(context.Background(), root, "db", "cpu") {
		t.Fatal("an Iceberg metadata directory must not make the layout non-standard")
	}

	if err := os.WriteFile(filepath.Join(meas, "metadata", "stray.parquet"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	p.globCache.invalidate()
	if p.layoutIsStandard(context.Background(), root, "db", "cpu") {
		t.Fatal("a metadata directory holding Parquet must make the layout non-standard")
	}

	// Only metadata, no year directory: not a standard layout.
	only := filepath.Join(root, "db", "mem", "metadata")
	if err := os.MkdirAll(only, 0o755); err != nil {
		t.Fatal(err)
	}
	if p.layoutIsStandard(context.Background(), root, "db", "mem") {
		t.Fatal("a measurement with only a metadata directory is not a standard layout")
	}
}
