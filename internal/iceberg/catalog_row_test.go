package iceberg

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEnsureTable_CatalogRowWithoutMetadataIsActionable (regression, #637): a
// catalog row whose metadata file is gone — a backup restored without its
// outside-root warehouse — must produce an actionable error, not a CreateTable
// attempt. On main EnsureTable fell through to CreateTable, which wrote a fresh
// metadata file and then failed on the catalog's primary key, on every pass.
func TestEnsureTable_CatalogRowWithoutMetadataIsActionable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	f1 := filepath.Join(dir, "data", "cpu_h1.parquet")
	writeArcStyleParquet(t, f1, 1_700_000_000_000_000, 10)
	exp := newTestExporter(t, dir, 0)
	sc, err := SchemaFromParquet(f1)
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := exp.EnsureTable(ctx, "mydb", "cpu", sc)
	if err != nil {
		t.Fatal(err)
	}
	metaDir := filepath.Dir(strings.TrimPrefix(tbl.MetadataLocation(), "file://"))
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".metadata.json") {
			if err := os.Remove(filepath.Join(metaDir, e.Name())); err != nil {
				t.Fatal(err)
			}
		}
	}
	before, _ := os.ReadDir(metaDir)

	_, err = exp.EnsureTable(ctx, "mydb", "cpu", sc)
	if err == nil {
		t.Fatal("EnsureTable succeeded with the metadata file missing")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error does not wrap the missing-file cause: %v", err)
	}
	if !strings.Contains(err.Error(), "in the catalog") || !strings.Contains(err.Error(), "iceberg.warehouse") {
		t.Errorf("error is not actionable: %v", err)
	}
	if strings.Contains(err.Error(), "failed to create table") {
		t.Errorf("EnsureTable still fell through to CreateTable: %v", err)
	}
	after, _ := os.ReadDir(metaDir)
	if len(after) != len(before) {
		t.Errorf("metadata dir changed from %d to %d entries: EnsureTable wrote an orphan metadata file", len(before), len(after))
	}
}

func TestLocalWarehousePath(t *testing.T) {
	cwd, _ := os.Getwd()
	cases := []struct {
		raw  string
		want string
		ok   bool
	}{
		{"file:///var/lib/arc/wh", filepath.Clean("/var/lib/arc/wh"), true},
		{"file:///var/lib/arc/wh/", filepath.Clean("/var/lib/arc/wh"), true},
		{"/var/lib/arc/wh", filepath.Clean("/var/lib/arc/wh"), true},
		{"./wh", filepath.Join(cwd, "wh"), true},
		{"file://./wh", filepath.Join(cwd, "wh"), true},
		{"s3://bucket/prefix", "", false},
		{"azure://container", "", false},
		{"", "", false},
		{"file://", "", false},
	}
	for _, c := range cases {
		got, ok := LocalWarehousePath(c.raw)
		if ok != c.ok || got != c.want {
			t.Errorf("LocalWarehousePath(%q) = %q, %v; want %q, %v", c.raw, got, ok, c.want, c.ok)
		}
	}
}
