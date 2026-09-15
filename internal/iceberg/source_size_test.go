package iceberg

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/storage"
)

func TestFilesAndLocal_PopulatesSize(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string][]byte{
		"mydb/cpu/2026/07/14/15/a.parquet": []byte("PAR1abc"),
		"mydb/cpu/2026/07/14/16/b.parquet": []byte("PAR1abcdefghij"),
	}
	for k, data := range keys {
		if err := backend.Write(ctx, k, data); err != nil {
			t.Fatal(err)
		}
	}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	files, local, err := src.FilesAndLocal(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || len(local) != 2 {
		t.Fatalf("files=%d local=%d, want 2/2", len(files), len(local))
	}
	for _, f := range files {
		rel, _ := filepath.Rel(root, filepath.FromSlash(f.PhysicalPath[len("file://"):]))
		want := int64(len(keys[filepath.ToSlash(rel)]))
		if f.SizeBytes != want {
			t.Errorf("%s: SizeBytes = %d, want %d", f.PhysicalPath, f.SizeBytes, want)
		}
	}
}

// TestFilesAndLocal_DropsFileVanishedAfterListing: a key the listing returned but that no
// longer exists when it is stat'ed (retention or compaction removed it in between) is left out
// of both slices instead of failing the measurement. A dangling symlink is listed by WalkDir
// like any file and fails os.Stat with IsNotExist, which is exactly that race.
func TestFilesAndLocal_DropsFileVanishedAfterListing(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Write(ctx, "mydb/cpu/2026/07/14/15/a.parquet", []byte("PAR1")); err != nil {
		t.Fatal(err)
	}
	gone := filepath.Join(root, "mydb/cpu/2026/07/14/16/gone.parquet")
	if err := os.MkdirAll(filepath.Dir(gone), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "nowhere.parquet"), gone); err != nil {
		t.Fatal(err)
	}
	if keys, err := backend.List(ctx, "mydb/cpu/"); err != nil || len(keys) != 2 {
		t.Fatalf("precondition: listing should return both keys, got %v (%v)", keys, err)
	}
	src := NewStorageWalkSource(backend, "arc", zerolog.Nop())
	files, local, err := src.FilesAndLocal(ctx, Measurement{Database: "mydb", Measurement: "cpu"})
	if err != nil {
		t.Fatalf("a file that vanished after the listing must be skipped, not fail the measurement: %v", err)
	}
	if len(files) != 1 || len(local) != 1 {
		t.Fatalf("files=%d local=%d, want 1/1 (vanished key must leave both slices)", len(files), len(local))
	}
	if filepath.Base(local[0]) != "a.parquet" {
		t.Fatalf("surviving local path = %s, want a.parquet", local[0])
	}
}
