package database

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestCleanupOrphanedSpillFiles_EmptyPath(t *testing.T) {
	if err := CleanupOrphanedSpillFiles("", zerolog.Nop()); err != nil {
		t.Fatalf("expected nil err for empty path, got %v", err)
	}
}

func TestCleanupOrphanedSpillFiles_NonExistentPath(t *testing.T) {
	if err := CleanupOrphanedSpillFiles("/nonexistent/path/should/not/exist", zerolog.Nop()); err != nil {
		t.Fatalf("expected nil err for non-existent path, got %v", err)
	}
}

func TestCleanupOrphanedSpillFiles_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	if err := CleanupOrphanedSpillFiles(dir, zerolog.Nop()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestCleanupOrphanedSpillFiles_MixedFiles(t *testing.T) {
	dir := t.TempDir()

	// Files the sweep should remove (correct prefix + .tmp suffix, old mtime)
	spills := map[string]int64{
		"duckdb_temp_storage_DEFAULT-0.tmp": 1024,
		"duckdb_temp_storage_S32K-3.tmp":    2048,
		"duckdb_temp_storage_S128K-0.tmp":   4096,
	}
	// Files the sweep must NOT touch
	keeps := []string{
		"something_else.tmp",            // wrong prefix
		"duckdb_temp_storage_DEFAULT-0", // missing .tmp suffix
		"DUCKDB_TEMP_STORAGE_X.tmp",     // wrong case
		"important.parquet",             // unrelated
	}

	for name, size := range spills {
		path := filepath.Join(dir, name)
		tMkSpillFile(t, path, size)
		tAgeFile(t, path, 2*spillFileLiveThreshold)
	}
	for _, name := range keeps {
		path := filepath.Join(dir, name)
		tMkSpillFile(t, path, 512)
		tAgeFile(t, path, 2*spillFileLiveThreshold)
	}

	if err := CleanupOrphanedSpillFiles(dir, zerolog.Nop()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	// Verify the keep set is intact.
	for _, name := range keeps {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("expected %s to survive sweep, got err: %v", name, err)
		}
	}
	// Verify the spill set is gone.
	for name := range spills {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be removed, stat err = %v", name, err)
		}
	}
}

func TestCleanupOrphanedSpillFiles_SkipsRecent(t *testing.T) {
	dir := t.TempDir()

	old := filepath.Join(dir, "duckdb_temp_storage_S32K-0.tmp")
	recent := filepath.Join(dir, "duckdb_temp_storage_S32K-1.tmp")
	tMkSpillFile(t, old, 100)
	tMkSpillFile(t, recent, 200)
	tAgeFile(t, old, 2*spillFileLiveThreshold)
	// recent left at "now" — should be skipped

	if err := CleanupOrphanedSpillFiles(dir, zerolog.Nop()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, err := os.Stat(recent); err != nil {
		t.Fatalf("expected recent file to survive sweep, got %v", err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("expected old file to be removed, stat err = %v", err)
	}
}

func TestCleanupOrphanedSpillFiles_IgnoresSubdirs(t *testing.T) {
	dir := t.TempDir()

	// A subdirectory whose name matches the spill prefix is ignored — we
	// only remove regular files. Validated against DuckDB 1.5.1 (flat
	// layout, no nesting). If a future DuckDB version nests per-query
	// subdirs, this test will catch the regression: orphans will return
	// and the sweep will silently no-op. At that point the sweep must
	// switch to filepath.WalkDir.
	subdir := filepath.Join(dir, "duckdb_temp_storage_DEFAULT-0.tmp")
	if err := os.Mkdir(subdir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	if err := CleanupOrphanedSpillFiles(dir, zerolog.Nop()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if _, err := os.Stat(subdir); err != nil {
		t.Fatalf("expected subdir to survive sweep, got %v", err)
	}
}

// tMkSpillFile creates a file of the given size at path. Renamed from
// writeFile to avoid collisions with stdlib/package-local helpers.
func tMkSpillFile(t *testing.T, path string, size int64) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	if size > 0 {
		if err := f.Truncate(size); err != nil {
			t.Fatalf("truncate %s: %v", path, err)
		}
	}
}

// tAgeFile sets the mtime of path to (now - age).
func tAgeFile(t *testing.T, path string, age time.Duration) {
	t.Helper()
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatalf("chtimes %s: %v", path, err)
	}
}
