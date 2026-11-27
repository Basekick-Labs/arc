package storage

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// ListLevel returns one level: the directories directly under the prefix and
// the objects directly in it, as bare names, without the backend prefix, and
// without what List leaves out. Nothing beneath a subdirectory is reported.

func assertLevel(t *testing.T, dirs, files []string, err error, wantDirs, wantFiles string) {
	t.Helper()
	if err != nil {
		t.Fatalf("ListLevel: %v", err)
	}
	sort.Strings(dirs)
	sort.Strings(files)
	if got := strings.Join(dirs, ","); got != wantDirs {
		t.Errorf("dirs = %q, want %q", got, wantDirs)
	}
	if got := strings.Join(files, ","); got != wantFiles {
		t.Errorf("files = %q, want %q", got, wantFiles)
	}
}

func TestLocalListLevel(t *testing.T) {
	root := t.TempDir()
	for _, f := range []string{
		"db/cpu/2026/01/15/cpu_daily.parquet",
		"db/cpu/2026/01/15/notes.txt",
		"db/cpu/2026/01/15/.arc-123.tmp",
		"db/cpu/2026/01/15/10/a.parquet",
		"db/cpu/2026/01/15/11/b.parquet",
		"db/cpu/2026/01/16/10/c.parquet",
	} {
		p := filepath.Join(root, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b, err := NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	ctx := context.Background()

	dirs, files, err := b.ListLevel(ctx, "db/cpu/2026/01/15/")
	assertLevel(t, dirs, files, err, "10,11", "cpu_daily.parquet,notes.txt")

	// A day with only hour directories holds no files of its own.
	dirs, files, err = b.ListLevel(ctx, "db/cpu/2026/01/16/")
	assertLevel(t, dirs, files, err, "10", "")

	// A level that does not exist is empty, not an error.
	dirs, files, err = b.ListLevel(ctx, "db/cpu/2026/01/17/")
	assertLevel(t, dirs, files, err, "", "")

	// ListDirectories is the same listing's directories.
	only, err := b.ListDirectories(ctx, "db/cpu/2026/01/15/")
	assertLevel(t, only, nil, err, "10,11", "")
}

func TestAzureListLevel(t *testing.T) {
	ctx := context.Background()
	blobs := []stubBlob{
		{stubPrefix + "mydb/cpu/2026/01/15/cpu_daily.parquet", 4},
		{stubPrefix + "mydb/cpu/2026/01/15/10/a.parquet", 8},
		{stubPrefix + "mydb/cpu/2026/01/15/11/b.parquet", 8},
		// A key the contract refuses: hidden from files, as List hides it.
		{stubPrefix + `mydb/cpu/2026/01/15/bad\key.parquet`, 32},
		{stubPrefix + "mydb/cpu/2026/01/16/10/c.parquet", 8},
	}

	b, stub := newStubbedAzureBackend(t, stubPrefix, blobs)
	dirs, files, err := b.ListLevel(ctx, "mydb/cpu/2026/01/15")
	assertLevel(t, dirs, files, err, "10,11", "cpu_daily.parquet")
	assertSentPrefix(t, stub, stubPrefix+"mydb/cpu/2026/01/15/")

	b, _ = newStubbedAzureBackend(t, stubPrefix, blobs)
	dirs, files, err = b.ListLevel(ctx, "mydb/cpu/2026/01/16/")
	assertLevel(t, dirs, files, err, "10", "")
}
