package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirectoryBytesIncludesWALAndQuarantinedFiles(t *testing.T) {
	dir := t.TempDir()
	for name, body := range map[string]string{
		"first.wal":             "wal-data",
		"second.wal.failed":     "retained",
		"third.wal.123.failed":  "quarantined",
		"unrelated.txt":         "ignored",
		"not-wal.failed.backup": "ignored",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}

	got, err := DirectoryBytes(dir)
	if err != nil {
		t.Fatalf("DirectoryBytes: %v", err)
	}
	want := int64(len("wal-data") + len("retained") + len("quarantined"))
	if got != want {
		t.Fatalf("DirectoryBytes = %d, want %d", got, want)
	}
}

func TestDirectoryBytesMissingDirectoryIsZero(t *testing.T) {
	got, err := DirectoryBytes(filepath.Join(t.TempDir(), "missing"))
	if err != nil {
		t.Fatalf("DirectoryBytes: %v", err)
	}
	if got != 0 {
		t.Fatalf("DirectoryBytes = %d, want 0", got)
	}
}
