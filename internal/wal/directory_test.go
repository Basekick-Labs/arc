package wal

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// The gauge counts every FILE in the WAL directory, not a suffix list: the
// recovery artifacts a suffix list missed — attempt sidecars, leaked write
// temporaries, legacy .recovered files — are exactly what accumulates when
// recovery is failing, which is the state its capacity alert exists for.
func TestDirectoryBytesCountsEveryFileInTheDirectory(t *testing.T) {
	dir := t.TempDir()
	bodies := map[string]string{
		"first.wal":             "wal-data",
		"second.wal.failed":     "retained",
		"third.wal.123.failed":  "quarantined",
		"first.wal.recovery":    "sidecar",
		".wal-recovery-123456":  "leaked-temp",
		"fourth.wal.recovered":  "legacy",
		"unrelated.txt":         "counted",
		"not-wal.failed.backup": "counted2",
	}
	var want int64
	for name, body := range bodies {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
		want += int64(len(body))
	}
	// Directories occupy the directory but are not files; IsRegular excludes
	// them and anything else that is not a plain file.
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0700); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "nested", "deep.wal"), []byte("not counted"), 0600); err != nil {
		t.Fatalf("WriteFile(nested): %v", err)
	}

	got, err := DirectoryBytes(dir)
	if err != nil {
		t.Fatalf("DirectoryBytes: %v", err)
	}
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

// vanishedEntry is a directory entry whose file was removed between the
// ReadDir and the Info — a sidecar write temporary's deferred remove, the
// artifact sweep, or a purge.
type vanishedEntry struct{ os.DirEntry }

func (vanishedEntry) Info() (os.FileInfo, error) {
	return nil, &os.PathError{Op: "lstat", Path: "arc-vanished.wal", Err: fs.ErrNotExist}
}

type sizedEntry struct {
	os.DirEntry
	info os.FileInfo
}

func (e sizedEntry) Info() (os.FileInfo, error) { return e.info, nil }

// A file that disappears mid-walk must not fail the sample: the caller logs the
// error and leaves the gauge at its last value, so an error here silently
// freezes the only disk signal the WAL capacity alert reads.
func TestDirectoryBytesSkipsAnEntryThatVanishedMidWalk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "present.wal")
	if err := os.WriteFile(path, []byte("survivor"), 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	got, err := sumRegularFiles([]os.DirEntry{vanishedEntry{}, sizedEntry{info: info}})
	if err != nil {
		t.Fatalf("sumRegularFiles returned an error for a vanished entry: %v", err)
	}
	if want := int64(len("survivor")); got != want {
		t.Fatalf("sumRegularFiles = %d, want %d", got, want)
	}
}
