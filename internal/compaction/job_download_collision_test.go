package compaction

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestDownloadSameBasenameKeepsDistinctContents(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	ctx := context.Background()
	basename := "cpu_20260411_140001_123456789.parquet"

	keys := []string{
		"testdb/cpu/2026/04/11/14/" + basename,
		"testdb/cpu/2026/04/11/15/" + basename,
	}

	contents := [][]byte{
		[]byte("original-content-from-hour-14"),
		[]byte("different-content-from-hour-15"),
	}

	for i, key := range keys {
		if err := backend.Write(ctx, key, contents[i]); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}

	job := newDownloadJob(t, backend, keys)
	tempDir := t.TempDir()

	first := job.downloadSingleFile(ctx, tempDir, 0, keys[0])
	if first.err != nil {
		t.Fatalf("first download: %v", first.err)
	}

	second := job.downloadSingleFile(ctx, tempDir, 1, keys[1])
	if second.err != nil {
		t.Fatalf("second download: %v", second.err)
	}

	if first.file == nil || second.file == nil {
		t.Fatal("both downloads should return files")
	}

	if first.file.localPath == second.file.localPath {
		t.Fatalf(
			"colliding source keys share temporary path: %q",
			first.file.localPath,
		)
	}

	for i, result := range []downloadResult{first, second} {
		got, err := os.ReadFile(result.file.localPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, contents[i]) {
			t.Fatalf("download %d has incorrect contents: %q", i, got)
		}
	}
}

func TestDownloadRejectsExistingTemporaryFile(t *testing.T) {
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	ctx := context.Background()
	key := "testdb/cpu/2026/04/11/14/a.parquet"

	if err := backend.Write(ctx, key, []byte("source")); err != nil {
		t.Fatal(err)
	}

	job := newDownloadJob(t, backend, []string{key})
	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "0_a.parquet")

	if err := os.WriteFile(target, []byte("sentinel"), 0600); err != nil {
		t.Fatal(err)
	}

	result := job.downloadSingleFile(ctx, tempDir, 0, key)
	if result.err == nil {
		t.Fatal("existing temporary file must not be overwritten")
	}

	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}

	if string(got) != "sentinel" {
		t.Fatalf("existing file was modified: %q", got)
	}
}
