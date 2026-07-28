package backup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func TestStreamBackupFile(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	// Set up data storage with a test file
	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	// Set up backup storage
	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}

	// Write a test file to data storage
	testData := bytes.Repeat([]byte("parquet-data-line\n"), 1000)
	srcPath := "testdb/cpu/2026/07/28/00/data_001.parquet"
	if err := dataStorage.Write(ctx, srcPath, testData); err != nil {
		t.Fatalf("failed to write test data: %v", err)
	}

	// Keep temp files inside the test's own dir so the cleanup assertion below
	// cannot be confused by unrelated processes using the shared temp dir.
	t.Setenv("TMPDIR", t.TempDir())

	// Stream backup
	destPath := "backup-123/data/" + srcPath
	written, err := m.streamBackupFile(ctx, srcPath, destPath)
	if err != nil {
		t.Fatalf("streamBackupFile failed: %v", err)
	}
	if written != int64(len(testData)) {
		t.Errorf("streamBackupFile returned %d bytes, want %d", written, len(testData))
	}

	// Verify the backup file exists and has correct content
	backedUp, err := m.backupStorage.Read(ctx, destPath)
	if err != nil {
		t.Fatalf("failed to read backup file: %v", err)
	}
	if !bytes.Equal(backedUp, testData) {
		t.Errorf("backup data mismatch: got %d bytes, want %d bytes", len(backedUp), len(testData))
	}

	// Verify no temp files left behind
	matches, _ := filepath.Glob(filepath.Join(os.TempDir(), "arc-backup-*.parquet"))
	if len(matches) != 0 {
		t.Errorf("temp files not cleaned up: %v", matches)
	}
}

func TestStreamBackupFile_SourceNotFound(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}

	_, err = m.streamBackupFile(ctx, "nonexistent.parquet", "backup/data/nonexistent.parquet")
	if err == nil {
		t.Fatal("expected error for nonexistent source file")
	}
	// A missing source must classify as a skippable read error, otherwise a
	// benign delete-during-backup race would abort the whole backup.
	if !isSourceReadError(err) {
		t.Errorf("missing source should be a skippable read error, got %v", err)
	}
}

func TestCopyDataFiles_StreamsMultipleFiles(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}

	// Write test files to data storage
	files := []storage.ObjectInfo{
		{Path: "db1/cpu/2026/07/28/00/f1.parquet", Size: 500},
		{Path: "db1/cpu/2026/07/28/00/f2.parquet", Size: 300},
		{Path: "db2/mem/2026/07/28/00/f3.parquet", Size: 200},
	}
	for _, f := range files {
		data := bytes.Repeat([]byte("x"), int(f.Size))
		if err := dataStorage.Write(ctx, f.Path, data); err != nil {
			t.Fatalf("failed to write test file %s: %v", f.Path, err)
		}
	}

	backupID := "backup-test-stream"
	progress := &Progress{
		Operation:  "backup",
		BackupID:   backupID,
		Status:     "running",
		TotalFiles: int64(len(files)),
	}

	if err := m.copyDataFiles(ctx, backupID, files, progress); err != nil {
		t.Fatalf("copyDataFiles failed: %v", err)
	}

	// Verify all files were backed up
	if progress.ProcessedFiles != 3 {
		t.Errorf("expected 3 processed files, got %d", progress.ProcessedFiles)
	}
	if progress.ProcessedBytes != 1000 {
		t.Errorf("expected 1000 processed bytes, got %d", progress.ProcessedBytes)
	}

	// Verify each file exists in backup storage
	for _, f := range files {
		destPath := backupID + "/data/" + f.Path
		backedUp, err := m.backupStorage.Read(ctx, destPath)
		if err != nil {
			t.Errorf("backup file %s not found: %v", destPath, err)
			continue
		}
		if int64(len(backedUp)) != f.Size {
			t.Errorf("backup file %s: got %d bytes, want %d", destPath, len(backedUp), f.Size)
		}
	}
}

func TestCopyDataFiles_SkipsFailedFiles(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}

	// Write only one file; the other will fail to read
	goodFile := storage.ObjectInfo{Path: "db/cpu/2026/07/28/00/good.parquet", Size: 100}
	if err := dataStorage.Write(ctx, goodFile.Path, bytes.Repeat([]byte("y"), 100)); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	badFile := storage.ObjectInfo{Path: "db/cpu/2026/07/28/00/missing.parquet", Size: 100}

	backupID := "backup-test-skip"
	progress := &Progress{
		Operation:  "backup",
		BackupID:   backupID,
		Status:     "running",
		TotalFiles: 2,
	}

	// Should not return error — failed files are skipped
	if err := m.copyDataFiles(ctx, backupID, []storage.ObjectInfo{badFile, goodFile}, progress); err != nil {
		t.Fatalf("copyDataFiles should not fail on individual file errors: %v", err)
	}

	// Only the good file should have been processed
	if progress.ProcessedFiles != 1 {
		t.Errorf("expected 1 processed file (skipped bad file), got %d", progress.ProcessedFiles)
	}
}

func TestIsSourceReadError(t *testing.T) {
	readErr := fmt.Errorf("failed to read from data storage: %w: %w", errBackupRead, fmt.Errorf("file not found"))
	if !isSourceReadError(readErr) {
		t.Error("source read error should be classified as skippable")
	}

	// Everything not explicitly tagged must be fatal — classification is positive,
	// not by exclusion, so new failure modes default to aborting the backup.
	for _, err := range []error{
		fmt.Errorf("failed to create temp file: %w", fmt.Errorf("no space left on device")),
		fmt.Errorf("failed to stat temp file: %w", fmt.Errorf("bad fd")),
		fmt.Errorf("failed to seek temp file: %w", fmt.Errorf("bad fd")),
		fmt.Errorf("failed to write to backup storage: %w", fmt.Errorf("disk full")),
	} {
		if isSourceReadError(err) {
			t.Errorf("non-source-read error must be fatal, got skippable: %v", err)
		}
	}
}

// Regression guard for the silent-skip bug: when the temp directory is unusable
// (small/absent tmpfs in a container), os.CreateTemp fails for every file. That
// must abort the backup, not skip every file and report success.
func TestCopyDataFiles_TempFileFailureIsFatal(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}

	files := []storage.ObjectInfo{
		{Path: "db/cpu/2026/07/28/00/a.parquet", Size: 10},
		{Path: "db/cpu/2026/07/28/00/b.parquet", Size: 10},
	}
	for _, f := range files {
		if err := dataStorage.Write(ctx, f.Path, bytes.Repeat([]byte("z"), int(f.Size))); err != nil {
			t.Fatalf("failed to write test file: %v", err)
		}
	}

	// Unusable temp dir => os.CreateTemp fails for every file.
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "does-not-exist"))

	progress := &Progress{Operation: "backup", TotalFiles: int64(len(files))}
	err = m.copyDataFiles(ctx, "bkid", files, progress)

	if err == nil {
		t.Fatalf("expected fatal error when temp files cannot be created; got nil with %d/%d files copied",
			progress.ProcessedFiles, len(files))
	}
	if isSourceReadError(err) {
		t.Errorf("temp file failure must not be classified as a skippable read error: %v", err)
	}
}

// A backup whose every file is unreadable must fail rather than silently
// producing an empty backup that reports success.
func TestCopyDataFiles_AllFilesSkippedIsFatal(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}

	// None of these exist in data storage — every read fails.
	files := []storage.ObjectInfo{
		{Path: "db/cpu/2026/07/28/00/gone1.parquet", Size: 10},
		{Path: "db/cpu/2026/07/28/00/gone2.parquet", Size: 10},
	}

	progress := &Progress{Operation: "backup", TotalFiles: int64(len(files))}
	if err := m.copyDataFiles(ctx, "bkid", files, progress); err == nil {
		t.Fatal("expected error when every file is skipped, got nil (empty backup would report success)")
	}
}

// A partial skip is tolerated (the file may have been compacted away mid-backup)
// but must be counted so the manifest does not overstate the backup's contents.
func TestCopyDataFiles_PartialSkipRecordsCount(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}

	good := storage.ObjectInfo{Path: "db/cpu/2026/07/28/00/good.parquet", Size: 100}
	if err := dataStorage.Write(ctx, good.Path, bytes.Repeat([]byte("y"), 100)); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	missing := storage.ObjectInfo{Path: "db/cpu/2026/07/28/00/missing.parquet", Size: 100}

	progress := &Progress{Operation: "backup", TotalFiles: 2}
	if err := m.copyDataFiles(ctx, "bkid", []storage.ObjectInfo{missing, good}, progress); err != nil {
		t.Fatalf("partial skip should not fail the backup: %v", err)
	}

	if progress.ProcessedFiles != 1 {
		t.Errorf("ProcessedFiles = %d, want 1", progress.ProcessedFiles)
	}
	if progress.SkippedFiles != 1 {
		t.Errorf("SkippedFiles = %d, want 1", progress.SkippedFiles)
	}
}

// Progress bytes must reflect what was actually copied, not the (possibly stale)
// size from the listing.
func TestCopyDataFiles_ProgressUsesActualBytes(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, backupDir, logger),
		logger:        logger,
	}

	// Actual file is 50 bytes, but the listing claims 999 (stale entry).
	path := "db/cpu/2026/07/28/00/stale.parquet"
	if err := dataStorage.Write(ctx, path, bytes.Repeat([]byte("q"), 50)); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	files := []storage.ObjectInfo{{Path: path, Size: 999}}

	progress := &Progress{Operation: "backup", TotalFiles: 1}
	if err := m.copyDataFiles(ctx, "bkid", files, progress); err != nil {
		t.Fatalf("copyDataFiles failed: %v", err)
	}

	if progress.ProcessedBytes != 50 {
		t.Errorf("ProcessedBytes = %d, want 50 (actual bytes, not the stale listing size)", progress.ProcessedBytes)
	}
}

func mustLocalBackend(t *testing.T, dir string, logger zerolog.Logger) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(dir, logger)
	if err != nil {
		t.Fatalf("failed to create local backend at %s: %v", dir, err)
	}
	return b
}
