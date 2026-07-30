package backup

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

	// 20 readable files plus one missing: a single skip stays under maxSkipRatio,
	// so the backup tolerates it. (See TestCopyDataFiles_SkipRatioExceeded for the
	// case where too many files are skipped.)
	var files []storage.ObjectInfo
	files = append(files, storage.ObjectInfo{Path: "db/cpu/2026/07/28/00/missing.parquet", Size: 100})
	for i := 0; i < 20; i++ {
		f := storage.ObjectInfo{Path: fmt.Sprintf("db/cpu/2026/07/28/00/good_%d.parquet", i), Size: 100}
		if err := dataStorage.Write(ctx, f.Path, bytes.Repeat([]byte("y"), 100)); err != nil {
			t.Fatalf("failed to write test file: %v", err)
		}
		files = append(files, f)
	}

	backupID := "backup-test-skip"
	progress := &Progress{
		Operation:  "backup",
		BackupID:   backupID,
		Status:     "running",
		TotalFiles: int64(len(files)),
	}

	// Should not return error — an isolated unreadable file is skipped
	if err := m.copyDataFiles(ctx, backupID, files, progress); err != nil {
		t.Fatalf("copyDataFiles should not fail on individual file errors: %v", err)
	}

	if progress.ProcessedFiles != 20 {
		t.Errorf("expected 20 processed files (skipped bad file), got %d", progress.ProcessedFiles)
	}
	if progress.SkippedFiles != 1 {
		t.Errorf("expected 1 skipped file, got %d", progress.SkippedFiles)
	}
}

// Skipping tolerates the compaction/retention race, not a storage outage. Once
// more than maxSkipRatio of the files fail to read, the backup must fail rather
// than return a fraction of the data as a success.
func TestCopyDataFiles_SkipRatioExceeded(t *testing.T) {
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

	// 10 files, 5 of them unreadable — 50%, well over the ratio.
	var files []storage.ObjectInfo
	for i := 0; i < 5; i++ {
		f := storage.ObjectInfo{Path: fmt.Sprintf("db/cpu/2026/07/28/00/ok_%d.parquet", i), Size: 100}
		if err := dataStorage.Write(ctx, f.Path, bytes.Repeat([]byte("y"), 100)); err != nil {
			t.Fatalf("failed to write test file: %v", err)
		}
		files = append(files, f)
	}
	for i := 0; i < 5; i++ {
		files = append(files, storage.ObjectInfo{
			Path: fmt.Sprintf("db/cpu/2026/07/28/00/gone_%d.parquet", i),
			Size: 100,
		})
	}

	progress := &Progress{Operation: "backup", TotalFiles: int64(len(files))}
	if err := m.copyDataFiles(ctx, "bkid", files, progress); err != nil {
		t.Fatalf("copyDataFiles should record skips, not fail: %v", err)
	}
	// The ratio is evaluated once over the whole backup, not per copy group.
	err = m.checkSkipRatio(progress, len(files))
	if err == nil {
		t.Fatal("expected failure when the skip ratio is exceeded, got nil (partial backup would report success)")
	}
	if !strings.Contains(err.Error(), "source storage may be degraded") {
		t.Errorf("unexpected error: %v", err)
	}
	// The count must still be recorded even though the backup failed.
	if progress.SkippedFiles != 5 {
		t.Errorf("expected 5 skipped files recorded, got %d", progress.SkippedFiles)
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
// producing an empty backup that reports success. (100% skipped also exceeds
// maxSkipRatio, but this pins the total-loss case explicitly.)
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
	if err := m.copyDataFiles(ctx, "bkid", files, progress); err != nil {
		t.Fatalf("copyDataFiles should record skips, not fail: %v", err)
	}
	if err := m.checkSkipRatio(progress, len(files)); err == nil {
		t.Fatal("expected error when every file is skipped, got nil (empty backup would report success)")
	}
}

// The ratio must span every copy group. A small Iceberg metadata set with one
// stale entry is a large fraction of that set but a negligible fraction of the
// backup, and must not abort a run whose data files all copied.
func TestCheckSkipRatio_SpansAllFileGroups(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}
	m := &Manager{
		dataStorage:   dataStorage,
		backupStorage: mustLocalBackend(t, t.TempDir(), logger),
		logger:        logger,
	}

	// 30 healthy data files.
	var dataFiles []storage.ObjectInfo
	for i := 0; i < 30; i++ {
		p := fmt.Sprintf("db/cpu/2026/07/28/00/f%d.parquet", i)
		if err := dataStorage.Write(ctx, p, bytes.Repeat([]byte("d"), 10)); err != nil {
			t.Fatalf("write: %v", err)
		}
		dataFiles = append(dataFiles, storage.ObjectInfo{Path: p, Size: 10})
	}
	// A 3-file Iceberg metadata set where one entry is already gone — 33% of
	// that group, but only ~3% of the backup.
	if err := dataStorage.Write(ctx, "db/cpu/metadata/a.metadata.json", []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := dataStorage.Write(ctx, "db/cpu/metadata/b.metadata.json", []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	metaFiles := []storage.ObjectInfo{
		{Path: "db/cpu/metadata/a.metadata.json", Size: 1},
		{Path: "db/cpu/metadata/b.metadata.json", Size: 1},
		{Path: "db/cpu/metadata/vanished.metadata.json", Size: 1},
	}

	progress := &Progress{Operation: "backup", TotalFiles: int64(len(dataFiles) + len(metaFiles))}
	if err := m.copyDataFiles(ctx, "bkid", dataFiles, progress); err != nil {
		t.Fatalf("data files: %v", err)
	}
	if err := m.copyDataFiles(ctx, "bkid", metaFiles, progress); err != nil {
		t.Fatalf("iceberg metadata: %v", err)
	}

	// Skips must accumulate across groups, not be overwritten by the last one.
	if progress.SkippedFiles != 1 {
		t.Errorf("SkippedFiles = %d, want 1 (a later group must not erase earlier skips)", progress.SkippedFiles)
	}
	if err := m.checkSkipRatio(progress, len(dataFiles)+len(metaFiles)); err != nil {
		t.Errorf("1 stale entry out of 33 files must not fail the backup: %v", err)
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

	// One skip out of 21 files stays under maxSkipRatio.
	files := []storage.ObjectInfo{{Path: "db/cpu/2026/07/28/00/missing.parquet", Size: 100}}
	for i := 0; i < 20; i++ {
		f := storage.ObjectInfo{Path: fmt.Sprintf("db/cpu/2026/07/28/00/good_%d.parquet", i), Size: 100}
		if err := dataStorage.Write(ctx, f.Path, bytes.Repeat([]byte("y"), 100)); err != nil {
			t.Fatalf("failed to write test file: %v", err)
		}
		files = append(files, f)
	}

	progress := &Progress{Operation: "backup", TotalFiles: int64(len(files))}
	if err := m.copyDataFiles(ctx, "bkid", files, progress); err != nil {
		t.Fatalf("partial skip should not fail the backup: %v", err)
	}

	if progress.ProcessedFiles != 20 {
		t.Errorf("ProcessedFiles = %d, want 20", progress.ProcessedFiles)
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

// partialWriteBackend wraps a real backend and makes WriteReader fail partway
// through, reproducing what LocalBackend does on a transport error: it consumes
// some of the reader, leaves a "<path>.part" staging file behind, and returns an
// error. Delete is delegated so cleanup can be observed.
type partialWriteBackend struct {
	storage.Backend
	dir           string
	deleteCalls   []string
	failWriteWith error
}

func (b *partialWriteBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	if b.failWriteWith == nil {
		return b.Backend.WriteReader(ctx, path, reader, size)
	}
	// Mimic LocalBackend: stage partial bytes to "<path>.part", then fail and
	// deliberately leave the staging file in place.
	stagingPath := filepath.Join(b.dir, path+".part")
	if err := os.MkdirAll(filepath.Dir(stagingPath), 0o700); err != nil {
		return err
	}
	f, err := os.OpenFile(stagingPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, _ = io.CopyN(f, reader, 4) // partial transfer
	f.Close()
	return b.failWriteWith
}

func (b *partialWriteBackend) Delete(ctx context.Context, path string) error {
	b.deleteCalls = append(b.deleteCalls, path)
	return b.Backend.Delete(ctx, path)
}

// A failed backup write must not leave an orphaned ".part" staging file in
// backup storage. LocalBackend keeps it for the replication puller to resume
// from; backup has no resume path, so it is unreferenced garbage.
func TestStreamBackupFile_CleansUpPartFileOnWriteFailure(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	backupBackend := &partialWriteBackend{
		Backend:       mustLocalBackend(t, backupDir, logger),
		dir:           backupDir,
		failWriteWith: fmt.Errorf("simulated transport failure"),
	}
	m := &Manager{dataStorage: dataStorage, backupStorage: backupBackend, logger: logger}

	srcPath := "db/cpu/2026/07/28/00/a.parquet"
	if err := dataStorage.Write(ctx, srcPath, bytes.Repeat([]byte("d"), 64)); err != nil {
		t.Fatalf("failed to write source: %v", err)
	}

	destPath := "bkid/data/" + srcPath
	if _, err := m.streamBackupFile(ctx, srcPath, destPath); err == nil {
		t.Fatal("expected write failure")
	}

	// The staging file must not survive the failure.
	staging := filepath.Join(backupDir, destPath+".part")
	if _, statErr := os.Stat(staging); !os.IsNotExist(statErr) {
		t.Errorf("orphaned .part file left in backup storage: %s (stat err: %v)", staging, statErr)
	}

	// And cleanup must have targeted exactly the staging path.
	if len(backupBackend.deleteCalls) != 1 || backupBackend.deleteCalls[0] != destPath+".part" {
		t.Errorf("expected cleanup of %q, got delete calls %v", destPath+".part", backupBackend.deleteCalls)
	}
}

// A cleanup failure must not mask or replace the original write error.
func TestStreamBackupFile_CleanupFailureDoesNotMaskWriteError(t *testing.T) {
	ctx := context.Background()
	logger := zerolog.Nop()

	dataDir := t.TempDir()
	dataStorage, err := storage.NewLocalBackend(dataDir, logger)
	if err != nil {
		t.Fatalf("failed to create data storage: %v", err)
	}

	backupDir := t.TempDir()
	backupBackend := &deleteFailingBackend{
		Backend:       mustLocalBackend(t, backupDir, logger),
		failWriteWith: fmt.Errorf("simulated transport failure"),
	}
	m := &Manager{dataStorage: dataStorage, backupStorage: backupBackend, logger: logger}

	srcPath := "db/cpu/2026/07/28/00/a.parquet"
	if err := dataStorage.Write(ctx, srcPath, bytes.Repeat([]byte("d"), 64)); err != nil {
		t.Fatalf("failed to write source: %v", err)
	}

	_, err = m.streamBackupFile(ctx, srcPath, "bkid/data/"+srcPath)
	if err == nil {
		t.Fatal("expected write failure")
	}
	if !strings.Contains(err.Error(), "simulated transport failure") {
		t.Errorf("original write error was masked by cleanup failure: %v", err)
	}
	// Must still be fatal, not reclassified as a skippable read error.
	if isSourceReadError(err) {
		t.Errorf("write failure must not be skippable: %v", err)
	}
}

type deleteFailingBackend struct {
	storage.Backend
	failWriteWith error
}

func (b *deleteFailingBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	return b.failWriteWith
}

func (b *deleteFailingBackend) Delete(ctx context.Context, path string) error {
	return fmt.Errorf("simulated cleanup failure")
}

func mustLocalBackend(t *testing.T, dir string, logger zerolog.Logger) storage.Backend {
	t.Helper()
	b, err := storage.NewLocalBackend(dir, logger)
	if err != nil {
		t.Fatalf("failed to create local backend at %s: %v", dir, err)
	}
	return b
}
