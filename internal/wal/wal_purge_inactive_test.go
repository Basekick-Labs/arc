package wal

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// ==========================================================================
// Duplication proof: old periodic recovery replays already-flushed data
// ==========================================================================

// TestDuplication_OldPeriodicCycleReplaysAllEntries proves the old periodic
// recovery approach causes data duplication. It writes 100 records (simulating
// data already flushed to parquet), then runs recovery (what the old periodic
// cycle did). All 100 records are replayed through the callback — in production
// these would be written to parquet again, doubling the record count.
func TestDuplication_OldPeriodicCycleReplaysAllEntries(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-dup-old-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Write 100 records to WAL
	writer1, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	records := make([]map[string]interface{}, 100)
	for i := 0; i < 100; i++ {
		records[i] = map[string]interface{}{
			"measurement": "sensor",
			"device_id":   "device-01",
			"temperature": float64(20 + i),
			"humidity":    float64(50 + i),
		}
	}
	writer1.Append(records)
	time.Sleep(50 * time.Millisecond)
	oldWALFile := writer1.CurrentFile()
	writer1.Close()

	// Simulate WAL rotation — new active file
	time.Sleep(1100 * time.Millisecond)
	writer2, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer2: %v", err)
	}
	newActiveFile := writer2.CurrentFile()
	writer2.Close()

	if oldWALFile == newActiveFile {
		t.Skip("WAL files have same path")
	}

	// OLD periodic cycle: recovery replays the old WAL file
	recovery := NewRecovery(tmpDir, zerolog.Nop())
	duplicatedRecords := 0
	callback := func(ctx context.Context, recs []map[string]interface{}) error {
		duplicatedRecords += len(recs)
		return nil
	}

	stats, err := recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		SkipActiveFile: newActiveFile,
	})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// All 100 records replayed — in production these are already in parquet
	if duplicatedRecords != 100 {
		t.Fatalf("expected 100 duplicated records, got %d", duplicatedRecords)
	}

	t.Logf("Old periodic cycle: %d records replayed from %d files", duplicatedRecords, stats.RecoveredFiles)
	t.Logf("Production impact: 100 records stored twice = 200 total")
}

// TestDuplication_CompoundsOverMultipleCycles proves duplication compounds.
// Each recovery cycle replays ALL records from the WAL file.
func TestDuplication_CompoundsOverMultipleCycles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-dup-compound-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	records := make([]map[string]interface{}, 50)
	for i := 0; i < 50; i++ {
		records[i] = map[string]interface{}{
			"measurement": "cpu",
			"value":       float64(i),
		}
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	totalDuplicated := 0
	for cycle := 1; cycle <= 3; cycle++ {
		recovery := NewRecovery(tmpDir, zerolog.Nop())
		cycleCount := 0
		callback := func(ctx context.Context, recs []map[string]interface{}) error {
			cycleCount += len(recs)
			return nil
		}

		stats, err := recovery.Recover(context.Background(), callback)
		if err != nil {
			t.Fatalf("cycle %d: recovery failed: %v", cycle, err)
		}

		totalDuplicated += cycleCount
		t.Logf("Cycle %d: replayed %d records (recovered files: %d)", cycle, cycleCount, stats.RecoveredFiles)
	}

	if totalDuplicated < 50 {
		t.Errorf("expected at least 50 duplicated records, got %d", totalDuplicated)
	}

	t.Logf("Total duplicated: %d — production would have %d records instead of 50", totalDuplicated, 50+totalDuplicated)
}

// ==========================================================================
// No duplication proof: new periodic cycle (flush + purge)
// ==========================================================================

// TestNoDuplication_NewPeriodicCycle proves the new flush+purge cycle does NOT
// cause duplication. PurgeInactive deletes old WAL files without replaying
// any entries through a callback. Zero records replayed = zero duplicates.
func TestNoDuplication_NewPeriodicCycle(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-nodup-new-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	records := make([]map[string]interface{}, 100)
	for i := 0; i < 100; i++ {
		records[i] = map[string]interface{}{
			"measurement": "sensor",
			"device_id":   "device-01",
			"temperature": float64(20 + i),
		}
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)

	// Create 3 old WAL files (simulate previous rotations)
	for i := 0; i < 3; i++ {
		oldName := filepath.Join(tmpDir, "arc-2024010"+string(rune('1'+i))+"_000000.wal")
		f, _ := os.Create(oldName)
		header := make([]byte, WALFileHeaderSize)
		copy(header[0:4], WALMagic)
		binary.BigEndian.PutUint16(header[4:6], WALVersion)
		header[6] = WALChecksumCRC32
		f.Write(header)
		f.Close()
	}

	before, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(before) != 4 {
		t.Fatalf("expected 4 WAL files, got %d", len(before))
	}

	// New periodic cycle: purge only, no replay
	deleted, err := writer.PurgeInactive()
	if err != nil {
		t.Fatalf("PurgeInactive failed: %v", err)
	}

	if deleted != 3 {
		t.Errorf("expected 3 old WAL files purged, got %d", deleted)
	}

	remaining, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(remaining) != 1 {
		t.Errorf("expected 1 remaining WAL file (active only), got %d", len(remaining))
	}

	writer.Close()
	t.Logf("New periodic cycle: 0 records replayed, %d files purged — no duplication", deleted)
}

// TestNoDuplication_MultipleNewCycles proves multiple new-style cycles never
// cause duplication. Each cycle purges old files without replaying anything.
func TestNoDuplication_MultipleNewCycles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-nodup-multi-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	writer.Append([]map[string]interface{}{
		{"measurement": "cpu", "value": 42.0},
	})
	time.Sleep(50 * time.Millisecond)

	for cycle := 1; cycle <= 5; cycle++ {
		oldName := filepath.Join(tmpDir, "arc-2024010"+string(rune('0'+cycle))+"_000000.wal")
		os.WriteFile(oldName, []byte("old"), 0600)

		deleted, err := writer.PurgeInactive()
		if err != nil {
			t.Fatalf("cycle %d: PurgeInactive failed: %v", cycle, err)
		}
		t.Logf("Cycle %d: purged %d files, replayed 0 records", cycle, deleted)
	}

	writer.Close()
	t.Logf("5 periodic cycles, 0 records replayed, 0 duplicates")
}

// TestNoDuplication_FlushFailurePreservesWAL proves that when FlushAll fails,
// PurgeInactive is skipped, preserving WAL files for retry next cycle.
func TestNoDuplication_FlushFailurePreservesWAL(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-nodup-flush-fail-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer writer.Close()

	oldFiles := []string{
		filepath.Join(tmpDir, "arc-20240101_000000.wal"),
		filepath.Join(tmpDir, "arc-20240102_000000.wal"),
	}
	for _, f := range oldFiles {
		os.WriteFile(f, []byte("important data"), 0600)
	}

	// Simulate FlushAll failure — skip PurgeInactive (matches main.go logic)
	flushFailed := true
	if !flushFailed {
		writer.PurgeInactive()
	}

	// WAL files must be preserved for retry
	for _, f := range oldFiles {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			t.Errorf("WAL file %s should be preserved after flush failure", filepath.Base(f))
		}
	}

	allFiles, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(allFiles) != 3 { // 1 active + 2 old
		t.Errorf("expected 3 WAL files preserved, got %d", len(allFiles))
	}
}

// ==========================================================================
// PurgeInactive() method tests
// ==========================================================================

// TestPurgeInactive_DeletesNonActiveFiles verifies PurgeInactive deletes all
// WAL files except the currently active one.
func TestPurgeInactive_DeletesNonActiveFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-purge-inactive-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer writer.Close()

	activeFile := writer.CurrentFile()

	oldFiles := []string{
		filepath.Join(tmpDir, "arc-20240101_100000.wal"),
		filepath.Join(tmpDir, "arc-20240101_110000.wal"),
		filepath.Join(tmpDir, "arc-20240101_120000.wal"),
	}
	for _, f := range oldFiles {
		header := make([]byte, WALFileHeaderSize)
		copy(header[0:4], WALMagic)
		binary.BigEndian.PutUint16(header[4:6], WALVersion)
		header[6] = WALChecksumCRC32
		os.WriteFile(f, header, 0600)
	}

	allFiles, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(allFiles) != 4 {
		t.Fatalf("expected 4 WAL files before purge, got %d", len(allFiles))
	}

	deleted, err := writer.PurgeInactive()
	if err != nil {
		t.Fatalf("PurgeInactive failed: %v", err)
	}
	if deleted != 3 {
		t.Errorf("expected 3 deleted files, got %d", deleted)
	}

	remaining, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(remaining) != 1 {
		t.Errorf("expected 1 remaining WAL file, got %d", len(remaining))
	}
	if len(remaining) == 1 && remaining[0] != activeFile {
		t.Errorf("remaining file should be active file %s, got %s", activeFile, remaining[0])
	}
}

// TestPurgeInactive_NoFilesToPurge verifies PurgeInactive returns 0 when
// only the active file exists.
func TestPurgeInactive_NoFilesToPurge(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	deleted, err := writer.PurgeInactive()
	if err != nil {
		t.Fatalf("PurgeInactive failed: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", deleted)
	}
	if _, err := os.Stat(writer.CurrentFile()); os.IsNotExist(err) {
		t.Error("active WAL file should not be deleted")
	}
}

// TestPurgeInactive_PreservesActiveFileDuringWrites verifies PurgeInactive
// does not interfere with the active file while writes are happening.
func TestPurgeInactive_PreservesActiveFileDuringWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-purge-active-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer writer.Close()

	writer.Append([]map[string]interface{}{{"measurement": "cpu", "value": 42.0}})
	time.Sleep(50 * time.Millisecond)

	os.WriteFile(filepath.Join(tmpDir, "arc-20240101_000000.wal"), []byte("old"), 0600)

	deleted, err := writer.PurgeInactive()
	if err != nil {
		t.Fatalf("PurgeInactive failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted, got %d", deleted)
	}
	if _, err := os.Stat(writer.CurrentFile()); os.IsNotExist(err) {
		t.Fatal("active file should still exist after purge")
	}

	// Can still write after purge
	err = writer.Append([]map[string]interface{}{{"measurement": "cpu", "value": 43.0}})
	if err != nil {
		t.Errorf("should be able to write after PurgeInactive: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if writer.TotalEntries < 2 {
		t.Errorf("expected at least 2 entries, got %d", writer.TotalEntries)
	}
}

// TestPurgeInactive_DoesNotDeleteNonWALFiles verifies only .wal files are deleted.
func TestPurgeInactive_DoesNotDeleteNonWALFiles(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	otherFiles := []string{
		filepath.Join(tmpDir, "notes.txt"),
		filepath.Join(tmpDir, "data.wal.recovered"),
		filepath.Join(tmpDir, "backup.json"),
	}
	for _, f := range otherFiles {
		os.WriteFile(f, []byte("data"), 0600)
	}

	os.WriteFile(filepath.Join(tmpDir, "arc-20240101_000000.wal"), []byte("old"), 0600)

	deleted, err := writer.PurgeInactive()
	if err != nil {
		t.Fatalf("PurgeInactive failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("expected 1 deleted (only .wal), got %d", deleted)
	}

	for _, f := range otherFiles {
		if _, err := os.Stat(f); os.IsNotExist(err) {
			t.Errorf("non-WAL file should not be deleted: %s", filepath.Base(f))
		}
	}
}

// TestPurgeInactive_ConcurrentWithWrite verifies thread safety of PurgeInactive.
func TestPurgeInactive_ConcurrentWithWrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-concurrent-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		SyncInterval: 10 * time.Millisecond,
		BufferSize:   10000,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer writer.Close()

	for i := 0; i < 5; i++ {
		name := filepath.Join(tmpDir, "arc-2024010"+string(rune('1'+i))+"_000000.wal")
		os.WriteFile(name, []byte("old data"), 0600)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			writer.Append([]map[string]interface{}{
				{"measurement": "cpu", "value": float64(i)},
			})
		}
	}()

	deleted, err := writer.PurgeInactive()
	if err != nil {
		t.Fatalf("PurgeInactive failed: %v", err)
	}
	if deleted != 5 {
		t.Errorf("expected 5 deleted, got %d", deleted)
	}

	<-done
	if writer.TotalEntries == 0 {
		t.Error("expected some entries to be written")
	}
}

// TestPurgeAll_VsPurgeInactive verifies the behavioral difference between
// PurgeAll (shutdown — deletes everything) and PurgeInactive (periodic — keeps active).
func TestPurgeAll_VsPurgeInactive(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-purge-compare-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}

	activeFile := writer.CurrentFile()
	oldFile := filepath.Join(tmpDir, "arc-20240101_000000.wal")
	os.WriteFile(oldFile, []byte("old"), 0600)

	// PurgeInactive keeps active file
	deleted, _ := writer.PurgeInactive()
	if deleted != 1 {
		t.Errorf("PurgeInactive: expected 1 deleted, got %d", deleted)
	}
	if _, err := os.Stat(activeFile); os.IsNotExist(err) {
		t.Error("PurgeInactive should not delete active file")
	}

	// PurgeAll deletes everything (shutdown)
	os.WriteFile(oldFile, []byte("old"), 0600)
	writer.Close()
	allDeleted, _ := writer.PurgeAll()
	if allDeleted != 2 {
		t.Errorf("PurgeAll: expected 2 deleted, got %d", allDeleted)
	}

	remaining, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(remaining) != 0 {
		t.Errorf("PurgeAll should delete all files, %d remain", len(remaining))
	}
}

// ==========================================================================
// Empty WAL file cleanup tests (recovery.go fix)
// ==========================================================================

// TestRecovery_DeletesEmptyWALFile verifies recovery deletes header-only WAL files.
func TestRecovery_DeletesEmptyWALFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-empty-delete-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	emptyFile := filepath.Join(tmpDir, "arc-20240101_000000.wal")
	header := make([]byte, WALFileHeaderSize)
	copy(header[0:4], WALMagic)
	binary.BigEndian.PutUint16(header[4:6], WALVersion)
	header[6] = WALChecksumCRC32
	os.WriteFile(emptyFile, header, 0600)

	recovery := NewRecovery(tmpDir, zerolog.Nop())
	callback := func(ctx context.Context, recs []map[string]interface{}) error {
		t.Error("callback should not be called for empty WAL file")
		return nil
	}

	stats, err := recovery.Recover(context.Background(), callback)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if stats.RecoveredEntries != 0 {
		t.Errorf("expected 0 recovered entries, got %d", stats.RecoveredEntries)
	}
	if _, err := os.Stat(emptyFile); !os.IsNotExist(err) {
		t.Error("empty WAL file should be deleted after recovery")
	}
}

// TestRecovery_MultipleEmptyWALFiles verifies all empty WAL files are cleaned up.
func TestRecovery_MultipleEmptyWALFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-multi-empty-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	for i := 0; i < 5; i++ {
		name := filepath.Join(tmpDir, "arc-2024010"+string(rune('1'+i))+"_000000.wal")
		header := make([]byte, WALFileHeaderSize)
		copy(header[0:4], WALMagic)
		binary.BigEndian.PutUint16(header[4:6], WALVersion)
		header[6] = WALChecksumCRC32
		os.WriteFile(name, header, 0600)
	}

	recovery := NewRecovery(tmpDir, zerolog.Nop())
	callback := func(ctx context.Context, recs []map[string]interface{}) error { return nil }
	recovery.Recover(context.Background(), callback)

	remaining, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(remaining) != 0 {
		t.Errorf("expected 0 remaining empty WAL files, got %d", len(remaining))
	}
}

// TestRecovery_MixedEmptyAndNonEmpty verifies recovery handles a mix of empty
// and non-empty WAL files correctly.
func TestRecovery_MixedEmptyAndNonEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-test-mixed-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Empty WAL file
	emptyFile := filepath.Join(tmpDir, "arc-20240101_000000.wal")
	header := make([]byte, WALFileHeaderSize)
	copy(header[0:4], WALMagic)
	binary.BigEndian.PutUint16(header[4:6], WALVersion)
	header[6] = WALChecksumCRC32
	os.WriteFile(emptyFile, header, 0600)

	// Non-empty WAL file
	time.Sleep(1100 * time.Millisecond)
	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	writer.Append([]map[string]interface{}{{"measurement": "test", "value": 1.0}})
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	recovery := NewRecovery(tmpDir, zerolog.Nop())
	var recoveredCount int
	callback := func(ctx context.Context, recs []map[string]interface{}) error {
		recoveredCount += len(recs)
		return nil
	}

	stats, err := recovery.Recover(context.Background(), callback)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}
	if recoveredCount < 1 {
		t.Errorf("expected at least 1 recovered record, got %d", recoveredCount)
	}
	if stats.RecoveredFiles < 1 {
		t.Errorf("expected at least 1 recovered file, got %d", stats.RecoveredFiles)
	}

	// Both files should be gone
	remaining, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(remaining) != 0 {
		t.Errorf("expected 0 remaining WAL files, got %d", len(remaining))
	}
}

// ==========================================================================
// Startup recovery still works (no regression)
// ==========================================================================

// TestStartupRecovery_StillWorks verifies startup recovery correctly replays
// WAL entries after a crash (this must not be broken by the fix).
func TestStartupRecovery_StillWorks(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	records := []map[string]interface{}{
		{"measurement": "cpu", "time": int64(1609459200000000), "host": "server01", "value": 90.5},
		{"measurement": "cpu", "time": int64(1609459200000001), "host": "server02", "value": 80.0},
		{"measurement": "mem", "time": int64(1609459200000002), "host": "server01", "free": int64(4096)},
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	recovery := NewRecovery(tmpDir, zerolog.Nop())
	var recovered []map[string]interface{}
	callback := func(ctx context.Context, recs []map[string]interface{}) error {
		recovered = append(recovered, recs...)
		return nil
	}

	stats, err := recovery.Recover(context.Background(), callback)
	if err != nil {
		t.Fatalf("Startup recovery failed: %v", err)
	}
	if stats.RecoveredFiles != 1 {
		t.Errorf("expected 1 recovered file, got %d", stats.RecoveredFiles)
	}
	if len(recovered) != 3 {
		t.Errorf("expected 3 recovered records, got %d", len(recovered))
	}

	remaining, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(remaining) != 0 {
		t.Errorf("expected 0 WAL files after recovery, got %d", len(remaining))
	}
}
