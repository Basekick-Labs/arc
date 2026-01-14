package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestWriter(t *testing.T, syncMode SyncMode) (*Writer, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := &WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     syncMode,
		MaxSizeBytes: 10 * 1024 * 1024, // 10MB
		MaxAge:       time.Hour,
		SyncInterval: 100 * time.Millisecond,
		SyncBytes:    1024 * 1024,
		BufferSize:   1000,
		Logger:       zerolog.Nop(),
	}

	writer, err := NewWriter(cfg)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create writer: %v", err)
	}

	return writer, tmpDir
}

func TestNewWriter(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeFdatasync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	// Check that WAL directory was created
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Error("WAL directory was not created")
	}

	// Check that a WAL file was created
	files, _ := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if len(files) != 1 {
		t.Errorf("expected 1 WAL file, got %d", len(files))
	}

	// Check current file path is set
	if writer.CurrentFile() == "" {
		t.Error("current file path should not be empty")
	}
}

func TestWriter_Defaults(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-defaults-*")
	defer os.RemoveAll(tmpDir)

	cfg := &WriterConfig{
		WALDir: tmpDir,
		Logger: zerolog.Nop(),
	}

	writer, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("failed to create writer: %v", err)
	}
	defer writer.Close()

	// Check defaults were applied
	if writer.config.SyncMode != SyncModeFdatasync {
		t.Errorf("default sync mode: got %s, want %s", writer.config.SyncMode, SyncModeFdatasync)
	}
	if writer.config.MaxSizeBytes != 100*1024*1024 {
		t.Errorf("default max size: got %d, want %d", writer.config.MaxSizeBytes, 100*1024*1024)
	}
	if writer.config.MaxAge != time.Hour {
		t.Errorf("default max age: got %v, want %v", writer.config.MaxAge, time.Hour)
	}
}

func TestWriter_Append(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	records := []map[string]interface{}{
		{
			"measurement": "cpu",
			"time":        int64(1609459200000000),
			"host":        "server01",
			"usage":       90.5,
		},
		{
			"measurement": "memory",
			"time":        int64(1609459200000001),
			"host":        "server01",
			"free":        int64(1024),
		},
	}

	err := writer.Append(records)
	if err != nil {
		t.Errorf("Append failed: %v", err)
	}

	// Give async writer time to process
	time.Sleep(50 * time.Millisecond)

	// Close to ensure all data is written
	writer.Close()

	// Verify stats
	stats := writer.Stats()
	if stats["total_entries"].(int64) < 1 {
		t.Errorf("expected at least 1 entry, got %d", stats["total_entries"])
	}
}

func TestWriter_AppendRaw(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Pre-serialized msgpack data
	payload := []byte{0x91, 0x81, 0xa1, 0x6d, 0xa3, 0x63, 0x70, 0x75} // Simple msgpack

	err := writer.AppendRaw(payload)
	if err != nil {
		t.Errorf("AppendRaw failed: %v", err)
	}

	// Give async writer time to process
	time.Sleep(50 * time.Millisecond)

	writer.Close()

	if writer.TotalEntries < 1 {
		t.Errorf("expected at least 1 entry, got %d", writer.TotalEntries)
	}
}

func TestWriter_SyncModes(t *testing.T) {
	modes := []SyncMode{SyncModeFsync, SyncModeFdatasync, SyncModeAsync}

	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			writer, tmpDir := newTestWriter(t, mode)
			defer os.RemoveAll(tmpDir)

			records := []map[string]interface{}{
				{"measurement": "test", "value": 1.0},
			}

			err := writer.Append(records)
			if err != nil {
				t.Errorf("Append failed with mode %s: %v", mode, err)
			}

			time.Sleep(50 * time.Millisecond)
			writer.Close()

			if writer.TotalEntries < 1 {
				t.Errorf("mode %s: expected at least 1 entry", mode)
			}
		})
	}
}

func TestWriter_Stats(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	stats := writer.Stats()

	// Check required stat fields
	requiredFields := []string{
		"current_file",
		"current_size_mb",
		"current_age_seconds",
		"sync_mode",
		"total_entries",
		"total_bytes",
		"total_syncs",
		"total_rotations",
		"dropped_entries",
		"buffer_size",
		"buffer_used",
	}

	for _, field := range requiredFields {
		if _, ok := stats[field]; !ok {
			t.Errorf("missing stat field: %s", field)
		}
	}

	writer.Close()
}

func TestWriter_Close(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write some data
	records := []map[string]interface{}{
		{"measurement": "test", "value": 1.0},
	}
	writer.Append(records)

	// Give async writer time to process
	time.Sleep(50 * time.Millisecond)

	// Close should not error
	err := writer.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	// Verify data was written
	if writer.TotalEntries < 1 {
		t.Errorf("expected at least 1 entry after close, got %d", writer.TotalEntries)
	}
}

func TestReader_ReadAll(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write test records
	records := []map[string]interface{}{
		{
			"measurement": "cpu",
			"time":        int64(1609459200000000),
			"host":        "server01",
			"usage":       90.5,
		},
	}

	writer.Append(records)
	time.Sleep(50 * time.Millisecond)

	walFile := writer.CurrentFile()
	writer.Close()

	// Read back
	reader := NewReader(walFile, zerolog.Nop())
	entries, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if len(entries) < 1 {
		t.Errorf("expected at least 1 entry, got %d", len(entries))
	}

	// Verify entry content
	if len(entries) > 0 && len(entries[0].Records) > 0 {
		rec := entries[0].Records[0]
		if rec["measurement"] != "cpu" {
			t.Errorf("measurement: got %v, want 'cpu'", rec["measurement"])
		}
	}
}

func TestReader_InvalidMagic(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-invalid-*")
	defer os.RemoveAll(tmpDir)

	// Create file with invalid magic
	invalidFile := filepath.Join(tmpDir, "invalid.wal")
	os.WriteFile(invalidFile, []byte("BADM0000"), 0600)

	reader := NewReader(invalidFile, zerolog.Nop())
	_, err := reader.ReadAll()
	if err == nil {
		t.Error("expected error for invalid magic bytes")
	}
}

func TestReader_CorruptedChecksum(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write valid data
	records := []map[string]interface{}{
		{"measurement": "cpu", "value": 90.5},
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)

	walFile := writer.CurrentFile()
	writer.Close()

	// Corrupt the file by modifying bytes after the header
	data, _ := os.ReadFile(walFile)
	if len(data) > 30 {
		data[30] ^= 0xFF // Flip some bits in the payload
		os.WriteFile(walFile, data, 0600)
	}

	// Read should handle corrupted entries gracefully
	reader := NewReader(walFile, zerolog.Nop())
	_, err := reader.ReadAll()
	// Error is acceptable, panic is not
	_ = err
}

func TestRecovery_Recover(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write test records
	records := []map[string]interface{}{
		{
			"measurement": "cpu",
			"time":        int64(1609459200000000),
			"host":        "server01",
			"usage":       90.5,
		},
	}

	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	// Recover
	recovery := NewRecovery(tmpDir, zerolog.Nop())

	var recoveredRecords []map[string]interface{}
	callback := func(ctx context.Context, records []map[string]interface{}) error {
		recoveredRecords = append(recoveredRecords, records...)
		return nil
	}

	ctx := context.Background()
	stats, err := recovery.Recover(ctx, callback)
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if stats.RecoveredFiles < 1 {
		t.Errorf("expected at least 1 recovered file, got %d", stats.RecoveredFiles)
	}

	if len(recoveredRecords) < 1 {
		t.Errorf("expected at least 1 recovered record, got %d", len(recoveredRecords))
	}

	// Check that WAL file was archived
	activeFiles, recoveredFiles, _ := recovery.ListWALFiles()
	if len(activeFiles) != 0 {
		t.Errorf("expected 0 active files, got %d", len(activeFiles))
	}
	if len(recoveredFiles) != 1 {
		t.Errorf("expected 1 recovered file, got %d", len(recoveredFiles))
	}
}

func TestRecovery_NoWALDirectory(t *testing.T) {
	recovery := NewRecovery("/nonexistent/path", zerolog.Nop())

	stats, err := recovery.Recover(context.Background(), func(ctx context.Context, records []map[string]interface{}) error {
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if stats.RecoveredFiles != 0 {
		t.Errorf("expected 0 recovered files for nonexistent dir, got %d", stats.RecoveredFiles)
	}
}

func TestRecovery_EmptyDirectory(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-empty-*")
	defer os.RemoveAll(tmpDir)

	recovery := NewRecovery(tmpDir, zerolog.Nop())

	stats, err := recovery.Recover(context.Background(), func(ctx context.Context, records []map[string]interface{}) error {
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if stats.RecoveredFiles != 0 {
		t.Errorf("expected 0 recovered files for empty dir, got %d", stats.RecoveredFiles)
	}
}

func TestRecovery_ContextCancellation(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)

	// Write test records
	records := []map[string]interface{}{
		{"measurement": "cpu", "value": 90.5},
	}
	writer.Append(records)
	time.Sleep(50 * time.Millisecond)
	writer.Close()

	// Recover with cancelled context
	recovery := NewRecovery(tmpDir, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, err := recovery.Recover(ctx, func(ctx context.Context, records []map[string]interface{}) error {
		return nil
	})

	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestRecovery_CleanupOldWALs(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-cleanup-*")
	defer os.RemoveAll(tmpDir)

	// Create some old recovered files
	oldFile := filepath.Join(tmpDir, "old.wal.recovered")
	os.WriteFile(oldFile, []byte("old data"), 0600)

	// Set file modification time to past
	oldTime := time.Now().Add(-48 * time.Hour)
	os.Chtimes(oldFile, oldTime, oldTime)

	recovery := NewRecovery(tmpDir, zerolog.Nop())

	// Cleanup files older than 24 hours
	deleted, freed, err := recovery.CleanupOldWALs(24 * time.Hour)
	if err != nil {
		t.Fatalf("CleanupOldWALs failed: %v", err)
	}

	if deleted != 1 {
		t.Errorf("expected 1 deleted file, got %d", deleted)
	}
	if freed == 0 {
		t.Error("expected non-zero freed bytes")
	}

	// Verify file was deleted
	if _, err := os.Stat(oldFile); !os.IsNotExist(err) {
		t.Error("old file should have been deleted")
	}
}

func TestRecovery_ListWALFiles(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "wal-test-list-*")
	defer os.RemoveAll(tmpDir)

	// Create mix of files
	os.WriteFile(filepath.Join(tmpDir, "active1.wal"), []byte("data"), 0600)
	os.WriteFile(filepath.Join(tmpDir, "active2.wal"), []byte("data"), 0600)
	os.WriteFile(filepath.Join(tmpDir, "old.wal.recovered"), []byte("data"), 0600)
	os.WriteFile(filepath.Join(tmpDir, "other.txt"), []byte("data"), 0600)

	recovery := NewRecovery(tmpDir, zerolog.Nop())

	active, recovered, err := recovery.ListWALFiles()
	if err != nil {
		t.Fatalf("ListWALFiles failed: %v", err)
	}

	if len(active) != 2 {
		t.Errorf("expected 2 active files, got %d", len(active))
	}
	if len(recovered) != 1 {
		t.Errorf("expected 1 recovered file, got %d", len(recovered))
	}
}

func TestWALFileFormat(t *testing.T) {
	// Test WAL constants
	if len(WALMagic) != 4 {
		t.Errorf("WALMagic should be 4 bytes, got %d", len(WALMagic))
	}
	if string(WALMagic) != "ARCW" {
		t.Errorf("WALMagic should be 'ARCW', got %q", string(WALMagic))
	}
	if WALVersion != 0x0001 {
		t.Errorf("WALVersion should be 0x0001, got 0x%04x", WALVersion)
	}
	if WALFileHeaderSize != 7 {
		t.Errorf("WALFileHeaderSize should be 7, got %d", WALFileHeaderSize)
	}
	if WALEntryHeaderSize != 16 {
		t.Errorf("WALEntryHeaderSize should be 16, got %d", WALEntryHeaderSize)
	}
}

// Benchmark tests
func BenchmarkWriter_Append(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "wal-bench-*")
	defer os.RemoveAll(tmpDir)

	cfg := &WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 1024 * 1024 * 1024, // 1GB to prevent rotation
		BufferSize:   100000,
		Logger:       zerolog.Nop(),
	}

	writer, _ := NewWriter(cfg)
	defer writer.Close()

	records := []map[string]interface{}{
		{
			"measurement": "cpu",
			"time":        int64(1609459200000000),
			"host":        "server01",
			"usage":       90.5,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.Append(records)
	}
}

func BenchmarkWriter_AppendRaw(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "wal-bench-raw-*")
	defer os.RemoveAll(tmpDir)

	cfg := &WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 1024 * 1024 * 1024,
		BufferSize:   100000,
		Logger:       zerolog.Nop(),
	}

	writer, _ := NewWriter(cfg)
	defer writer.Close()

	payload := []byte{0x91, 0x84, 0xa1, 0x6d, 0xa3, 0x63, 0x70, 0x75, 0xa1, 0x74, 0xcf, 0x00, 0x05, 0xb7, 0x8d, 0x8d, 0x40, 0x00, 0x00, 0xa1, 0x68, 0xa8, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x30, 0x31, 0xa5, 0x75, 0x73, 0x61, 0x67, 0x65, 0xcb, 0x40, 0x56, 0xa0, 0x00, 0x00, 0x00, 0x00, 0x00}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.AppendRaw(payload)
	}
}

func TestWriter_AppendRaw_PayloadTooLarge(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	// Create payload exceeding the limit
	payload := make([]byte, MaxWALPayloadSize+1)

	err := writer.AppendRaw(payload)
	if err == nil {
		t.Fatal("AppendRaw should fail for oversized payload")
	}
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("Expected ErrPayloadTooLarge, got: %v", err)
	}
}

func TestWriter_AppendRaw_PayloadTooLarge_ErrorMessage(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	oversizeAmount := 1000
	payload := make([]byte, MaxWALPayloadSize+oversizeAmount)

	err := writer.AppendRaw(payload)
	if err == nil {
		t.Fatal("Expected error for oversized payload")
	}

	// Verify error message contains useful information
	errMsg := err.Error()
	if !strings.Contains(errMsg, "104858600") { // MaxWALPayloadSize + 1000
		t.Errorf("Error message should contain actual size, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "104857600") { // MaxWALPayloadSize
		t.Errorf("Error message should contain limit, got: %s", errMsg)
	}
}

func TestWriter_AppendRaw_PayloadAtLimit(t *testing.T) {
	writer, tmpDir := newTestWriter(t, SyncModeAsync)
	defer os.RemoveAll(tmpDir)
	defer writer.Close()

	// Create payload exactly at the limit - should succeed
	payload := make([]byte, MaxWALPayloadSize)

	err := writer.AppendRaw(payload)
	if err != nil {
		t.Errorf("AppendRaw should succeed for payload at limit: %v", err)
	}
}
