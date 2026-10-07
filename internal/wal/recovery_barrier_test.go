package wal

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func writeRecoveryFiles(t *testing.T, count int) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	writer, err := NewWriter(&WriterConfig{
		WALDir:       dir,
		SyncMode:     SyncModeFsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = writer.Close()
		}
	})

	files := make([]string, 0, count)
	for i := 0; i < count; i++ {
		if err := writer.Append([]map[string]interface{}{{"index": i}}); err != nil {
			t.Fatalf("Append(%d): %v", i, err)
		}
		files = append(files, writer.CurrentFile())
		if i+1 < count {
			if err := writer.Rotate(); err != nil {
				t.Fatalf("Rotate(%d): %v", i, err)
			}
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true
	return dir, files
}

func TestRecoveryBarrierFailureKeepsReplayedWALFile(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 3)
	recovery := NewRecovery(dir, zerolog.Nop())
	callback := func(context.Context, []map[string]interface{}) error { return nil }

	stats, err := recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		BarrierBatchFiles: 1,
		BeforeDelete:      func(context.Context) error { return errors.New("storage unavailable") },
	})
	if err == nil {
		t.Fatal("RecoverWithOptions returned nil after the flush barrier failed")
	}
	if stats.BarrierFailures != 1 || stats.KeptFiles != 3 || stats.RecoveredFiles != 0 {
		t.Fatalf("first recovery stats = %+v, want all three files kept after barrier failure", stats)
	}
	for _, file := range files {
		if _, err := os.Stat(file); err != nil {
			t.Fatalf("WAL file removed after failed barrier: %v", err)
		}
	}

	stats, err = recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		BeforeDelete: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("second RecoverWithOptions: %v", err)
	}
	if stats.BarrierFailures != 0 || stats.KeptFiles != 0 || stats.RecoveredFiles != 3 {
		t.Fatalf("second recovery stats = %+v, want three recovered files", stats)
	}
	for _, file := range files {
		if _, err := os.Stat(file); !os.IsNotExist(err) {
			t.Fatalf("WAL file remains after successful barrier: %v", err)
		}
	}
}

func TestRecoveryPassesRowEntryIdentityToTrackedCallback(t *testing.T) {
	dir, _ := writeRecoveryFiles(t, 1)
	recovery := NewRecovery(dir, zerolog.Nop())
	var gotIdentity string
	var gotRows int
	callback := func(context.Context, []map[string]interface{}) error {
		t.Fatal("legacy row callback used when tracked callback is configured")
		return nil
	}

	stats, err := recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		TrackedRowCallback: func(_ context.Context, records []map[string]interface{}, identity string) error {
			gotIdentity = identity
			gotRows = len(records)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RecoverWithOptions: %v", err)
	}
	if gotIdentity == "" || gotRows != 1 {
		t.Fatalf("tracked callback received identity %q and %d rows, want a non-empty identity and one row", gotIdentity, gotRows)
	}
	if stats.RecoveredFiles != 1 || stats.RecoveredEntries != 1 || stats.RecoveredBatches != 1 {
		t.Fatalf("recovery stats = %+v, want one recovered row/file/batch", stats)
	}
}

func TestRecoveryBarriersBatchFiles(t *testing.T) {
	dir, _ := writeRecoveryFiles(t, 3)
	recovery := NewRecovery(dir, zerolog.Nop())
	barrierCalls := 0
	callback := func(context.Context, []map[string]interface{}) error { return nil }
	stats, err := recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		BarrierBatchFiles: 2,
		BeforeDelete: func(context.Context) error {
			barrierCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("RecoverWithOptions: %v", err)
	}
	if barrierCalls != 2 {
		t.Fatalf("barrier calls = %d, want 2 for batches of 2 and 1 files", barrierCalls)
	}
	if stats.RecoveredFiles != 3 || stats.KeptFiles != 0 || stats.BarrierFailures != 0 {
		t.Fatalf("recovery stats = %+v, want all three files recovered", stats)
	}
}

func TestRecoveryQuarantinesFileAfterRepeatedReplayFailures(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	recovery := NewRecovery(dir, zerolog.Nop())
	callback := func(context.Context, []map[string]interface{}) error { return errors.New("permanent decode failure") }
	options := &RecoveryOptions{MaxReplayFailures: 2}

	first, err := recovery.RecoverWithOptions(context.Background(), callback, options)
	if err != nil {
		t.Fatalf("first recovery: %v", err)
	}
	if first.KeptFiles != 1 || first.QuarantinedFiles != 0 {
		t.Fatalf("first recovery stats = %+v, want file kept for retry", first)
	}

	second, err := recovery.RecoverWithOptions(context.Background(), callback, options)
	if err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	if second.KeptFiles != 0 || second.QuarantinedFiles != 1 {
		t.Fatalf("second recovery stats = %+v, want one quarantined file", second)
	}
	if _, err := os.Stat(files[0]); !os.IsNotExist(err) {
		t.Fatalf("original WAL path still exists: %v", err)
	}
	if _, err := os.Stat(files[0] + ".failed"); err != nil {
		t.Fatalf("quarantined WAL file missing: %v", err)
	}
}

func TestRecoveryHonorsMinFileAgeForNonExemptFile(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	recovery := NewRecovery(dir, zerolog.Nop())
	callback := func(context.Context, []map[string]interface{}) error { return nil }

	stats, err := recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		MinFileAge: time.Hour,
	})
	if err != nil {
		t.Fatalf("RecoverWithOptions: %v", err)
	}
	if stats.SkippedFiles != 1 || stats.KeptFiles != 1 {
		t.Fatalf("recovery stats = %+v, want one recent file skipped and kept", stats)
	}

	stats, err = recovery.RecoverWithOptions(context.Background(), callback, &RecoveryOptions{
		MinFileAge:            time.Hour,
		MinFileAgeExemptFiles: []string{files[0]},
	})
	if err != nil {
		t.Fatalf("exempt recovery: %v", err)
	}
	if stats.RecoveredFiles != 1 || stats.KeptFiles != 0 {
		t.Fatalf("exempt recovery stats = %+v, want the closed file recovered", stats)
	}
}
