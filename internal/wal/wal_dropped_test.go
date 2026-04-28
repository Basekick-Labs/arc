package wal

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// TestAppendRaw_ReturnsErrWALDroppedWhenFull is a regression test for
// 26.05.1 critical bug C3: when the async entry channel was full,
// AppendRaw / AppendRawWithMeta silently incremented DroppedEntries
// and returned nil — making downstream "data preserved in WAL"
// log messages untrue.
//
// The fix: return ErrWALDropped on channel-full so callers (the
// ingestion buffer) can increment their own error counters and
// surface accurate operator-facing messages.
//
// Drops are made deterministic by halting the writer's background
// drain goroutine (close w.done + wg.Wait) before issuing the
// dropping append. With the drain stopped and BufferSize=1, the
// channel saturates after a single in-test send and every
// subsequent AppendRaw is guaranteed to drop. No timing assumptions,
// no t.Skip on a fast machine.
func TestAppendRaw_ReturnsErrWALDroppedWhenFull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-dropped-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 10 * 1024 * 1024,
		MaxAge:       time.Hour,
		BufferSize:   1,
		Logger:       zerolog.Nop(),
	}
	writer, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	// Stop the background drain so the channel fills deterministically.
	// Note: we don't call writer.Close() because Close also closes the
	// underlying file, which would make subsequent AppendRaws fail with
	// a real I/O error rather than ErrWALDropped — masking the test.
	close(writer.done)
	writer.wg.Wait()

	// Fill the buffer with a single direct send (drainer is gone, so
	// the slot stays occupied for the rest of the test).
	writer.entryChan <- walEntry{data: []byte{0xff}}

	// Now every AppendRaw must observe a full channel and return
	// ErrWALDropped — no timing window, no skip.
	const attempts = 10
	for i := 0; i < attempts; i++ {
		err := writer.AppendRaw([]byte{byte(i)})
		if !errors.Is(err, ErrWALDropped) {
			t.Fatalf("attempt %d: expected ErrWALDropped, got: %v", i, err)
		}
	}

	if got := atomic.LoadInt64(&writer.DroppedEntries); got != attempts {
		t.Errorf("DroppedEntries = %d, want %d", got, attempts)
	}
}

// TestAppendRawWithMeta_ReturnsErrWALDroppedWhenFull mirrors the test
// above for the metadata-envelope variant (used by the ingest hot
// path for zero-copy WAL writes).
func TestAppendRawWithMeta_ReturnsErrWALDroppedWhenFull(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "wal-dropped-meta-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := &WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeAsync,
		MaxSizeBytes: 10 * 1024 * 1024,
		MaxAge:       time.Hour,
		BufferSize:   1,
		Logger:       zerolog.Nop(),
	}
	writer, err := NewWriter(cfg)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	close(writer.done)
	writer.wg.Wait()

	writer.entryChan <- walEntry{data: []byte{0xff}}

	const attempts = 10
	for i := 0; i < attempts; i++ {
		err := writer.AppendRawWithMeta("testdb", []byte{byte(i)})
		if !errors.Is(err, ErrWALDropped) {
			t.Fatalf("attempt %d: expected ErrWALDropped, got: %v", i, err)
		}
	}

	if got := atomic.LoadInt64(&writer.DroppedEntries); got != attempts {
		t.Errorf("DroppedEntries = %d, want %d", got, attempts)
	}
}
