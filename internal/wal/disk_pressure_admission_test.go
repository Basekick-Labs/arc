package wal

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestDiskPressureAdmissionAccountsForQueuedWrites(t *testing.T) {
	const (
		concurrentWrites = 4
		payloadBytes     = 2 << 20
		mebibyte         = 1 << 20
	)

	dir := t.TempDir()
	_, freeBytes, err := filesystemUsage(dir)
	if err != nil {
		t.Skipf("filesystem usage unavailable for test directory: %v", err)
	}
	freeMiB := int(freeBytes / mebibyte)
	if freeMiB <= 5 {
		t.Skipf("only %d MiB free; cannot establish a deterministic reserve", freeMiB)
	}

	w, err := NewWriter(&WriterConfig{
		WALDir:                   dir,
		SyncMode:                 SyncModeAsync,
		BufferSize:               concurrentWrites + 1,
		MaxSizeBytes:             64 * mebibyte,
		DiskHighWatermarkPercent: 99,
		DiskMinFreeMB:            freeMiB - 5,
		Logger:                   zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	releaseHook := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseHook) }) }
	enteredHook := make(chan struct{}, concurrentWrites)
	results := make(chan error, concurrentWrites)
	w.replicationHook = func(*ReplicationEntry) {
		enteredHook <- struct{}{}
		<-releaseHook
	}
	t.Cleanup(func() {
		release()
		_ = w.Close()
	})

	payload := make([]byte, payloadBytes)
	for i := 0; i < concurrentWrites; i++ {
		go func() {
			_, appendErr := w.AppendRawWithMetaTracked("pressure-test", payload)
			results <- appendErr
		}()
	}

	// Hold successful calls after admission but before enqueue. This keeps the
	// filesystem free-space reading constant while concurrent writers race the
	// same per-entry check.
	admissions := 0
	pressured := 0
	accepted := 0
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for admissions < concurrentWrites {
		select {
		case <-enteredHook:
			accepted++
			admissions++
		case appendErr := <-results:
			if !errors.Is(appendErr, ErrWALDiskPressure) {
				t.Fatalf("append returned %v before reaching the replication hook", appendErr)
			}
			pressured++
			admissions++
		case <-deadline.C:
			t.Fatalf("timed out after %d admissions", admissions)
		}
	}
	release()

	completed := pressured
	for completed < concurrentWrites {
		if appendErr := <-results; appendErr != nil {
			t.Fatalf("append after admission = %v", appendErr)
		}
		completed++
	}
	if accepted == 0 {
		t.Fatal("test reserve rejected every entry; expected at least one admitted write")
	}
	if pressured == 0 {
		t.Fatalf("all %d concurrent entries passed admission with only about 5 MiB above the reserve", accepted)
	}

	w.pendingMu.Lock()
	pending := len(w.pendingSeqs)
	w.pendingMu.Unlock()
	if pending != accepted {
		t.Fatalf("pending tracked sequences = %d, want %d accepted entries", pending, accepted)
	}
}

func TestDiskPressureBatchAdmissionRejectsWholeRequest(t *testing.T) {
	const (
		payloadBytes = 2 << 20
		mebibyte     = 1 << 20
	)

	dir := t.TempDir()
	_, freeBytes, err := filesystemUsage(dir)
	if err != nil {
		t.Skipf("filesystem usage unavailable for test directory: %v", err)
	}
	freeMiB := int(freeBytes / mebibyte)
	if freeMiB <= 4 {
		t.Skipf("only %d MiB free; cannot establish a deterministic reserve", freeMiB)
	}

	w, err := NewWriter(&WriterConfig{
		WALDir:                   dir,
		SyncMode:                 SyncModeAsync,
		DiskHighWatermarkPercent: 99,
		DiskMinFreeMB:            freeMiB - 3,
		Logger:                   zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	t.Cleanup(func() { _ = w.Close() })

	payload := make([]byte, payloadBytes)
	single, err := w.ReserveRawWithMetaBatch("pressure-test", [][]byte{payload})
	if err != nil {
		t.Fatalf("one payload should fit in the remaining headroom: %v", err)
	}
	single.Release()

	if _, err := w.ReserveRawWithMetaBatch("pressure-test", [][]byte{payload, payload}); !errors.Is(err, ErrWALDiskPressure) {
		t.Fatalf("batch reservation error = %v, want ErrWALDiskPressure", err)
	}
	if got := atomic.LoadInt64(&w.TotalEntries); got != 0 {
		t.Fatalf("batch rejection wrote %d WAL entries before admission", got)
	}
	w.pendingMu.Lock()
	pending := len(w.pendingSeqs)
	w.pendingMu.Unlock()
	if pending != 0 {
		t.Fatalf("batch rejection created %d pending WAL identities", pending)
	}
}
