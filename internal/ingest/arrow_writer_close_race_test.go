package ingest

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// TestArrowBuffer_CloseRaceWithConcurrentWrites is a regression test
// for the 26.05.1 critical bug C1: Close() called close(flushQueue)
// AFTER cancel(), creating a narrow race window where a writer
// goroutine past shard.mu.Unlock() but not yet at the channel send
// would panic with "send on closed channel".
//
// The fix replaces the channel-close with a closing atomic.Bool flag
// + ctx.Done() select arm, and stops closing the channel entirely
// (workers exit on ctx.Done()).
//
// The test waits for evidence that writers are actively producing
// (totalRecordsBuffered crosses a threshold) before triggering
// Close, instead of a fixed time.Sleep that's timing-fragile on
// slow CI runners. Each writer goroutine has its own deferred
// recover so a panic inside the buffer becomes a t.Errorf rather
// than crashing the test binary before the main goroutine sees it.
func TestArrowBuffer_CloseRaceWithConcurrentWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping race-stress test in -short mode")
	}

	cfg := &config.IngestConfig{
		MaxBufferSize:   100, // small buffer so flush triggers often
		MaxBufferAgeMS:  1000,
		Compression:     "snappy",
		FlushWorkers:    4,
		FlushQueueSize:  16,
		ShardCount:      8,
		DataPageVersion: "2.0",
	}
	storage := newHangingStorage(0, 1*time.Millisecond) // tiny "hang" — effectively succeed quickly
	buf := NewArrowBuffer(cfg, storage, zerolog.Nop())

	const numWriters = 32
	const writesPerWriter = 200
	const evidenceThreshold = 1000 // records buffered before Close fires

	var startBarrier sync.WaitGroup
	startBarrier.Add(1)
	var wg sync.WaitGroup
	wg.Add(numWriters)

	// Launch writers; they all wait on the barrier so we get a true
	// concurrent burst instead of staggered starts. Per-goroutine
	// recover turns any panic from the buffer into a t.Errorf.
	for i := 0; i < numWriters; i++ {
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("writer %d panicked: %v", id, r)
				}
			}()
			startBarrier.Wait()

			cols := makeColumns(50) // 50 records per write — triggers buffer fills
			for j := 0; j < writesPerWriter; j++ {
				// Ignore errors — Close mid-flight may produce
				// "skipped: buffer is closing" warnings; the
				// assertion is panic-free, not error-free.
				_ = buf.WriteColumnarDirect(context.Background(), "testdb", "test_measurement", cols)
			}
		}(i)
	}

	// Release the writers and wait for *evidence* that they're active
	// before triggering Close — replaces a fixed time.Sleep that
	// would race the writers on a slow CI runner.
	startBarrier.Done()
	deadline := time.Now().Add(2 * time.Second)
	for buf.totalRecordsBuffered.Load() < evidenceThreshold {
		if time.Now().After(deadline) {
			t.Fatalf("writers failed to buffer %d records within 2s; only %d buffered", evidenceThreshold, buf.totalRecordsBuffered.Load())
		}
		runtime.Gosched()
	}

	// Close while writers are still writing. With the bug, Close
	// races the writers' channel sends and panics. With the fix,
	// Close observes the closing flag + ctx cancellation and shuts
	// down cleanly.
	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Drain any remaining writers; they should exit cleanly even
	// though many of their sends were no-ops after Close.
	wg.Wait()
}

// TestArrowBuffer_WriteAfterCloseDoesNotPanic asserts that a writer
// goroutine attempting to write AFTER Close has fully returned does
// not panic. This is the steady-state version of the race test —
// Close has set b.closing and cancelled b.ctx; subsequent writes
// observe the flag and return without sending.
func TestArrowBuffer_WriteAfterCloseDoesNotPanic(t *testing.T) {
	cfg := &config.IngestConfig{
		MaxBufferSize:   10,
		MaxBufferAgeMS:  1000,
		Compression:     "snappy",
		FlushWorkers:    2,
		FlushQueueSize:  4,
		ShardCount:      4,
		DataPageVersion: "2.0",
	}
	storage := newHangingStorage(0, 1*time.Millisecond)
	buf := NewArrowBuffer(cfg, storage, zerolog.Nop())

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Now write — must not panic. Either returns nil (record
	// silently dropped because closing) or a non-panicking error.
	cols := makeColumns(20)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Write after Close panicked: %v", r)
		}
	}()
	_ = buf.WriteColumnarDirect(context.Background(), "testdb", "test_measurement", cols)
}
