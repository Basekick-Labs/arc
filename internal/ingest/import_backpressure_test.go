package ingest

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func TestWaitBufferedBelow(t *testing.T) {
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 10, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 1, ShardCount: 1, DataPageVersion: "2.0"}, &mockStorageBackend{}, zerolog.Nop())
	defer b.Close()

	b.bufferBytesBuffered.Store(100)
	if err := b.WaitBufferedBelow(context.Background(), fixed(100), time.Hour, nil, 0); err != nil {
		t.Fatalf("at the limit: %v", err)
	}

	// Above it: returns once the bytes drop.
	go func() {
		time.Sleep(50 * time.Millisecond)
		b.bufferBytesBuffered.Store(10)
	}()
	start := time.Now()
	b.bufferBytesBuffered.Store(500)
	if err := b.WaitBufferedBelow(context.Background(), fixed(100), time.Hour, nil, 0); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("returned before the bytes dropped")
	}

	// Stuck: gives up after maxWait.
	b.bufferBytesBuffered.Store(500)
	if err := b.WaitBufferedBelow(context.Background(), fixed(100), 30*time.Millisecond, nil, 0); !errors.Is(err, ErrBufferedBytesStuck) {
		t.Fatalf("stuck: %v", err)
	}
	// Cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.WaitBufferedBelow(ctx, fixed(100), time.Hour, nil, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	b.bufferBytesBuffered.Store(0)
}

func fixed(n int64) func() int64 { return func() int64 { return n } }

// The limit is read on every recheck: a wait that started under one limit
// ends when the limit rises above the buffered bytes, without them dropping
// (#1429: another import joined the shared limit).
func TestWaitBufferedBelowRereadsTheLimit(t *testing.T) {
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 10, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 1, ShardCount: 1, DataPageVersion: "2.0"}, &mockStorageBackend{}, zerolog.Nop())
	defer b.Close()
	defer b.bufferBytesBuffered.Store(0)

	b.bufferBytesBuffered.Store(500)
	var lim atomic.Int64
	lim.Store(100)
	go func() {
		time.Sleep(40 * time.Millisecond)
		lim.Store(1000)
	}()
	if err := b.WaitBufferedBelow(context.Background(), lim.Load, 2*time.Second, nil, 0); err != nil {
		t.Fatalf("limit rose above the bytes, wait should end: %v", err)
	}
}

// Above the limit, nudge runs every nudgeEvery, not on every poll, and not
// before the first period has passed.
func TestWaitBufferedBelowNudges(t *testing.T) {
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 10, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 1, ShardCount: 1, DataPageVersion: "2.0"}, &mockStorageBackend{}, zerolog.Nop())
	defer b.Close()
	defer b.bufferBytesBuffered.Store(0)

	b.bufferBytesBuffered.Store(500)
	var nudges atomic.Int64
	nudge := func() {
		if nudges.Add(1) == 3 {
			b.bufferBytesBuffered.Store(0)
		}
	}
	start := time.Now()
	if err := b.WaitBufferedBelow(context.Background(), fixed(100), 5*time.Second, nudge, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if n := nudges.Load(); n != 3 {
		t.Fatalf("nudges = %d, want 3", n)
	}
	if el := time.Since(start); el < 140*time.Millisecond {
		t.Fatalf("3 nudges 50 ms apart took %v: nudged on every poll", el)
	}
}
