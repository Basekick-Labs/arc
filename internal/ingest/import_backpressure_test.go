package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

func TestWaitBufferedBelow(t *testing.T) {
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 10, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 1, ShardCount: 1, DataPageVersion: "2.0"}, &mockStorageBackend{}, zerolog.Nop())
	defer b.Close()

	b.bufferBytesBuffered.Store(100)
	if err := b.WaitBufferedBelow(context.Background(), 100, time.Hour); err != nil {
		t.Fatalf("at the limit: %v", err)
	}

	// Above it: returns once the bytes drop.
	go func() {
		time.Sleep(50 * time.Millisecond)
		b.bufferBytesBuffered.Store(10)
	}()
	start := time.Now()
	b.bufferBytesBuffered.Store(500)
	if err := b.WaitBufferedBelow(context.Background(), 100, time.Hour); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("returned before the bytes dropped")
	}

	// Stuck: gives up after maxWait.
	b.bufferBytesBuffered.Store(500)
	if err := b.WaitBufferedBelow(context.Background(), 100, 30*time.Millisecond); !errors.Is(err, ErrBufferedBytesStuck) {
		t.Fatalf("stuck: %v", err)
	}
	// Cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.WaitBufferedBelow(ctx, 100, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	b.bufferBytesBuffered.Store(0)
}
