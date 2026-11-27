package ingest

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// latencyStorage makes every write take a fixed time and records how many
// were in flight at once: the shape of a durable local write, whose cost since
// #1618 is dominated by the file and directory fsync.
type latencyStorage struct {
	*hangingStorageBackend
	latency  time.Duration
	inflight atomic.Int32
	peak     atomic.Int32
	writes   atomic.Int32
}

func (s *latencyStorage) Write(ctx context.Context, path string, data []byte) error {
	n := s.inflight.Add(1)
	defer s.inflight.Add(-1)
	for {
		p := s.peak.Load()
		if n <= p || s.peak.CompareAndSwap(p, n) {
			break
		}
	}
	select {
	case <-time.After(s.latency):
	case <-ctx.Done():
		return ctx.Err()
	}
	s.writes.Add(1)
	return nil
}

func (s *latencyStorage) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	data, _ := io.ReadAll(r)
	return s.Write(ctx, path, data)
}

// The aged sweep writes a shard's aged buffers concurrently, bounded by
// flush_workers, and returns only once they are written (#1632). Before, it
// wrote them one at a time under the shard lock, so a sweep over N aged
// buffers cost N x the write latency: 64 buffers at 50 ms is 3.2 s serial.
func TestAgedSweepWritesConcurrently(t *testing.T) {
	const (
		measurements = 64
		workers      = 8
		latency      = 50 * time.Millisecond
	)
	st := &latencyStorage{hangingStorageBackend: newHangingStorage(1<<30, 0), latency: latency}
	cfg := &config.IngestConfig{
		MaxBufferSize:  1 << 20,
		MaxBufferAgeMS: 3_600_000, // the periodic sweep never fires on its own
		Compression:    "snappy",
		FlushWorkers:   workers,
		FlushQueueSize: 4,
		ShardCount:     1, // every buffer in one shard: the case the old serial loop was worst at
	}
	buf := NewArrowBuffer(cfg, st, zerolog.Nop())
	t.Cleanup(func() { _ = buf.Close() })

	base := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	for i := 0; i < measurements; i++ {
		cols := map[string][]interface{}{"time": {base + int64(i)}, "v": {float64(i)}}
		if err := buf.WriteColumnarDirect(context.Background(), "db", fmt.Sprintf("m%03d", i), cols); err != nil {
			t.Fatal(err)
		}
	}
	// Stop the background loops so the only sweep is the one under test: a
	// new-buffer signal still pending would otherwise let periodicFlush see
	// the aged start times below and sweep concurrently. Flush I/O derives
	// from flushParent, not b.ctx, so the sweep itself is unaffected.
	buf.cancel()
	// Age every buffer past the threshold.
	shard := buf.shards[0]
	shard.mu.Lock()
	if len(shard.bufferStartTimes) != measurements {
		shard.mu.Unlock()
		t.Fatalf("premise: %d buffers, want %d", len(shard.bufferStartTimes), measurements)
	}
	for k := range shard.bufferStartTimes {
		shard.bufferStartTimes[k] = time.Now().UTC().Add(-2 * time.Hour)
	}
	shard.mu.Unlock()

	start := time.Now()
	buf.flushAgedBuffers()
	// The sweep starts its writes without waiting for them (#1695); wait for
	// them here, as Close does through b.wg.
	buf.agedWG.Wait()
	took := time.Since(start)

	if got := st.writes.Load(); got != measurements {
		t.Fatalf("%d of %d aged buffers written", got, measurements)
	}
	if p := st.peak.Load(); p < 2 || p > workers {
		t.Errorf("peak concurrent writes = %d, want between 2 and flush_workers (%d)", p, workers)
	}
	serial := time.Duration(measurements) * latency
	if took > serial/2 {
		t.Errorf("sweep took %s for %d buffers at %s each; serial is %s, so the writes did not overlap", took, measurements, latency, serial)
	}
	shard.mu.RLock()
	left := len(shard.buffers)
	shard.mu.RUnlock()
	if left != 0 {
		t.Errorf("%d buffers left after the sweep", left)
	}
	if d := len(buf.flushQueue); d != 0 {
		t.Errorf("the sweep used the flush queue (%d tasks); aged writes must not take writers' slots", d)
	}
}

// A sweep that finds Close begun extracts nothing and leaves every buffer to
// Close's own loop, which writes them all: no buffer ends up in neither place.
func TestAgedSweepLeavesBuffersToCloseOnceClosing(t *testing.T) {
	st := &latencyStorage{hangingStorageBackend: newHangingStorage(1<<30, 0), latency: time.Millisecond}
	cfg := &config.IngestConfig{
		MaxBufferSize: 1 << 20, MaxBufferAgeMS: 3_600_000, Compression: "snappy",
		FlushWorkers: 4, FlushQueueSize: 4, ShardCount: 2,
	}
	buf := NewArrowBuffer(cfg, st, zerolog.Nop())
	base := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	const n = 16
	for i := 0; i < n; i++ {
		cols := map[string][]interface{}{"time": {base + int64(i)}, "v": {float64(i)}}
		if err := buf.WriteColumnarDirect(context.Background(), "db", fmt.Sprintf("m%02d", i), cols); err != nil {
			t.Fatal(err)
		}
	}
	buf.cancel() // only the sweep under test runs
	for _, sh := range buf.shards {
		sh.mu.Lock()
		for k := range sh.bufferStartTimes {
			sh.bufferStartTimes[k] = time.Now().UTC().Add(-2 * time.Hour)
		}
		sh.mu.Unlock()
	}
	buf.closing.Store(true) // as Close sets it before waiting for the sweep
	buf.flushAgedBuffers()
	if got := st.writes.Load(); got != 0 {
		t.Fatalf("the sweep wrote %d buffers after closing was set", got)
	}
	buf.closing.Store(false)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); _ = buf.Close() }()
	wg.Wait()
	if got := st.writes.Load(); got != n {
		t.Fatalf("%d of %d buffers written after the sweep and Close", got, n)
	}
	if !buf.CloseFlushedCleanly() {
		t.Error("Close did not report a clean flush")
	}
}

// closingStorage sets closing on the buffer from inside the first write: Close
// beginning while the sweep is writing, the window that matters.
type closingStorage struct {
	*latencyStorage
	buf  *ArrowBuffer
	once sync.Once
}

func (s *closingStorage) Write(ctx context.Context, path string, data []byte) error {
	s.once.Do(func() { s.buf.closing.Store(true) })
	return s.latencyStorage.Write(ctx, path, data)
}

func (s *closingStorage) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	data, _ := io.ReadAll(r)
	return s.Write(ctx, path, data)
}

// Close starting mid-sweep stops the sweep once a slot frees: at most
// flush_workers writes (the sweep's slots), not every aged buffer of the
// shard, so Close's own loop gets the rest within its budget.
func TestAgedSweepStopsAfterCurrentBatchWhenCloseBegins(t *testing.T) {
	const workers, n = 4, 40
	inner := &latencyStorage{hangingStorageBackend: newHangingStorage(1<<30, 0), latency: 20 * time.Millisecond}
	st := &closingStorage{latencyStorage: inner}
	cfg := &config.IngestConfig{
		MaxBufferSize: 1 << 20, MaxBufferAgeMS: 3_600_000, Compression: "snappy",
		FlushWorkers: workers, FlushQueueSize: 4, ShardCount: 1,
	}
	buf := NewArrowBuffer(cfg, st, zerolog.Nop())
	st.buf = buf
	base := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	for i := 0; i < n; i++ {
		cols := map[string][]interface{}{"time": {base + int64(i)}, "v": {float64(i)}}
		if err := buf.WriteColumnarDirect(context.Background(), "db", fmt.Sprintf("m%02d", i), cols); err != nil {
			t.Fatal(err)
		}
	}
	buf.cancel() // only the sweep under test runs; see TestAgedSweepWritesConcurrently
	shard := buf.shards[0]
	shard.mu.Lock()
	for k := range shard.bufferStartTimes {
		shard.bufferStartTimes[k] = time.Now().UTC().Add(-2 * time.Hour)
	}
	shard.mu.Unlock()

	buf.flushAgedBuffers()
	buf.agedWG.Wait() // writes are detached (#1695)
	if got := inner.writes.Load(); got < 1 || got > workers {
		t.Fatalf("sweep wrote %d buffers after Close began, want 1..%d (the batch in flight)", got, workers)
	}
	shard.mu.RLock()
	left := len(shard.buffers)
	shard.mu.RUnlock()
	if left != n-int(inner.writes.Load()) {
		t.Fatalf("%d buffers left in the shard, want %d: a buffer is in neither place", left, n-int(inner.writes.Load()))
	}
	buf.closing.Store(false)
	_ = buf.Close()
	if got := inner.writes.Load(); got != n {
		t.Fatalf("%d of %d written after Close", got, n)
	}
}
