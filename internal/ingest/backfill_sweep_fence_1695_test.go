package ingest

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1695: a long backfill flush must not hold up the age sweep, nor barriers
// for other keys.

// gatedPathStorage blocks every write whose path contains block until
// released; other writes are counted and return at once.
type gatedPathStorage struct {
	mockStorageBackend
	block    string
	release  chan struct{}
	blocked  atomic.Int32
	fast     atomic.Int32
	slowDone atomic.Int32
}

func (s *gatedPathStorage) Write(ctx context.Context, path string, _ []byte) error {
	if strings.Contains(path, s.block) {
		s.blocked.Add(1)
		select {
		case <-s.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		s.slowDone.Add(1)
		return nil
	}
	s.fast.Add(1)
	return nil
}

func (s *gatedPathStorage) WriteReader(ctx context.Context, path string, r io.Reader, _ int64) error {
	data, _ := io.ReadAll(r)
	return s.Write(ctx, path, data)
}

func newSweepBuffer1695(t *testing.T, st *gatedPathStorage, workers int) *ArrowBuffer {
	t.Helper()
	cfg := &config.IngestConfig{
		MaxBufferSize: 1 << 20, MaxBufferAgeMS: 3_600_000, Compression: "snappy",
		FlushWorkers: workers, FlushQueueSize: 8, ShardCount: 1, FlushTimeoutSeconds: 30,
	}
	b := NewArrowBuffer(cfg, st, zerolog.New(io.Discard))
	t.Cleanup(func() {
		select {
		case <-st.release:
		default:
			close(st.release)
		}
		_ = b.Close()
	})
	return b
}

func ageAll1695(b *ArrowBuffer) {
	for _, sh := range b.shards {
		sh.mu.Lock()
		for k := range sh.bufferStartTimes {
			sh.bufferStartTimes[k] = time.Now().UTC().Add(-2 * time.Hour)
		}
		sh.mu.Unlock()
	}
}

func write1695(t *testing.T, b *ArrowBuffer, measurement string, hours int) {
	t.Helper()
	times := make([]interface{}, hours)
	vals := make([]interface{}, hours)
	for i := range times {
		times[i] = base1692 + int64(i)*hour1692
		vals[i] = float64(i)
	}
	require.NoError(t, b.WriteColumnarDirect(context.Background(), "db", measurement, map[string][]interface{}{"time": times, "v": vals}))
}

// A backfill whose writes hang holds one sweep slot; the sweep returns, and
// the other aged buffers are written without waiting for it.
func TestAgedSweepDoesNotWaitForABackfill_1695(t *testing.T) {
	st := &gatedPathStorage{block: "/slow/", release: make(chan struct{})}
	b := newSweepBuffer1695(t, st, 4)
	write1695(t, b, "slow", 3)
	for i := 0; i < 6; i++ {
		write1695(t, b, fmt.Sprintf("fast%d", i), 1)
	}
	b.cancel() // only the sweep under test runs
	ageAll1695(b)

	done := make(chan struct{})
	go func() { b.flushAgedBuffers(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the sweep waited for the backfill's write")
	}
	require.Eventually(t, func() bool { return st.fast.Load() == 6 }, 5*time.Second, 10*time.Millisecond,
		"every other aged buffer written while the backfill hangs")
	require.EqualValues(t, 1, st.blocked.Load())

	close(st.release)
	b.agedWG.Wait()
	require.EqualValues(t, 3, st.slowDone.Load(), "the backfill completes once released")
}

// While a key's aged write runs, the sweep does not extract it again: one
// slow backfill cannot take every slot, and its new rows stay buffered.
func TestAgedSweepSkipsAKeyInFlight_1695(t *testing.T) {
	st := &gatedPathStorage{block: "/slow/", release: make(chan struct{})}
	b := newSweepBuffer1695(t, st, 4)
	write1695(t, b, "slow", 2)
	b.cancel()
	ageAll1695(b)
	b.flushAgedBuffers()
	require.Eventually(t, func() bool { return st.blocked.Load() == 1 }, 5*time.Second, 10*time.Millisecond)

	write1695(t, b, "slow", 2) // more history for the same key
	ageAll1695(b)
	b.flushAgedBuffers()
	b.shards[0].mu.RLock()
	_, buffered := b.shards[0].buffers["db/slow"]
	b.shards[0].mu.RUnlock()
	require.True(t, buffered, "a key with an aged write in flight is left in the buffer")
	require.EqualValues(t, 1, st.blocked.Load(), "no second concurrent write of the key")
	require.True(t, b.computeNextFlushDeadline().After(time.Now().Add(time.Minute)),
		"an in-flight key is out of the flush deadline (else periodicFlush spins)")

	close(st.release)
	b.agedWG.Wait()
	require.False(t, b.agedKeyInFlight("db/slow"))
}

// A barrier scoped to one database does not wait for another database's
// in-flight flush; one scoped to it, or unscoped, does.
func TestScopedBarrierIgnoresOtherKeys_1695(t *testing.T) {
	b := NewArrowBuffer(cfg1692(30), &mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	taskID := b.beginAsyncFlushTask("dbA/backfill") // a long flush of dbA
	defer b.completeAsyncFlushTask(taskID)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := b.FlushScopeAndWait(ctx, "dbB", "")
	require.NoError(t, err, "dbB does not wait for dbA's flush")

	short, cancelShort := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelShort()
	_, err = b.FlushScopeAndWait(short, "dbA", "")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "still in progress")

	short2, cancelShort2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelShort2()
	require.ErrorIs(t, b.FlushAllAndWait(short2), context.DeadlineExceeded, "the unscoped barrier fences every task")
}

// A task with no key is fenced by every barrier.
func TestScopedBarrierFencesUnkeyedTasks_1695(t *testing.T) {
	b := NewArrowBuffer(cfg1692(30), &mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	taskID := b.beginAsyncFlushTask("")
	defer b.completeAsyncFlushTask(taskID)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := b.FlushScopeAndWait(ctx, "dbB", "")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// A barrier waiting for its own scope's flush does not hold the gate: a
// concurrent barrier for another scope returns meanwhile.
func TestWaitingBarrierDoesNotHoldTheGate_1695(t *testing.T) {
	b := NewArrowBuffer(cfg1692(30), &mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	taskID := b.beginAsyncFlushTask("dbA/backfill")

	var wg sync.WaitGroup
	wg.Add(1)
	aErr := make(chan error, 1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := b.FlushScopeAndWait(ctx, "dbA", "")
		aErr <- err
	}()
	time.Sleep(100 * time.Millisecond) // dbA's barrier is now waiting on its fence

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	_, err := b.FlushScopeAndWait(ctx, "dbB", "")
	require.NoError(t, err, "dbB's barrier is not queued behind dbA's wait")
	require.Less(t, time.Since(start), 500*time.Millisecond)

	b.completeAsyncFlushTask(taskID)
	wg.Wait()
	require.NoError(t, <-aErr, "dbA's barrier returns once its flush completes")
}

// Close waits for a detached aged write: it does not return, and so cannot
// report a clean flush, while the backfill's write still runs.
func TestCloseWaitsForDetachedAgedWrite_1695(t *testing.T) {
	st := &gatedPathStorage{block: "/slow/", release: make(chan struct{})}
	b := newSweepBuffer1695(t, st, 4)
	write1695(t, b, "slow", 3)
	b.cancel()
	ageAll1695(b)
	b.flushAgedBuffers()
	require.Eventually(t, func() bool { return st.blocked.Load() == 1 }, 5*time.Second, 10*time.Millisecond)

	closed := make(chan struct{})
	go func() { _ = b.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while an aged write was still running")
	case <-time.After(500 * time.Millisecond):
	}
	close(st.release)
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return after the write finished")
	}
	require.EqualValues(t, 3, st.slowDone.Load(), "the backfill's hour files were written before Close returned")
	require.True(t, b.CloseFlushedCleanly())
}

// With periodicFlush live: rows that reached a key while its aged write ran
// are written as soon as that write ends (the nudge), not a full
// max_buffer_age later at the next timer.
func TestAgedKeyPickedUpWhenItsWriteEnds_1695(t *testing.T) {
	st := &gatedPathStorage{block: "/slow/", release: make(chan struct{})}
	cfg := &config.IngestConfig{
		MaxBufferSize: 1 << 20, MaxBufferAgeMS: 2000, Compression: "snappy",
		FlushWorkers: 2, FlushQueueSize: 8, ShardCount: 1, FlushTimeoutSeconds: 30,
	}
	b := NewArrowBuffer(cfg, st, zerolog.New(io.Discard))
	t.Cleanup(func() {
		select {
		case <-st.release:
		default:
			close(st.release)
		}
		_ = b.Close()
	})
	write1695(t, b, "slow", 1)
	require.Eventually(t, func() bool { return st.blocked.Load() == 1 }, 6*time.Second, 20*time.Millisecond,
		"the first aged write starts and hangs")
	write1695(t, b, "slow", 1) // new rows for the key while its write runs
	// Past the next sweep, which skips the key and leaves it out of the
	// deadline, so the timer is armed a full max age ahead.
	time.Sleep(2300 * time.Millisecond)
	require.EqualValues(t, 1, st.blocked.Load(), "the key was not extracted again while in flight")

	start := time.Now()
	close(st.release)
	require.Eventually(t, func() bool { return st.slowDone.Load() == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Less(t, time.Since(start), time.Second,
		"the key's new rows waited for the next timer instead of being picked up when its write ended")
}
