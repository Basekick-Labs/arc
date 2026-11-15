package ingest

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1692: ingest.flush_timeout_seconds bounds each file of a flush, not the
// whole flush. A multi-hour flush writes one file per hour, one after another;
// a single deadline over all of them failed any backfill of more than ~1,100
// hours at ~25 ms per same-region S3 PUT.

const hour1692 = int64(3_600_000_000)
const base1692 = int64(1_790_856_000_000_000) // an hour boundary, in µs

// hoursRecord1692 is one row in each of n consecutive hours.
func hoursRecord1692(n int, payload byte) *models.ColumnarRecord {
	times := make([]interface{}, n)
	vals := make([]interface{}, n)
	for i := 0; i < n; i++ {
		times[i] = base1692 + int64(i)*hour1692
		vals[i] = float64(i)
	}
	return &models.ColumnarRecord{
		Measurement: "backfill", Columnar: true, TimeUnit: "us", RawPayload: []byte{0x16, 0x92, payload},
		Columns: map[string][]interface{}{"time": times, "value": vals},
	}
}

func cfg1692(timeoutSeconds int) *config.IngestConfig {
	return &config.IngestConfig{
		MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 16, FlushTimeoutSeconds: timeoutSeconds,
	}
}

// flushWithin runs FlushAll and fails the test if it has not returned by limit,
// so a flush left unbounded fails here rather than hanging the package.
func flushWithin(t *testing.T, b *ArrowBuffer, limit time.Duration) (error, time.Duration) {
	t.Helper()
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- b.FlushAll(context.Background()) }()
	select {
	case err := <-done:
		return err, time.Since(start)
	case <-time.After(limit):
		t.Fatalf("FlushAll still running after %s: the flush is not bounded", limit)
		return nil, 0
	}
}

// Many healthy writes that together take longer than flushTimeout: every one
// is stored. On main the flush failed at the deadline part-way (#1692).
func TestMultiHourFlush_TimeoutIsPerFile_1692(t *testing.T) {
	store := &ctxHonouringBackend{delay: 60 * time.Millisecond}
	b := NewArrowBuffer(cfg1692(1), store, zerolog.Nop())
	t.Cleanup(func() { _ = b.Close() })

	const hours = 30 // 30 × 60 ms = 1.8 s > the 1 s timeout; each write ≪ 1 s
	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(hours, 1), true, ""))

	err, took := flushWithin(t, b, 20*time.Second)
	require.NoError(t, err)
	require.Greater(t, took, time.Second, "the test must make the flush outlast one flushTimeout")
	require.EqualValues(t, hours, store.completed.Load(), "every hour file stored")
	require.Zero(t, store.cancelled.Load(), "no write cut short")
}

// One write slower than flushTimeout still fails: the bound is per file, not
// gone.
func TestMultiHourFlush_OneSlowWriteStillTimesOut_1692(t *testing.T) {
	store := &ctxHonouringBackend{delay: 5 * time.Second}
	b := NewArrowBuffer(cfg1692(1), store, zerolog.Nop())
	t.Cleanup(func() { _ = b.Close() })

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(1, 2), true, ""))
	err, took := flushWithin(t, b, 4*time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, took, 3*time.Second)
	require.EqualValues(t, 1, store.cancelled.Load())
}

// blockingRegistrar1692 is a field-schema registrar whose EnsureFile does what
// a hung storage call inside the registry does: block until its context ends.
type blockingRegistrar1692 struct {
	mu        sync.Mutex
	calls     int
	deadlines int
	remaining []time.Duration // time left on the call's deadline when it started
	errs      []error
}

func (r *blockingRegistrar1692) EnsureFile(ctx context.Context, _, _ string, _ *arrow.Schema, _ []string, _ string) error {
	r.mu.Lock()
	r.calls++
	if dl, ok := ctx.Deadline(); ok {
		r.deadlines++
		r.remaining = append(r.remaining, time.Until(dl))
	}
	r.mu.Unlock()
	<-ctx.Done()
	r.mu.Lock()
	r.errs = append(r.errs, ctx.Err())
	r.mu.Unlock()
	return ctx.Err()
}

// The field-schema registry reads and writes storage under the flush's
// context. Each file's registration stays bounded by that file's timeout: a
// hung registry call must not stall the flush (the plan's first version bounded
// only storage.Write and would have left it waiting forever).
func TestMultiHourFlush_FieldSchemaCallBoundedPerFile_1692(t *testing.T) {
	b := NewArrowBuffer(cfg1692(1), &mockStorageBackend{}, zerolog.Nop())
	t.Cleanup(func() { _ = b.Close() })
	reg := &blockingRegistrar1692{}
	b.SetFieldSchema(reg)

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(3, 3), true, ""))
	err, _ := flushWithin(t, b, 15*time.Second)
	require.NoError(t, err, "a registry failure is logged, never fails the flush")

	reg.mu.Lock()
	defer reg.mu.Unlock()
	require.Equal(t, 3, reg.calls)
	require.Equal(t, 3, reg.deadlines, "each registration runs under a deadline")
	for _, e := range reg.errs {
		require.ErrorIs(t, e, context.DeadlineExceeded)
	}
	// Per file, not per flush: every call starts with (nearly) a whole
	// timeout. Under one flush-wide deadline the first call would use it up
	// and the later ones would start with none left.
	for i, left := range reg.remaining {
		require.Greater(t, left, 500*time.Millisecond, "call %d started with only %s of its deadline", i, left)
	}
}

// Same for the single-hour flush, which is every live-ingest flush. (One file:
// per file and per flush are the same bound here; this pins that the
// registration is bounded at all, which bounding only storage.Write would not.)
func TestSingleHourFlush_FieldSchemaCallBounded_1692(t *testing.T) {
	b := NewArrowBuffer(cfg1692(1), &mockStorageBackend{}, zerolog.Nop())
	t.Cleanup(func() { _ = b.Close() })
	reg := &blockingRegistrar1692{}
	b.SetFieldSchema(reg)

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(1, 4), true, ""))
	err, took := flushWithin(t, b, 10*time.Second)
	require.NoError(t, err)
	require.Less(t, took, 3*time.Second)
	reg.mu.Lock()
	defer reg.mu.Unlock()
	require.Equal(t, 1, reg.deadlines)
}

// blockingRowCheckpointWAL blocks every per-hour row checkpoint until its
// context ends, as a stalled WAL writer (slow fsync, full disk) does to
// MarkFlushedContext's admission.
type blockingRowCheckpointWAL struct {
	*wal.Writer
	rowCalls     atomic.Int64
	rowDeadlines atomic.Int64
	mu           sync.Mutex
	remaining    []time.Duration
}

func (w *blockingRowCheckpointWAL) MarkFlushedContext(ctx context.Context, ids []string) error {
	if _, _, _, ok := wal.ParseRecoveryRowIdentity(ids[0]); ok {
		w.rowCalls.Add(1)
		if dl, ok := ctx.Deadline(); ok {
			w.rowDeadlines.Add(1)
			w.mu.Lock()
			w.remaining = append(w.remaining, time.Until(dl))
			w.mu.Unlock()
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return w.Writer.MarkFlushedContext(ctx, ids)
}

// The WAL row checkpoint of each hour file is bounded by that file's timeout.
func TestMultiHourFlush_RowCheckpointBoundedPerFile_1692(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(dir, "wal"), SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	b := NewArrowBuffer(cfg1692(1), &mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close(); _ = w.Close() })
	bw := &blockingRowCheckpointWAL{Writer: w}
	b.SetWAL(bw)

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(3, 5), false, ""))
	err, _ = flushWithin(t, b, 15*time.Second)
	require.NoError(t, err, "a dropped row checkpoint is counted, never fails the flush")
	require.EqualValues(t, 3, bw.rowCalls.Load(), "one row checkpoint per hour file (per-hour path)")
	require.EqualValues(t, 3, bw.rowDeadlines.Load(), "each under a deadline")
	bw.mu.Lock()
	defer bw.mu.Unlock()
	for i, left := range bw.remaining {
		require.Greater(t, left, 500*time.Millisecond, "row checkpoint %d started with only %s: the bound is not per file", i, left)
	}
}

// sleepingBackend1692 ignores its context, as LocalBackend.Write does.
type sleepingBackend1692 struct {
	countingBackend
	started atomic.Int64
}

func (s *sleepingBackend1692) Write(ctx context.Context, p string, d []byte) error {
	s.started.Add(1)
	return s.countingBackend.Write(ctx, p, d)
}

// Close cancels flushParent when its budget runs out; a flush then stops
// between hour files even on a backend that ignores its context. Without the
// check between hours the loop would keep writing every remaining file and
// Close would wait for all of them.
func TestMultiHourFlush_StopsBetweenFilesWhenClosed_1692(t *testing.T) {
	store := &sleepingBackend1692{countingBackend: countingBackend{delay: 200 * time.Millisecond}}
	cfg := cfg1692(30)
	cfg.MaxBufferSize = 40 // the 40-row write below hands its buffer to a worker
	b := NewArrowBuffer(cfg, store, zerolog.Nop())
	b.SetCloseBudget(time.Second) // flushParent cancelled at budget/2

	const hours = 40 // 40 × 200 ms = 8 s if nothing stops the loop
	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(hours, 6), true, ""))
	require.Eventually(t, func() bool { return store.started.Load() >= 1 }, 5*time.Second, 10*time.Millisecond)

	start := time.Now()
	_ = b.Close()
	took := time.Since(start)
	n := store.started.Load()
	time.Sleep(500 * time.Millisecond)

	require.Less(t, took, 3*time.Second, "Close bounded by its budget, not by the remaining hour files")
	require.Less(t, n, int64(hours), "the flush stopped before writing every hour file")
	require.Equal(t, n, store.started.Load(), "no write after Close returned")
}

// fileContext derives from the context it is given: Close's own flushes run
// on a context that outlives a cancelled flushParent.
func TestFileContext_DerivesFromItsArgument_1692(t *testing.T) {
	b := NewArrowBuffer(cfg1692(1), &mockStorageBackend{}, zerolog.Nop())
	t.Cleanup(func() { _ = b.Close() })
	b.flushParentCancel()

	ctx, cancel := b.fileContext(context.Background())
	defer cancel()
	require.NoError(t, ctx.Err(), "not cancelled with flushParent")
	dl, ok := ctx.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(time.Second), dl, 200*time.Millisecond)

	flushCtx, flushCancel := b.newFlushContext()
	defer flushCancel()
	require.True(t, errors.Is(flushCtx.Err(), context.Canceled), "the flush context still follows flushParent")
	_, hasDeadline := flushCtx.Deadline()
	require.False(t, hasDeadline, "the flush itself has no deadline of its own (#1692)")
}
