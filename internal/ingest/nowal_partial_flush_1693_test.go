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

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1693: without a WAL, a multi-hour flush that fails part-way registers the
// hour files it already stored, and reports only the rows actually lost.

// failAtBackend stores writes until the failAt-th (1-based), which fails.
type failAtBackend struct {
	mockStorageBackend
	failAt int64
	writes atomic.Int64
}

func (f *failAtBackend) Write(_ context.Context, _ string, _ []byte) error {
	if f.writes.Add(1) == f.failAt {
		return errors.New("injected storage failure")
	}
	return nil
}

type recordingRegistrar1693 struct {
	mu    sync.Mutex
	paths []string
}

func (r *recordingRegistrar1693) RegisterFile(_, _, path string, _ time.Time, _ int64, _ string) {
	r.mu.Lock()
	r.paths = append(r.paths, path)
	r.mu.Unlock()
}

func (r *recordingRegistrar1693) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.paths)
}

func droppedRecords() int64 {
	return metrics.Get().Snapshot()["buffer_records_dropped_total"].(int64)
}

// The flush stores hours 1..K-1, fails at K: those K-1 files are registered,
// the error reports them, and the dropped-records metric counts only the
// rows in no stored file.
func TestNoWALPartialFlushRegistersStoredHours_1693(t *testing.T) {
	const hours, failAt = 10, 7 // hours 1..6 stored, 7 fails, 8..10 never attempted
	store := &failAtBackend{failAt: failAt}
	reg := &recordingRegistrar1693{}
	b := NewArrowBuffer(cfg1692(5), store, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	b.SetFileRegistrar(reg)

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(hours, 1), false, ""))
	before := droppedRecords()
	err, _ := flushWithin(t, b, 20*time.Second)
	require.Error(t, err)

	var partial *flushPartialError
	require.True(t, errors.As(err, &partial), "a part-stored flush reports what it stored: %v", err)
	require.Equal(t, failAt-1, partial.StoredFiles)
	require.Equal(t, failAt-1, partial.StoredRows)
	require.Equal(t, hours-(failAt-1), partial.LostWithoutWAL, "lost = rows in no stored file")
	require.False(t, partial.Checkpointed)
	require.NotContains(t, err.Error(), "checkpointed")

	require.Equal(t, failAt-1, reg.count(), "every stored hour file is registered")
	require.EqualValues(t, hours-(failAt-1), droppedRecords()-before, "only the rows in no stored file count as dropped")
}

// A successful WAL-off multi-hour flush registers each file once and counts
// its records once (the per-hour totals and the post-loop totals must not
// both run).
func TestNoWALMultiHourFlushCountsOnce_1693(t *testing.T) {
	const hours = 5
	reg := &recordingRegistrar1693{}
	b := NewArrowBuffer(cfg1692(5), &mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	b.SetFileRegistrar(reg)

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(hours, 2), false, ""))
	writtenBefore, flushesBefore := b.totalRecordsWritten.Load(), b.totalFlushes.Load()
	err, _ := flushWithin(t, b, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, hours, reg.count())
	require.EqualValues(t, hours, b.totalRecordsWritten.Load()-writtenBefore)
	require.EqualValues(t, hours, b.totalFlushes.Load()-flushesBefore)
}

// With a WAL and a merged batch that has no row map at all (a layout the
// merge does not produce today; set by hand), registration still waits for
// the last hour: recovery replays the whole flush, and an early-registered
// file beside its replayed twin would double its rows.
func TestWALWithoutRowMapKeepsDeferredRegistration_1693(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(dir, "wal"), SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	store := &failAtBackend{failAt: 3}
	reg := &recordingRegistrar1693{}
	b := NewArrowBuffer(cfg1692(5), store, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close(); _ = w.Close() })
	b.SetWAL(w)
	b.SetFileRegistrar(reg)

	merged, err := b.mergeBatchesForFlush("backfill", []interface{}{typedHoursBatch1693(t, b, 5)})
	require.NoError(t, err)
	merged.walSpans = nil // a layout the row map cannot name
	err = b.flushPartitionedData(context.Background(), "db/backfill", "db", "backfill", merged, 5, flushTypeAsync, time.Now(), nil)
	require.Error(t, err)
	var partial *flushPartialError
	require.False(t, errors.As(err, &partial), "nothing was registered, so nothing is reported stored")
	require.Zero(t, reg.count(), "no hour file registered before the last hour succeeded")
}

// Close's drain counts a part-stored WAL-off flush's rows correctly: none is
// "unwritten with a WAL copy" (that count went negative and hid other tasks'
// rows).
func TestCloseDrainPartialNoWALNotNegative_1693(t *testing.T) {
	store := &failAtBackend{failAt: 4}
	b := NewArrowBuffer(cfg1692(5), store, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	b.SetFileRegistrar(&recordingRegistrar1693{})
	records := []interface{}{typedHoursBatch1693(t, b, 6)}
	task := flushTask{bufferKey: "db/backfill", database: "db", measurement: "backfill", records: records, recordCount: 6}
	task.taskID = b.beginAsyncFlushTask()
	b.adjustFlushRecordMetrics(int64(task.recordCount), 0) // as enqueue does
	b.flushQueue <- task
	unwrit, lost := b.drainAndFlushQueue(time.Now().Add(10 * time.Second))
	require.Zero(t, unwrit, "no WAL: nothing is unwritten-with-a-copy, and never negative")
	require.Zero(t, lost, "the task was attempted")
}

// The defensive fallback: no WAL and no row map. Every row not stored is
// lost, and every stored row is copy-less.
func TestNoWALNoRowMapPartialCountsLoss_1693(t *testing.T) {
	store := &failAtBackend{failAt: 3}
	reg := &recordingRegistrar1693{}
	b := NewArrowBuffer(cfg1692(5), store, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close() })
	b.SetFileRegistrar(reg)
	merged, err := b.mergeBatchesForFlush("backfill", []interface{}{typedHoursBatch1693(t, b, 5)})
	require.NoError(t, err)
	merged.walSpans = nil
	err = b.flushPartitionedData(context.Background(), "db/backfill", "db", "backfill", merged, 5, flushTypeAsync, time.Now(), nil)
	var partial *flushPartialError
	require.True(t, errors.As(err, &partial), "%v", err)
	require.Equal(t, 2, partial.StoredRows)
	require.Equal(t, 3, partial.LostWithoutWAL)
	require.Equal(t, 2, partial.StoredWithoutWAL)
	require.Equal(t, 2, reg.count())
}

// storedWithoutCopy counts stored positions of copy-less spans only.
func TestStoredWithoutCopy_1693(t *testing.T) {
	s := &walRowSpans{rows: 6, spans: []walRowSpan{{lo: 0, hi: 3, copied: true}, {lo: 3, hi: 6}}}
	written := []bool{true, true, false, true, false, true}
	require.Equal(t, 2, s.storedWithoutCopy(written))
	require.Equal(t, 1, s.lostWithoutCopy(written))
	require.Zero(t, (*walRowSpans)(nil).storedWithoutCopy(written))
}

// typedHoursBatch1693 is a TypedColumnBatch with one row in each of n hours,
// as the write path builds it.
func typedHoursBatch1693(t *testing.T, b *ArrowBuffer, n int) *TypedColumnBatch {
	t.Helper()
	times := make([]int64, n)
	vals := make([]float64, n)
	for i := range times {
		times[i] = base1692 + int64(i)*hour1692
		vals[i] = float64(i)
	}
	return &TypedColumnBatch{Data: map[string]interface{}{"time": times, "value": vals}}
}
