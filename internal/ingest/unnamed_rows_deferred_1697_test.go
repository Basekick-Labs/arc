package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1697: with a WAL, a multi-hour flush registers each hour file as it is
// stored and checkpoints its rows (#1492). A file holding rows the row map
// cannot name (a chunked payload; more than maxRowCheckpointRanges ranges) is
// deferred: registered after the last hour, and on a part-failed flush handed
// to the ambiguous-write ledger, which removes it once replay has stored its
// rows, never before.

const (
	tokC = "0123456789abcdef0000000000000c01"
	tokD = "0123456789abcdef0000000000000d01"
)

// memStore1697 keeps what was written, by path and size, and fails the
// failAt-th write (1-based; 0 never).
type memStore1697 struct {
	mockStorageBackend
	mu     sync.Mutex
	files  map[string]int64
	writes atomic.Int64
	failAt int64
}

func newMemStore1697(failAt int64) *memStore1697 {
	return &memStore1697{files: map[string]int64{}, failAt: failAt}
}

func (m *memStore1697) Write(_ context.Context, path string, data []byte) error {
	if m.writes.Add(1) == m.failAt {
		return errors.New("injected storage failure")
	}
	m.mu.Lock()
	m.files[path] = int64(len(data))
	m.mu.Unlock()
	return nil
}

func (m *memStore1697) StatFile(_ context.Context, path string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if size, ok := m.files[path]; ok {
		return size, nil
	}
	return -1, nil
}

func (m *memStore1697) Delete(_ context.Context, path string) error {
	m.mu.Lock()
	delete(m.files, path)
	m.mu.Unlock()
	return nil
}

func (m *memStore1697) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.files)
}

func walBuffer1697(t *testing.T, store *memStore1697) *ArrowBuffer {
	t.Helper()
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(t.TempDir(), "wal"), SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	cfg := cfg1692(5)
	b := NewArrowBuffer(cfg, store, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close(); _ = w.Close() })
	b.SetWAL(w)
	return b
}

// writeChunked buffers a record whose rows are two WAL entries (a chunked
// payload): its span has no parent, only tokens.
func writeChunked(t *testing.T, b *ArrowBuffer, rec *models.ColumnarRecord) {
	t.Helper()
	require.NoError(t, b.writeColumnarInternalWithWALCopy(context.Background(), "db", rec, true, []string{tokC, tokD}, true))
}

// hoursFrom is hoursRecord1692 starting `first` hours after its base.
func hoursFrom(first, n int, payload byte) *models.ColumnarRecord {
	rec := hoursRecord1692(n, payload)
	for i := range rec.Columns["time"] {
		rec.Columns["time"][i] = rec.Columns["time"][i].(int64) + int64(first)*hour1692
	}
	return rec
}

// A part-failed flush that stored only deferred files: none is registered,
// every one is handed to the ledger (before partial()'s storedFiles == 0
// return), and the ledger removes them only once their rows are stored again.
func TestDeferredFilesHandedToLedgerOnPartialFailure_1697(t *testing.T) {
	store := newMemStore1697(4) // hours 1-3 stored, the 4th fails
	b := walBuffer1697(t, store)
	reg := &recordingRegistrar1693{}
	b.SetFileRegistrar(reg)

	writeChunked(t, b, hoursRecord1692(6, 1))
	err, _ := flushWithin(t, b, 20*time.Second)
	require.Error(t, err)
	var partial *flushPartialError
	require.False(t, errors.As(err, &partial), "nothing registered: not a partial-stored error")

	require.Zero(t, reg.count(), "a deferred file is never registered by a failed flush")
	require.Equal(t, 3, store.count(), "the three deferred files are stored")
	require.GreaterOrEqual(t, b.ambiguous.count.Load(), int64(3), "each is in the ledger")

	// Replay has not stored the rows: the files stay, however often the
	// sweeper looks. This is what makes a WAL that accepted and then failed
	// an entry safe: the file is never removed on less than that proof.
	b.sweepAmbiguousWrites(context.Background())
	require.Equal(t, 3, store.count(), "kept until replay stores their rows")

	// Replay re-stores both chunked entries whole.
	b.ambiguous.stored([]string{tokC, tokD})
	b.sweepAmbiguousWrites(context.Background())
	require.Zero(t, store.count(), "removed once their rows are stored elsewhere")
}

// Success: every deferred file is registered once, after the last hour.
func TestDeferredFilesRegisteredAfterLastHour_1697(t *testing.T) {
	store := newMemStore1697(0)
	b := walBuffer1697(t, store)
	reg := &recordingRegistrar1693{}
	b.SetFileRegistrar(reg)

	before := b.totalRecordsWritten.Load()
	writeChunked(t, b, hoursRecord1692(4, 2))
	err, _ := flushWithin(t, b, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, 4, reg.count(), "each deferred file registered once")
	require.EqualValues(t, 4, b.totalRecordsWritten.Load()-before, "records counted once")
	require.Zero(t, b.ambiguous.count.Load(), "nothing handed to the ledger")
}

// Nameable hours are written first and register as stored; a failure on a
// deferred hour leaves them registered and checkpointed, and only the
// deferred file in the ledger.
func TestNameableHoursFirstDeferredHandedOver_1697(t *testing.T) {
	store := newMemStore1697(5) // 3 nameable + 1 deferred stored, the 5th (deferred) fails
	b := walBuffer1697(t, store)
	reg := &recordingRegistrar1693{}
	b.SetFileRegistrar(reg)

	require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursFrom(0, 3, 3), false, "")) // nameable, hours 0-2
	writeChunked(t, b, hoursFrom(3, 3, 4))                                                                 // chunked, hours 3-5
	err, _ := flushWithin(t, b, 20*time.Second)
	require.Error(t, err)

	var partial *flushPartialError
	require.True(t, errors.As(err, &partial))
	require.Equal(t, 3, partial.StoredFiles, "only the registered (nameable) files count as stored")
	require.Equal(t, 3, reg.count(), "the nameable hours were written first and registered")
	require.Equal(t, 4, store.count())
	require.GreaterOrEqual(t, b.ambiguous.count.Load(), int64(1), "the stored deferred file is in the ledger")
}

// A file holding a row with no WAL copy is that row's only copy: it is never
// deferred, so it is registered as stored even when a later hour fails.
func TestCopylessRowsKeepTheEarlyPath_1697(t *testing.T) {
	store := newMemStore1697(3) // 2 stored, the 3rd fails
	b := walBuffer1697(t, store)
	reg := &recordingRegistrar1693{}
	b.SetFileRegistrar(reg)

	writeChunked(t, b, hoursRecord1692(4, 5))
	// The same hours, rows with no WAL copy (a replicated entry): every file
	// holds both kinds.
	require.NoError(t, b.writeColumnarInternalWithWALCopy(context.Background(), "db", hoursRecord1692(4, 6), true, nil, false))
	err, _ := flushWithin(t, b, 20*time.Second)
	require.Error(t, err)
	require.Equal(t, 2, reg.count(), "registered as stored: never deferred")
	require.Equal(t, 2, store.count())
}

// More than maxRowCheckpointRanges ranges in one file (a backfill sent as many
// small batches): rangesIn gives nothing, so the file cannot be checkpointed
// with its registration. It is deferred, and named for the ledger by its full,
// unbounded ranges.
func TestOverRangeCapFileDeferred_1697(t *testing.T) {
	store := newMemStore1697(2) // the first file stored, the second fails
	b := walBuffer1697(t, store)
	reg := &recordingRegistrar1693{}
	b.SetFileRegistrar(reg)

	for i := 0; i < maxRowCheckpointRanges+50; i++ {
		// One row in each of two hours per batch: each file gets one range
		// per batch.
		require.NoError(t, b.writeColumnarInternal(context.Background(), "db", hoursRecord1692(2, byte(i)), false, ""))
	}
	err, _ := flushWithin(t, b, 60*time.Second)
	require.Error(t, err)
	require.Zero(t, reg.count(), "over the cap: deferred, not registered by a failed flush")
	require.Equal(t, 1, store.count())
	require.GreaterOrEqual(t, b.ambiguous.count.Load(), int64(1), "named by its ranges, in the ledger")
}

// Coverage on (cluster): a flush whose every file is deferred announces its
// group once, after the loop, and registers every file with it. The group is
// otherwise announced by the first early registration, which such a flush
// never makes; registering without it would leave the group unannounced and
// its batches counted lost.
func TestAllDeferredRegisterWithCoverage_1697(t *testing.T) {
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(t.TempDir(), "wal"), SyncMode: wal.SyncModeAsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy", ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 16, FlushTimeoutSeconds: 5},
		&mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = b.Close(); _ = w.Close() })
	b.SetWAL(w)
	reg := &coverageRegistrar{}
	b.SetFileRegistrar(reg)
	require.NoError(t, b.EnableCoverage())

	// A chunked record written through the WAL keeps its own tokens; the
	// coverage group is built from the buffer's batches.
	rec := hoursRecord1692(3, 7)
	rec.RawPayload = []byte{0x16, 0x97}
	require.NoError(t, b.writeColumnarInternalWithWALCopy(context.Background(), "db", rec, false, nil, false))
	// Force the batch's identities to a chunked shape: two tokens, no parent.
	forceChunked(t, b)

	err, _ = flushWithin(t, b, 20*time.Second)
	require.NoError(t, err)
	files := reg.snapshot()
	require.Len(t, files, 3, "every deferred file registered with coverage")
	require.Zero(t, reg.plain, "none registered without coverage (group announced)")
	for _, f := range files {
		require.NotNil(t, f.cov)
	}
}

// forceChunked rewrites every buffered batch's WAL identities to two tokens,
// the shape a chunked payload has.
func forceChunked(t *testing.T, b *ArrowBuffer) {
	t.Helper()
	n := 0
	for _, shard := range b.shards {
		shard.mu.Lock()
		for _, recs := range shard.buffers {
			for _, r := range recs {
				if batch, ok := r.(*TypedColumnBatch); ok {
					batch.WALHashes = []string{tokC, tokD}
					batch.hasWALCopy = true
					n++
				}
			}
		}
		shard.mu.Unlock()
	}
	require.Positive(t, n)
}

// deferredNeed: unnamed rows named by their tokens, named rows by unbounded
// ranges; nothing for a file with a copy-less row or an unnamed row with no
// tokens.
func TestDeferredNeed_1697(t *testing.T) {
	s := walRowSpansOf([]interface{}{
		spanBatch(3, []string{tokA}, true),       // 0-2 named
		spanBatch(2, []string{tokC, tokD}, true), // 3-4 chunked
		spanBatch(2, nil, false),                 // 5-6 copy-less
		spanBatch(1, []string{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, true), // 7 content hash
	})
	require.NotNil(t, s)

	ids, ok := s.deferredNeed([]int{0, 2, 3, 4})
	require.True(t, ok)
	require.ElementsMatch(t, []string{tokC, tokD, wal.RecoveryRowIdentity(tokA, 0, 1), wal.RecoveryRowIdentity(tokA, 2, 3)}, ids)

	_, ok = s.deferredNeed([]int{3, 5})
	require.False(t, ok, "a copy-less row: the file is its only copy")
	_, ok = s.deferredNeed([]int{0, 7})
	require.False(t, ok, "an unnamed row with no tracked token cannot be matched by replay")
}

// End to end, with the WAL, real recovery and the ledger sweep: a backfill
// sent as more small batches than a row checkpoint can name, whose flush
// fails after storing its first hour file. Recovery replays every row of the
// flush; the deferred file is removed once those rows are stored again, so
// every row is stored exactly once. Before #1697 the first file was
// registered with no row checkpoint and kept, and its rows were stored twice.
func TestPartFailedFlushOverRangeCapStoresEachRowOnce_1697(t *testing.T) {
	f := newAmbiguousFixture(t)
	const batches = maxRowCheckpointRanges + 30
	h0 := time.Now().UTC().Truncate(time.Hour).Add(-48 * time.Hour).UnixMicro()
	for i := 0; i < batches; i++ {
		// One row in each of two hours per batch, so each hour file holds one
		// range per batch: more than a checkpoint names.
		payload, err := msgpack.Marshal(map[string]interface{}{"m": "probe", "columns": map[string]interface{}{
			"time": []interface{}{h0 + int64(i), h0 + hour1692 + int64(i)},
			"v":    []interface{}{float64(i), float64(i)},
		}})
		require.NoError(t, err)
		decoded, err := NewMessagePackDecoder(zerolog.Nop()).Decode(payload)
		require.NoError(t, err)
		require.NoError(t, f.buffer.Write(context.Background(), "db", decoded))
	}
	f.storage.mu.Lock()
	f.storage.failAt = 2 // the first hour file is stored, the second fails
	f.storage.mu.Unlock()
	require.Error(t, f.buffer.FlushAll(context.Background()))
	require.Len(t, f.files(t), 1, "the first (deferred) hour file is stored")

	f.replay(t)
	f.sweep(0)
	require.EqualValues(t, 2*batches, fixtureRows(t, f), "every row stored exactly once")
}

func fixtureRows(t *testing.T, f *ambiguousFixture) int64 {
	t.Helper()
	var rows int64
	for _, p := range f.files(t) {
		if !filepath.IsAbs(p) {
			p = filepath.Join(f.dir, p)
		}
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		reader, err := file.NewParquetReader(bytes.NewReader(data))
		require.NoError(t, err)
		rows += reader.NumRows()
		require.NoError(t, reader.Close())
	}
	return rows
}

// A deferred file whose rows are never seen replayed (a WAL that accepted an
// entry and failed to write it; replay under another identity) may be their
// only copy: after deferredWriteMaxAge it is registered, not dropped, so a
// cluster's reconciliation sweep never takes it for an orphan.
func TestUnmetDeferredRegisteredAfterMaxAge_1697(t *testing.T) {
	store := newMemStore1697(3) // 2 deferred stored, the 3rd fails
	b := walBuffer1697(t, store)
	reg := &recordingRegistrar1693{}
	b.SetFileRegistrar(reg)
	clock := time.Now()
	b.ambiguous.now = func() time.Time { return clock }

	writeChunked(t, b, hoursRecord1692(4, 8))
	err, _ := flushWithin(t, b, 20*time.Second)
	require.Error(t, err)
	require.Zero(t, reg.count())

	clock = clock.Add(deferredWriteMaxAge - time.Minute)
	b.sweepAmbiguousWrites(context.Background())
	require.Zero(t, reg.count(), "still waiting for replay")

	clock = clock.Add(2 * time.Minute)
	b.sweepAmbiguousWrites(context.Background())
	require.Equal(t, 2, reg.count(), "registered as the copy of rows never seen replayed")
	require.Equal(t, 2, store.count(), "and kept")
}

// Close registers a deferred file still waiting: the ledger does not outlive
// the process.
func TestUnmetDeferredRegisteredAtClose_1697(t *testing.T) {
	store := newMemStore1697(3)
	b := walBuffer1697(t, store)
	reg := &recordingRegistrar1693{}
	b.SetFileRegistrar(reg)

	writeChunked(t, b, hoursRecord1692(4, 9))
	err, _ := flushWithin(t, b, 20*time.Second)
	require.Error(t, err)
	require.Zero(t, reg.count())
	_ = b.Close()
	require.Equal(t, 2, reg.count(), "registered at Close")
}

// A need past maxDeferredNeedRanges names the parents whole, keeping the
// ledger linear.
func TestDeferredNeedCappedToParents_1697(t *testing.T) {
	// One entry whose rows alternate between two hours: every other row is in
	// one file, each its own range.
	n := 2*maxDeferredNeedRanges + 10
	s := walRowSpansOf([]interface{}{spanBatch(n, []string{tokA}, true)})
	require.NotNil(t, s)
	var even []int
	for p := 0; p < n; p += 2 {
		even = append(even, p)
	}
	nameable, deferrable := s.classifyHour(even, maxRowCheckpointRanges)
	require.False(t, nameable)
	require.True(t, deferrable)
	ids, ok := s.deferredNeed(even)
	require.True(t, ok)
	require.Equal(t, []string{tokA}, ids, "past the cap: the parent, whole")
}

// classifyHour agrees with what rangesIn and deferredNeed would do.
func TestClassifyHourAgreesWithRangesAndNeed_1697(t *testing.T) {
	s := walRowSpansOf([]interface{}{
		spanBatch(3, []string{tokA}, true),
		spanBatch(2, []string{tokC, tokD}, true),
		spanBatch(2, nil, false),
		spanBatch(4, []string{tokB}, true),
	})
	require.NotNil(t, s)
	for _, idx := range [][]int{{0, 1, 2}, {0, 2}, {0, 3}, {3, 4}, {4, 5}, {7, 8, 10}, {0, 7, 9}, {5, 6}} {
		r := s.rangesIn(idx)
		wantNameable := rangeRows(r) == len(idx)
		_, wantDeferrable := s.deferredNeed(idx)
		nameable, deferrable := s.classifyHour(idx, maxRowCheckpointRanges)
		require.Equal(t, wantNameable, nameable, "nameable %v", idx)
		require.Equal(t, !wantNameable && wantDeferrable, deferrable, "deferrable %v", idx)
	}
}

// A large in-order need is recorded and met in linear time.
func TestLedgerLargeOrderedNeedIsLinear_1697(t *testing.T) {
	var ids []string
	for i := 0; i < 50000; i++ {
		ids = append(ids, wal.RecoveryRowIdentity(tokA, 2*i, 2*i+1))
	}
	l := newAmbiguousLedger()
	start := time.Now()
	require.True(t, l.record("p", 1, ids))
	require.Less(t, time.Since(start), 2*time.Second)
}
