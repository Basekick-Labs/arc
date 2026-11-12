package ingest

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// lateLandingStorage is a local backend whose next failing Write returns a
// client-side timeout but keeps the bytes, the way an object store that
// already held the body completes the PUT after the client gave up (#1635).
// land() completes it.
type lateLandingStorage struct {
	storage.Backend
	mu       sync.Mutex
	failNext bool
	pending  map[string][]byte
}

func (s *lateLandingStorage) Write(ctx context.Context, path string, data []byte) error {
	s.mu.Lock()
	if s.failNext {
		s.failNext = false
		s.pending[path] = append([]byte(nil), data...)
		s.mu.Unlock()
		return fmt.Errorf("operation error S3: PutObject: %w", context.DeadlineExceeded)
	}
	s.mu.Unlock()
	return s.Backend.Write(ctx, path, data)
}

// land completes the abandoned writes; it returns their paths.
func (s *lateLandingStorage) land(t *testing.T) []string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var paths []string
	for p, data := range s.pending {
		require.NoError(t, s.Backend.Write(context.Background(), p, data))
		paths = append(paths, p)
	}
	s.pending = map[string][]byte{}
	return paths
}

type ambiguousFixture struct {
	dir     string
	writer  *wal.Writer
	storage *lateLandingStorage
	buffer  *ArrowBuffer
	clock   time.Time
}

func newAmbiguousFixture(t *testing.T) *ambiguousFixture {
	t.Helper()
	tmp := t.TempDir()
	writer, err := wal.NewWriter(&wal.WriterConfig{
		WALDir: filepath.Join(tmp, "wal"), SyncMode: wal.SyncModeFsync, MaxSizeBytes: 100 << 20, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	local, err := storage.NewLocalBackend(filepath.Join(tmp, "data"), zerolog.Nop())
	require.NoError(t, err)
	st := &lateLandingStorage{Backend: local, pending: map[string][]byte{}}
	b := newReplayTestBuffer(t, filepath.Join(tmp, "unused"))
	b.storage = st
	b.SetWAL(writer)
	f := &ambiguousFixture{dir: filepath.Join(tmp, "data"), writer: writer, storage: st, buffer: b, clock: time.Now()}
	b.ambiguous.now = func() time.Time { return f.clock }
	t.Cleanup(func() {
		_ = b.Close()
		_ = writer.Close()
		_ = local.Close()
	})
	return f
}

// write sends one msgpack columnar batch of n rows in the current hour.
func (f *ambiguousFixture) write(t *testing.T, n int) {
	t.Helper()
	base := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	times := make([]interface{}, n)
	vals := make([]interface{}, n)
	for i := range times {
		times[i] = base + int64(i)
		vals[i] = float64(i)
	}
	payload, err := msgpack.Marshal(map[string]interface{}{"m": "probe", "columns": map[string]interface{}{"time": times, "v": vals}})
	require.NoError(t, err)
	decoded, err := NewMessagePackDecoder(zerolog.Nop()).Decode(payload)
	require.NoError(t, err)
	require.NoError(t, f.buffer.Write(context.Background(), "db", decoded))
}

// failFlush flushes with the write abandoned client-side; it returns the key.
func (f *ambiguousFixture) failFlush(t *testing.T) string {
	t.Helper()
	f.storage.mu.Lock()
	f.storage.failNext = true
	f.storage.mu.Unlock()
	require.Error(t, f.buffer.FlushAll(context.Background()))
	f.storage.mu.Lock()
	defer f.storage.mu.Unlock()
	require.Len(t, f.storage.pending, 1)
	for p := range f.storage.pending {
		return p
	}
	return ""
}

// replay runs the WAL recovery the periodic pass runs, through the replay
// entry point it uses (cmd/arc createColumnarRecoveryCallback), and flushes.
func (f *ambiguousFixture) replay(t *testing.T) {
	t.Helper()
	f.replayBatch(t, 0)
}

// replayBatch is replay with wal.recovery_batch_size = batch (0: unsplit):
// a row-format entry larger than it replays as row ranges.
func (f *ambiguousFixture) replayBatch(t *testing.T, batch int) {
	t.Helper()
	require.NoError(t, f.writer.Rotate())
	_, err := wal.NewRecovery(filepath.Dir(f.writer.CurrentFile()), zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&wal.RecoveryOptions{
			SkipActiveFile: f.writer.CurrentFile(),
			BatchSize:      batch,
			TrackedRowCallback: func(ctx context.Context, records []map[string]interface{}, id string) error {
				db, m, cols, err := WALRowsToColumns(records)
				if err != nil {
					return err
				}
				return f.buffer.WriteColumnarDirectReplay(ctx, db, m, cols, id)
			},
			ColumnarCallbackWithTagColumns: func(ctx context.Context, db, m string, cols map[string][]interface{}, id string, tags []string) error {
				return f.buffer.WriteColumnarDirectReplayWithTagColumns(ctx, db, m, cols, id, tags)
			},
			BeforeDelete: func(ctx context.Context) error { return f.buffer.FlushAll(ctx) },
		})
	require.NoError(t, err)
	require.NoError(t, f.buffer.FlushAll(context.Background()))
}

func (f *ambiguousFixture) files(t *testing.T) []string {
	t.Helper()
	return parquetPaths(t, f.dir)
}

func (f *ambiguousFixture) sweep(after time.Duration) {
	f.clock = f.clock.Add(after)
	f.buffer.sweepAmbiguousWrites(context.Background())
}

// The issue's sequence: the write times out, lands anyway, recovery replays
// the rows into a new file. The landed copy is removed; the replay stays.
func TestFailedWriteThatLandsIsRemovedOnceReplayStoresItsRows(t *testing.T) {
	f := newAmbiguousFixture(t)
	f.write(t, 50)
	failed := f.failFlush(t)
	require.Equal(t, int64(1), f.buffer.ambiguous.count.Load(), "the failed key is tracked")
	require.ElementsMatch(t, []string{failed}, f.storage.land(t))

	f.sweep(0)
	require.Len(t, f.files(t), 1, "nothing is removed before the rows are stored elsewhere")

	f.replay(t)
	files := f.files(t)
	require.Len(t, files, 2, "landed copy + replay: the duplicate of #1635")

	f.sweep(0)
	files = f.files(t)
	require.Len(t, files, 1)
	require.NotEqual(t, failed, filepath.ToSlash(files[0]), "the replayed file is the one kept")

	// Every scheduled look is taken, then the entry is gone.
	for _, d := range []time.Duration{time.Minute, 4 * time.Minute, 10 * time.Minute} {
		f.sweep(d)
	}
	require.Zero(t, f.buffer.ambiguous.count.Load())
	require.Len(t, f.files(t), 1)
}

// The abandoned PUT completes after the first look: a later look removes it.
func TestFailedWriteThatLandsAfterTheFirstLookIsRemoved(t *testing.T) {
	f := newAmbiguousFixture(t)
	f.write(t, 20)
	f.failFlush(t)
	f.replay(t)
	f.sweep(0) // satisfied, nothing at the key yet
	require.Len(t, f.files(t), 1)
	require.Equal(t, int64(1), f.buffer.ambiguous.count.Load(), "still watched after an empty look")

	f.storage.land(t)
	require.Len(t, f.files(t), 2)
	f.sweep(30 * time.Second)
	require.Len(t, f.files(t), 2, "the next look is not due yet")
	f.sweep(30 * time.Second)
	require.Len(t, f.files(t), 1)
}

// Nothing is deleted while the rows are not stored elsewhere.
func TestFailedWriteIsKeptUntilItsRowsAreStored(t *testing.T) {
	f := newAmbiguousFixture(t)
	f.write(t, 20)
	f.failFlush(t)
	f.storage.land(t)
	f.write(t, 5) // other rows, other identity
	require.NoError(t, f.buffer.FlushAll(context.Background()))
	for _, d := range []time.Duration{0, time.Minute, 5 * time.Minute, 20 * time.Minute} {
		f.sweep(d)
	}
	require.Len(t, f.files(t), 2, "the failed file's rows exist nowhere else")
	require.Equal(t, int64(1), f.buffer.ambiguous.count.Load())

	// Past the age limit the entry is given up, the file kept.
	f.sweep(ambiguousWriteMaxAge)
	require.Zero(t, f.buffer.ambiguous.count.Load())
	require.Len(t, f.files(t), 2)
}

// An object of another size at the key is not ours: never deleted.
func TestFailedWriteKeyHoldingOtherBytesIsNotDeleted(t *testing.T) {
	f := newAmbiguousFixture(t)
	f.write(t, 20)
	failed := f.failFlush(t)
	f.storage.mu.Lock()
	f.storage.pending = map[string][]byte{}
	f.storage.mu.Unlock()
	require.NoError(t, f.storage.Backend.Write(context.Background(), failed, []byte("not the flush's bytes")))
	f.replay(t)
	f.sweep(0)
	require.Len(t, f.files(t), 2)
	require.Zero(t, f.buffer.ambiguous.count.Load())
}

// Without a WAL there is no replay to wait for, and the late copy may be the
// only one: nothing is tracked.
func TestFailedWriteWithoutWALIsNotTracked(t *testing.T) {
	b := newReplayTestBuffer(t, t.TempDir())
	err := &failedStorageWrite{err: errors.New("x"), path: "db/m/f.parquet", size: 1}
	b.noteFailedWrite(err, []string{"0000000000000001000000000000002a"}, true)
	require.Zero(t, b.ambiguous.count.Load())
	// And with a WAL, a flush whose rows cannot all be named is not either.
	b.wal = &checkpointRecordingWAL{}
	b.noteFailedWrite(err, nil, false)
	require.Zero(t, b.ambiguous.count.Load())
}

// writeLine sends n rows the way line protocol does: no raw payload, so the
// WAL holds a row-format entry, which recovery splits by
// wal.recovery_batch_size.
func (f *ambiguousFixture) writeLine(t *testing.T, n int) {
	t.Helper()
	base := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	times := make([]interface{}, n)
	vals := make([]interface{}, n)
	for i := range times {
		times[i] = base + int64(i)
		vals[i] = float64(i)
	}
	require.NoError(t, f.buffer.WriteColumnarDirect(context.Background(), "db", "probe", map[string][]interface{}{"time": times, "v": vals}))
}

// A row-format entry above wal.recovery_batch_size (non-default, small here)
// replays as row ranges, never as the whole entry: the failed key's need has
// to be met by those ranges.
func TestFailedLineProtocolWriteIsRemovedWhenReplayedInRanges(t *testing.T) {
	f := newAmbiguousFixture(t)
	f.writeLine(t, 250)
	f.failFlush(t)
	f.storage.land(t)
	f.replayBatch(t, 100) // 0-100, 100-200, 200-250
	require.Len(t, f.files(t), 2)
	f.sweep(0)
	require.Len(t, f.files(t), 1)
}

// A replay that itself fails leaves a second key; the replay that finally
// stores the rows satisfies both.
func TestTwoFailedWritesOfTheSameRowsAreBothRemoved(t *testing.T) {
	f := newAmbiguousFixture(t)
	f.write(t, 30)
	f.failFlush(t)
	f.storage.land(t)
	require.NoError(t, f.writer.Rotate())
	// The first replay's flush fails as well, and lands.
	f.storage.mu.Lock()
	f.storage.failNext = true
	f.storage.mu.Unlock()
	_, err := wal.NewRecovery(filepath.Dir(f.writer.CurrentFile()), zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&wal.RecoveryOptions{
			SkipActiveFile: f.writer.CurrentFile(),
			ColumnarCallbackWithTagColumns: func(ctx context.Context, db, m string, cols map[string][]interface{}, id string, tags []string) error {
				return f.buffer.WriteColumnarDirectReplayWithTagColumns(ctx, db, m, cols, id, tags)
			},
			BeforeDelete: func(ctx context.Context) error { return f.buffer.FlushAll(ctx) },
		})
	require.Error(t, err, "the barrier failed: the file is kept for the next pass")
	f.storage.land(t)
	require.Equal(t, int64(2), f.buffer.ambiguous.count.Load())
	f.replay(t)
	require.Len(t, f.files(t), 3)
	f.sweep(0)
	require.Len(t, f.files(t), 1)
}

// Storage that does not answer is not "absent": the look is retried, not
// spent, and nothing is dropped.
func TestStatErrorRetriesTheLook(t *testing.T) {
	f := newAmbiguousFixture(t)
	f.write(t, 10)
	f.failFlush(t)
	f.replay(t)
	broken := &statFailingStorage{lateLandingStorage: f.storage}
	f.buffer.storage = broken
	for i := 0; i < 10; i++ {
		f.sweep(20 * time.Minute)
	}
	require.Equal(t, int64(1), f.buffer.ambiguous.count.Load())
	f.storage.land(t)
	f.buffer.storage = f.storage
	f.sweep(0)
	require.Len(t, f.files(t), 1)
}

type statFailingStorage struct{ *lateLandingStorage }

func (s *statFailingStorage) StatFile(context.Context, string) (int64, error) {
	return 0, errors.New("503 SlowDown")
}

// Close sweeps what its own final flushes satisfied.
func TestCloseRemovesWhatItsFinalFlushSatisfied(t *testing.T) {
	f := newAmbiguousFixture(t)
	f.write(t, 10)
	f.failFlush(t)
	f.storage.land(t)
	require.NoError(t, f.writer.Rotate())
	_, err := wal.NewRecovery(filepath.Dir(f.writer.CurrentFile()), zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&wal.RecoveryOptions{
			SkipActiveFile: f.writer.CurrentFile(),
			ColumnarCallbackWithTagColumns: func(ctx context.Context, db, m string, cols map[string][]interface{}, id string, tags []string) error {
				return f.buffer.WriteColumnarDirectReplayWithTagColumns(ctx, db, m, cols, id, tags)
			},
			BeforeDelete: func(context.Context) error { return nil }, // left in the buffer
		})
	require.NoError(t, err)
	require.Len(t, f.files(t), 1)
	require.NoError(t, f.buffer.Close())
	files := f.files(t)
	require.Len(t, files, 1)
}

func TestWALNeedOf(t *testing.T) {
	const p = "00000000000000010000000000000001"
	const q = "00000000000000010000000000000002"
	batch := func(n int, copied bool, ids ...string) *TypedColumnBatch {
		return &TypedColumnBatch{Data: map[string]interface{}{"time": make([]int64, n)}, WALHashes: ids, hasWALCopy: copied}
	}
	need, ok := walNeedOf([]interface{}{batch(5, true, p)})
	require.True(t, ok)
	require.Equal(t, []string{wal.RecoveryRowIdentity(p, 0, 5)}, need)

	need, ok = walNeedOf([]interface{}{batch(3, true, wal.RecoveryRowIdentity(p, 10, 13))})
	require.True(t, ok)
	require.Equal(t, []string{wal.RecoveryRowIdentity(p, 10, 13)}, need)

	need, ok = walNeedOf([]interface{}{batch(9, true, p, q)}) // chunked
	require.True(t, ok)
	require.Equal(t, []string{p, q}, need)

	for name, records := range map[string][]interface{}{
		"no WAL copy":           {batch(5, true, p), batch(5, false, q)},
		"no identity":           {batch(5, true)},
		"content hash":          {batch(5, true, "aa"+p+p[2:])},
		"range of another size": {batch(4, true, wal.RecoveryRowIdentity(p, 10, 13))},
		"chunk with a range":    {batch(5, true, p, wal.RecoveryRowIdentity(q, 0, 2))},
		"not a typed batch":     {map[string]interface{}{"time": []int64{1}}},
		"empty":                 {},
	} {
		_, ok := walNeedOf(records)
		require.False(t, ok, name)
	}
}

func TestAmbiguousLedgerNeeds(t *testing.T) {
	const p = "00000000000000010000000000000001"
	const q = "00000000000000010000000000000002"
	rows := func(id string, s, e int) string { return wal.RecoveryRowIdentity(id, s, e) }

	t.Run("content hash is never recorded", func(t *testing.T) {
		l := newAmbiguousLedger()
		hash := "aa" + p + p[2:] // 64 hex: an untracked entry's content hash
		require.False(t, l.record("f", 1, []string{p, hash}))
		require.False(t, l.record("f", 1, nil))
		require.Zero(t, l.count.Load())
	})

	t.Run("whole needs are met by whole identities", func(t *testing.T) {
		l := newAmbiguousLedger()
		require.True(t, l.record("f", 1, []string{p, q}))
		l.stored([]string{p})
		checks, _ := l.due()
		require.Empty(t, checks)
		l.stored([]string{rows(q, 0, 10)}) // a range never meets a whole need
		checks, _ = l.due()
		require.Empty(t, checks)
		l.stored([]string{q})
		checks, _ = l.due()
		require.Len(t, checks, 1)
	})

	t.Run("range needs are met piecewise", func(t *testing.T) {
		l := newAmbiguousLedger()
		require.True(t, l.record("f", 1, []string{rows(p, 0, 10), rows(p, 20, 30)}))
		l.stored([]string{rows(p, 0, 5), rows(p, 25, 30)})
		checks, _ := l.due()
		require.Empty(t, checks)
		l.stored([]string{rows(p, 5, 25)})
		checks, _ = l.due()
		require.Len(t, checks, 1)
	})

	t.Run("a whole identity meets a range need", func(t *testing.T) {
		l := newAmbiguousLedger()
		require.True(t, l.record("f", 1, []string{rows(p, 3, 7)}))
		l.stored([]string{p})
		checks, _ := l.due()
		require.Len(t, checks, 1)
	})

	t.Run("full ledger refuses", func(t *testing.T) {
		l := newAmbiguousLedger()
		for i := 0; i < maxAmbiguousWrites; i++ {
			require.True(t, l.record(fmt.Sprintf("f%d", i), 1, []string{p}))
		}
		require.False(t, l.record("over", 1, []string{p}))
		l.stored([]string{p})
		checks, _ := l.due()
		require.Len(t, checks, maxAmbiguousWrites)
	})
}

func TestRangeArithmetic(t *testing.T) {
	r := addRange(nil, rowRange{10, 20})
	r = addRange(r, rowRange{0, 5})
	r = addRange(r, rowRange{5, 10}) // touching ranges merge
	require.Equal(t, []rowRange{{0, 20}}, r)
	r = addRange(r, rowRange{30, 40})
	require.Equal(t, []rowRange{{0, 20}, {30, 40}}, r)
	r = subtractRange(r, rowRange{5, 35})
	require.Equal(t, []rowRange{{0, 5}, {35, 40}}, r)
	require.Empty(t, subtractRange(r, rowRange{0, 40}))
}
