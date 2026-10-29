package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #676 part D: a failed entry write. The entry is lost — the WAL cannot hold
// it — but nothing else may be: no torn bytes may stay between whole entries,
// a full disk must not churn a rotation per entry, and every caller waiting on
// the entry must be answered.

// writeFaults injects failures into the writer's file writes and truncates.
// Header writes (rotation) and entry writes are told apart by size: a header
// is exactly WALFileHeaderSize bytes and every entry is longer.
type writeFaults struct {
	mu sync.Mutex
	// dataFails entry writes fail with dataErr, after writing half the entry
	// when partial is set.
	dataFails int
	dataErr   error
	partial   bool
	// headerErr fails every header write while set.
	headerErr error
	// truncateFails truncates fail with EIO.
	truncateFails int

	headerAttempts atomic.Int64
	truncates      atomic.Int64
}

func installWriteFaults(t *testing.T) *writeFaults {
	t.Helper()
	wf := &writeFaults{}
	prevWrite, prevTruncate := writeFileFn, truncateFileFn
	writeFileFn = func(f *os.File, b []byte) (int, error) {
		wf.mu.Lock()
		if len(b) == WALFileHeaderSize {
			err := wf.headerErr
			wf.mu.Unlock()
			wf.headerAttempts.Add(1)
			if err != nil {
				return 0, err
			}
			return f.Write(b)
		}
		if wf.dataFails > 0 {
			wf.dataFails--
			err, partial := wf.dataErr, wf.partial
			wf.mu.Unlock()
			if partial {
				n, werr := f.Write(b[:len(b)/2])
				if werr != nil {
					return n, werr
				}
				return n, err
			}
			return 0, err
		}
		wf.mu.Unlock()
		return f.Write(b)
	}
	truncateFileFn = func(f *os.File, size int64) error {
		wf.truncates.Add(1)
		wf.mu.Lock()
		fail := wf.truncateFails > 0
		if fail {
			wf.truncateFails--
		}
		wf.mu.Unlock()
		if fail {
			return syscall.EIO
		}
		return f.Truncate(size)
	}
	// Registered before the writer's Close cleanup, so it runs after it.
	t.Cleanup(func() { writeFileFn, truncateFileFn = prevWrite, prevTruncate })
	return wf
}

func (wf *writeFaults) set(fn func(wf *writeFaults)) {
	wf.mu.Lock()
	defer wf.mu.Unlock()
	fn(wf)
}

func faultTestWriter(t *testing.T, maxSize int64) *Writer {
	t.Helper()
	if maxSize == 0 {
		maxSize = 1 << 40
	}
	w, err := NewWriter(&WriterConfig{
		WALDir: t.TempDir(), SyncMode: SyncModeFdatasync, BufferSize: 1024,
		MaxSizeBytes: maxSize, MaxAge: time.Hour, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// entriesDone is how many entries the writer loop has finished with: each
// one ends written, lost, or dropped before the queue.
func entriesDone(w *Writer) int64 {
	return atomic.LoadInt64(&w.TotalEntries) + atomic.LoadInt64(&w.FailedWrites) + atomic.LoadInt64(&w.DroppedEntries)
}

// waitEntriesDone waits until the writer loop has finished with n entries
// after before. Waiting for the queue to drain was not enough: the entry
// being written (a failed write, a rotation, the retry) could still be in
// flight under load.
func waitEntriesDone(t *testing.T, w *Writer, before, n int64) {
	t.Helper()
	require.Eventually(t, func() bool { return entriesDone(w) >= before+n }, 10*time.Second, time.Millisecond, "writer loop did not finish the entries")
}

// appendOne appends one tracked entry and waits until the writer loop has
// processed it.
func appendOne(t *testing.T, w *Writer, v int) string {
	t.Helper()
	before := entriesDone(w)
	ids, err := w.AppendTracked([]map[string]interface{}{{"m": "t", "v": v}})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	waitEntriesDone(t, w, before, 1)
	return ids[0]
}

// appendBurst appends n tracked entries without waiting between them (well
// inside one rotation backoff), then waits for the writer loop.
func appendBurst(t *testing.T, w *Writer, n int) {
	t.Helper()
	before := entriesDone(w)
	for i := 0; i < n; i++ {
		_, err := w.AppendTracked([]map[string]interface{}{{"m": "t", "v": i}})
		require.NoError(t, err)
	}
	waitEntriesDone(t, w, before, int64(n))
}

// readIdentities reads every *.wal file in dir in name order and returns the
// tracked identities found, failing on any corrupted or torn entry unless
// allowTorn names the file allowed to end in one.
func readIdentities(t *testing.T, dir string, allowTorn string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	sort.Strings(files)
	var ids []string
	for _, f := range files {
		r := NewReader(f, zerolog.Nop())
		entries, err := r.ReadAll()
		require.NoError(t, err)
		require.Zero(t, r.CorruptedEntries, "%s: an entry was read as corrupt", filepath.Base(f))
		if f != allowTorn {
			require.Zero(t, r.TruncatedEntries, "%s: torn bytes left in the file", filepath.Base(f))
		}
		for _, e := range entries {
			if len(e.CheckpointHashes) == 0 {
				ids = append(ids, e.PayloadHash)
			}
		}
	}
	return ids
}

// A write that fails part-way on a full disk loses its entry, and the next
// entries land in the same file right after the last whole one: the torn
// bytes are cut away, so replay reads every later entry and none as corrupt.
// Without the truncate the next entry is read at the torn entry's offset.
func TestFailedWriteRestoresTheTailSoLaterEntriesReplay(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	wf.headerAttempts.Store(0) // the initial file\'s header
	rotations := atomic.LoadInt64(&w.TotalRotations)

	var want []string
	for i := 0; i < 3; i++ {
		want = append(want, appendOne(t, w, i))
	}
	wf.set(func(wf *writeFaults) { wf.dataFails, wf.dataErr, wf.partial = 1, syscall.ENOSPC, true })
	lost := appendOne(t, w, 99)

	require.EqualValues(t, 1, atomic.LoadInt64(&w.FailedWrites))
	require.EqualValues(t, 1, atomic.LoadInt64(&w.DiskFullWrites))
	require.True(t, w.DiskFullObserved(), "an ENOSPC write must raise the disk-full flag")
	require.Equal(t, rotations, atomic.LoadInt64(&w.TotalRotations), "a full disk is not escaped by a rotation")
	require.Zero(t, wf.headerAttempts.Load())

	for i := 3; i < 6; i++ {
		want = append(want, appendOne(t, w, i))
	}
	require.False(t, w.DiskFullObserved(), "a successful write clears the disk-full flag")
	require.Equal(t, want, readIdentities(t, w.config.WALDir, ""))

	// The lost entry left the purge floor when it was lost: it is in no
	// file, so nothing would ever checkpoint it if its flush failed. The
	// file goes once the entries that ARE in it are flushed.
	lostSeq, ok := w.trackedTokenSeq(lost)
	require.True(t, ok)
	require.NotEqual(t, lostSeq, w.MinUnflushedSequence())
	active := w.CurrentFile()
	require.NoError(t, w.MarkFlushed(want))
	require.NoError(t, w.Rotate())
	deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Equal(t, 1, deleted)
	_, err = os.Stat(active)
	require.True(t, os.IsNotExist(err))
}

// A torn tail that cannot be truncated is never appended to: while neither
// the truncate nor a rotation succeeds, entries are dropped; once the
// truncate works, writing resumes after the last whole entry.
func TestTornTailThatCannotBeTruncatedIsNeverAppendedTo(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	wf.headerAttempts.Store(0) // the initial file\'s header

	want := []string{appendOne(t, w, 0)}
	wf.set(func(wf *writeFaults) {
		wf.dataFails, wf.dataErr, wf.partial = 1, syscall.ENOSPC, true
		wf.truncateFails = 1 << 30
		wf.headerErr = syscall.ENOSPC
	})
	appendOne(t, w, 1) // torn, untruncatable; the rotation fails
	appendOne(t, w, 2) // the tail is still torn: dropped, not appended
	appendOne(t, w, 3)
	require.EqualValues(t, 3, atomic.LoadInt64(&w.FailedWrites))
	require.EqualValues(t, 1, wf.headerAttempts.Load(), "the failed rotation is retried at most once a second")

	wf.set(func(wf *writeFaults) { wf.truncateFails = 0 })
	want = append(want, appendOne(t, w, 4))
	require.Equal(t, want, readIdentities(t, w.config.WALDir, ""))
}

// When the truncate fails but a rotation works, the entry is retried on the
// new file and kept, and it is attributed to the file it landed in (#1009).
// The old file ends in the torn bytes, which the reader stops at.
func TestUntruncatableTornWriteRotatesAndRetries(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	wf.headerAttempts.Store(0) // the initial file\'s header

	first := appendOne(t, w, 0)
	old := w.CurrentFile()
	wf.set(func(wf *writeFaults) {
		wf.dataFails, wf.dataErr, wf.partial = 1, syscall.ENOSPC, true
		// A second failure would hit the NEW file only if the old file's
		// torn tail were still taken to be its: the rotation leaves it behind.
		wf.truncateFails = 2
	})
	kept := appendOne(t, w, 1)
	require.Zero(t, atomic.LoadInt64(&w.FailedWrites), "the retry on the new file succeeded")
	require.EqualValues(t, 1, wf.truncates.Load(), "the new file is not truncated before its first write")
	require.NotEqual(t, old, w.CurrentFile())

	seq, _ := w.trackedTokenSeq(kept)
	cur := w.CurrentFile()
	w.mu.Lock()
	state := w.fileSeqs[cur]
	w.mu.Unlock()
	require.NotNil(t, state)
	require.Equal(t, seq, state.maxSeq, "the entry is recorded against the file it landed in")

	r := NewReader(old, zerolog.Nop())
	_, err := r.ReadAll()
	require.NoError(t, err)
	require.EqualValues(t, 1, r.TruncatedEntries, "the old file ends in the torn write")
	require.Equal(t, []string{first, kept}, readIdentities(t, w.config.WALDir, old))
}

// An error that is not a full disk rotates (the handle may be bad) and
// retries, but at most once a second, even when every rotation succeeds and
// the error follows the new file.
func TestFailingWritesRotateAtMostOnceASecond(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	wf.headerAttempts.Store(0) // the initial file\'s header
	rotations := atomic.LoadInt64(&w.TotalRotations)

	wf.set(func(wf *writeFaults) { wf.dataFails, wf.dataErr = 1<<30, syscall.EIO })
	appendBurst(t, w, 50)
	require.EqualValues(t, 50, atomic.LoadInt64(&w.FailedWrites))
	require.Zero(t, atomic.LoadInt64(&w.DiskFullWrites))
	require.False(t, w.DiskFullObserved())
	require.EqualValues(t, 1, atomic.LoadInt64(&w.TotalRotations)-rotations, "one rotation, then backoff")
}

// A full disk with a size-triggered rotation due: each attempt creates a file
// and removes it again, so it is retried at most once a second, not after
// every entry.
func TestSizeRotationOnAFullDiskBacksOff(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 1) // every entry crosses the size bound
	wf.headerAttempts.Store(0) // the initial file\'s header
	wf.set(func(wf *writeFaults) { wf.headerErr = syscall.ENOSPC })
	appendBurst(t, w, 50)
	require.EqualValues(t, 1, wf.headerAttempts.Load())
	files, err := filepath.Glob(filepath.Join(w.config.WALDir, "*.wal"))
	require.NoError(t, err)
	require.Len(t, files, 1, "the failed rotations left no file behind")
}

// A dropped checkpoint and a failed rotate command are answered with the
// error: markFlushed and RotateContext block on that answer.
func TestDroppedCheckpointAndRotateAreAnswered(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	wf.headerAttempts.Store(0) // the initial file\'s header
	tok := appendOne(t, w, 0)

	wf.set(func(wf *writeFaults) {
		wf.dataFails, wf.dataErr = 1<<30, syscall.ENOSPC
		wf.headerErr = syscall.ENOSPC
	})
	answered := make(chan error, 2)
	go func() { answered <- w.MarkFlushed([]string{tok}) }()
	select {
	case err := <-answered:
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ENOSPC), "got %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("a dropped checkpoint was never answered")
	}
	seq, _ := w.trackedTokenSeq(tok)
	require.Equal(t, seq, w.MinUnflushedSequence(), "a failed checkpoint releases nothing on its own; the caller's ForgetTracked does")

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		answered <- w.RotateContext(ctx)
	}()
	select {
	case err := <-answered:
		require.Error(t, err)
		require.True(t, errors.Is(err, syscall.ENOSPC), "got %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("a failed rotate command was never answered")
	}
	require.True(t, w.DiskFullObserved(), "a rotation failing with ENOSPC raises the flag")
	w.ForgetTracked([]string{tok})
	require.Greater(t, w.MinUnflushedSequence(), seq)
}

// A rotation that fails on a full disk raises the disk-full flag on its own,
// with no failed entry write.
func TestRotationOnAFullDiskRaisesTheFlag(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	appendOne(t, w, 0)
	require.False(t, w.DiskFullObserved())
	wf.set(func(wf *writeFaults) { wf.headerErr = syscall.ENOSPC })
	require.Error(t, w.Rotate())
	require.True(t, w.DiskFullObserved())
	require.Zero(t, atomic.LoadInt64(&w.FailedWrites))
}

// #676 H1: a lost entry whose flush then fails is never checkpointed —
// recovery replays only what is in the WAL. It must not hold the purge floor:
// with the rest recovered, every later flushed file is reclaimed.
func TestALostEntryNeverFlushedDoesNotPinThePurge(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)

	appendOne(t, w, 0)
	wf.set(func(wf *writeFaults) { wf.dataFails, wf.dataErr = 1, syscall.ENOSPC })
	drainPurgeHint(w)
	appendOne(t, w, 1) // lost; its buffer's flush fails, so nothing checkpoints it
	requirePurgeHint(t, w, "releasing a lost entry from the floor")
	appendOne(t, w, 2)
	require.NoError(t, w.Rotate())

	// The flush failed: periodic recovery replays the rotated file and
	// checkpoints what it holds.
	stats, err := NewRecovery(w.config.WALDir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		SkipActiveFile: w.CurrentFile(),
		TrackedRowCallback: func(_ context.Context, _ []map[string]interface{}, identity string) error {
			return w.MarkFlushed([]string{identity})
		},
		CheckpointRecovered: func(ctx context.Context, parents []string) error {
			return w.MarkFlushedContext(ctx, parents)
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.RecoveredFiles)

	later := w.CurrentFile()
	require.NoError(t, w.MarkFlushed([]string{appendOne(t, w, 3)}))
	require.NoError(t, w.Rotate())
	_, err = w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	_, err = os.Stat(later)
	require.True(t, os.IsNotExist(err), "the lost entry pinned the purge floor")
}

// A covered entry lost to a failed write is reported by CoverageLost, so a
// failed flush settles its batch rather than waiting for a replay.
func TestCoverageLostReportsALostCoveredEntry(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	commit := func(cseq uint64) {
		p, err := w.PrepareCovered("db", "db/cpu", []byte{0x90})
		require.NoError(t, err)
		_, err = p.Commit(cseq)
		require.NoError(t, err)
		waitForPurgeTestDrain(t, w)
	}
	commit(1)
	wf.set(func(wf *writeFaults) { wf.dataFails, wf.dataErr = 1, syscall.ENOSPC })
	commit(2)
	commit(3)
	require.False(t, w.CoverageLost("db/cpu", 1))
	require.True(t, w.CoverageLost("db/cpu", 2))
	require.False(t, w.CoverageLost("db/cpu", 3))
	require.False(t, w.CoverageLost("db/mem", 2))
}

// #676 L2: while a torn tail cannot be truncated, the entries dropped behind
// it keep the cause of the tear: a full disk is counted and logged as one.
func TestEntriesDroppedBehindATornTailKeepTheDiskFullCause(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	appendOne(t, w, 0)
	wf.set(func(wf *writeFaults) {
		wf.dataFails, wf.dataErr, wf.partial = 1, syscall.ENOSPC, true
		wf.truncateFails = 1 << 30
		wf.headerErr = syscall.ENOSPC
	})
	before := metrics.Get().Snapshot()["wal_disk_full_writes"].(int64)
	appendBurst(t, w, 3)
	require.EqualValues(t, 3, atomic.LoadInt64(&w.FailedWrites))
	require.EqualValues(t, 3, atomic.LoadInt64(&w.DiskFullWrites))
	require.EqualValues(t, before+3, metrics.Get().Snapshot()["wal_disk_full_writes"].(int64))
}

// #676 M1: a failed write the retry on a new file recovered is counted.
func TestARecoveredWriteIsCounted(t *testing.T) {
	wf := installWriteFaults(t)
	w := faultTestWriter(t, 0)
	wf.set(func(wf *writeFaults) { wf.dataFails, wf.dataErr = 1, syscall.EIO })
	before := metrics.Get().Snapshot()["wal_recovered_writes"].(int64)
	appendOne(t, w, 0)
	require.Zero(t, atomic.LoadInt64(&w.FailedWrites))
	require.EqualValues(t, 1, atomic.LoadInt64(&w.RecoveredWrites))
	require.EqualValues(t, before+1, metrics.Get().Snapshot()["wal_recovered_writes"].(int64))
}
