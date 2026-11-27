package ingest

// #1438: with the WAL on and no coverage, a write refused after its WAL append
// (a cancelled request or schema churn under the shard lock, a shard Close
// already swept) stayed in the WAL and was replayed at the next start,
// although the client was told it failed. A queued write now commits its entry
// under the shard lock after those checks; a durable one, which appends before
// the lock, retracts its entry by checkpoint.

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

var walModes = []wal.AckMode{wal.AckModeQueued, wal.AckModeDurable}

// newWALRefusalRig is a buffer over a real WAL in mode; wrap, when set,
// replaces the WAL the buffer sees.
func newWALRefusalRig(t *testing.T, mode wal.AckMode, wrap func(*wal.Writer) WALWriter) (*ArrowBuffer, *wal.Writer, string) {
	t.Helper()
	tmp := t.TempDir()
	dir := filepath.Join(tmp, "wal")
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: dir, SyncMode: wal.SyncModeFsync, AckMode: mode, Logger: zerolog.Nop()})
	require.NoError(t, err)
	buf := newReplayTestBuffer(t, filepath.Join(tmp, "live"))
	if wrap != nil {
		buf.SetWAL(wrap(w))
	} else {
		buf.SetWAL(w)
	}
	return buf, w, dir
}

// replayedWALEntries closes w and counts what a recovery of dir replays.
func replayedWALEntries(t *testing.T, w *wal.Writer, dir string) int {
	t.Helper()
	require.NoError(t, w.Close())
	n := 0
	_, err := wal.NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { n++; return nil },
		&wal.RecoveryOptions{ColumnarCallbackWithTagColumns: func(context.Context, string, string, map[string][]interface{}, string, []string) error {
			n++
			return nil
		}})
	require.NoError(t, err)
	return n
}

func refusalTestColumns() map[string][]interface{} {
	return map[string][]interface{}{"time": {time.Now().UTC().UnixMicro()}, "value": {1.0}}
}

func refusalTestBatch() *TypedColumnBatch {
	return &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{time.Now().UTC().UnixMicro()}, "value": []float64{1}}}
}

func cancelled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

// The control: an accepted, unflushed write is replayed. Without it the
// zero counts below could be a harness that sees nothing.
func TestAcceptedWriteIsInTheWAL(t *testing.T) {
	for _, mode := range walModes {
		t.Run(string(mode), func(t *testing.T) {
			buf, w, dir := newWALRefusalRig(t, mode, nil)
			require.NoError(t, buf.WriteColumnarDirect(context.Background(), "d", "m", refusalTestColumns()))
			require.NoError(t, buf.WriteTypedColumnarDirect(context.Background(), "d", "t", refusalTestBatch(), 1))
			require.Equal(t, 2, replayedWALEntries(t, w, dir))
			_ = buf.Close()
		})
	}
}

// A request cancelled before its write is refused under the shard lock, after
// the point where the entry used to be appended.
func TestWriteRefusedUnderTheLockLeavesNothingInTheWAL(t *testing.T) {
	for _, mode := range walModes {
		t.Run(string(mode), func(t *testing.T) {
			buf, w, dir := newWALRefusalRig(t, mode, nil)
			err := buf.WriteColumnarDirect(cancelled(), "d", "m", refusalTestColumns())
			require.ErrorIs(t, err, context.Canceled)
			err = buf.WriteTypedColumnarDirect(cancelled(), "d", "t", refusalTestBatch(), 1)
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, replayedWALEntries(t, w, dir), "a refused write is in the WAL")
			require.Zero(t, w.RetractionsFailed)
			requireQueuedWroteNothing(t, mode, w)
			_ = buf.Close()
		})
	}
}

// A write that reaches a shard Close already swept is refused, and nothing of
// it is kept: the shutdown purge is not skipped for it, and nothing replays.
func TestWriteRefusedByAClosingBufferLeavesNothingInTheWAL(t *testing.T) {
	for _, mode := range walModes {
		t.Run(string(mode), func(t *testing.T) {
			buf, w, dir := newWALRefusalRig(t, mode, nil)
			require.NoError(t, buf.Close())
			err := buf.WriteColumnarDirect(context.Background(), "d", "m", refusalTestColumns())
			require.True(t, errors.Is(err, ErrBufferClosing), "err = %v", err)
			err = buf.WriteTypedColumnarDirect(context.Background(), "d", "t", refusalTestBatch(), 1)
			require.True(t, errors.Is(err, ErrBufferClosing), "err = %v", err)
			require.True(t, buf.CloseFlushedCleanly(), "a refused write was counted WAL-only")
			require.Zero(t, replayedWALEntries(t, w, dir), "a refused write is in the WAL")
			requireQueuedWroteNothing(t, mode, w)
		})
	}
}

// noRetractWAL is a WAL whose retraction checkpoint cannot be written: it
// releases the entries, as the real writer does when the checkpoint fails.
type noRetractWAL struct{ *wal.Writer }

func (n noRetractWAL) RetractTracked(hashes []string) bool {
	n.ForgetTracked(hashes)
	return false
}

// When a durable write's retraction fails, the old behaviour holds: the entry
// is released, kept WAL-only by a closing buffer, and replayed at the next
// start. The refusal is still answered; the log says the write may come back.
func TestDurableRetractionFailureKeepsTheOldBehaviour(t *testing.T) {
	buf, w, dir := newWALRefusalRig(t, wal.AckModeDurable, func(w *wal.Writer) WALWriter { return noRetractWAL{w} })
	require.NoError(t, buf.Close())
	err := buf.WriteColumnarDirect(context.Background(), "d", "m", refusalTestColumns())
	require.True(t, errors.Is(err, ErrBufferClosing), "err = %v", err)
	require.False(t, buf.CloseFlushedCleanly(), "an unretracted entry must stay WAL-only, so the shutdown purge keeps it")
	require.Equal(t, 1, replayedWALEntries(t, w, dir))
}

// requireQueuedWroteNothing: a queued write's entry is only committed once the
// write can no longer be refused, so a refused one writes no entry at all —
// not even a retraction checkpoint. A durable one appends first and retracts.
func requireQueuedWroteNothing(t *testing.T, mode wal.AckMode, w *wal.Writer) {
	t.Helper()
	if mode == wal.AckModeQueued {
		require.Zero(t, atomic.LoadInt64(&w.TotalEntries), "a refused queued write reached the WAL file")
	}
}

type droppingCommit struct{ tokens []string }

func (d droppingCommit) Commit(uint64) ([]string, error) { return d.tokens, wal.ErrWALDropped }

// A full WAL queue drops a prepared write's later chunks at commit. The batch
// is still buffered; the chunks that reached the queue keep their tokens so
// the flush checkpoints them (unchecked, they would replay at the next start
// and duplicate rows the buffer already holds), and the batch has no complete
// copy.
func TestTrackedCommitDropKeepsTheEnqueuedTokens(t *testing.T) {
	batch := refusalTestBatch()
	err := commitTrackedLocked(droppingCommit{tokens: []string{"00000000000000070000000000000001"}}, batch)
	require.ErrorIs(t, err, wal.ErrWALDropped)
	require.Equal(t, []string{"00000000000000070000000000000001"}, batch.WALHashes)
	require.False(t, batch.hasWALCopy)
}
