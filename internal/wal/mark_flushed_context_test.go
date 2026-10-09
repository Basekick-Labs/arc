package wal

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Recovery's parent finalization ran on the drop-on-full enqueue that #1009
// moved the forced maintenance rotation OFF — same pass, same queue pressure.
// A dropped checkpoint retains the replayed file for another pass, which is
// safe, but it is one of the ways a file stays retained long enough to matter.
func TestMarkFlushedContextWaitsWhereMarkFlushedDrops(t *testing.T) {
	w, resume := pausedRotationWriter(t)
	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)
	_, err = w.AppendTracked([]map[string]interface{}{{"index": 1}})
	require.NoError(t, err)
	require.Equal(t, cap(w.entryChan), len(w.entryChan), "the queue must be full for this test to mean anything")

	// The ingest flush path keeps dropping: making its admission blocking
	// would stall flushes under exactly this pressure.
	require.ErrorIs(t, w.MarkFlushed(ids), ErrWALDropped)

	result := make(chan error, 1)
	started := make(chan struct{})
	go func() { close(started); result <- w.MarkFlushedContext(context.Background(), ids) }()
	<-started
	select {
	case err := <-result:
		t.Fatalf("MarkFlushedContext returned before the queue could drain: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	resume()
	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("MarkFlushedContext never resumed after the queue drained")
	}
}

// Cancellation returns an error so the caller RETAINS the WAL and replays
// later: at-least-once, never at-most-once.
func TestMarkFlushedContextCancellationIsAnError(t *testing.T) {
	w, _ := pausedRotationWriter(t)
	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)
	_, err = w.AppendTracked([]map[string]interface{}{{"index": 1}})
	require.NoError(t, err)
	require.Equal(t, cap(w.entryChan), len(w.entryChan))

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- w.MarkFlushedContext(ctx, ids) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("a cancelled admission wait never returned")
	}
}

// An already-cancelled context must not admit an entry at all.
func TestMarkFlushedContextRefusesACancelledContext(t *testing.T) {
	w := crashRecoveryWriter(t, t.TempDir())
	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, w.MarkFlushedContext(ctx, ids), context.Canceled)
}

// A closed writer reports failure rather than claiming a durability proof that
// was never written.
func TestMarkFlushedContextOnAClosedWriterFails(t *testing.T) {
	w, err := NewWriter(&WriterConfig{
		WALDir: t.TempDir(), SyncMode: SyncModeFsync, MaxSizeBytes: 100 * 1024 * 1024, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)
	require.NoError(t, w.Close())

	err = w.MarkFlushedContext(context.Background(), ids)
	require.EqualError(t, err, "WAL writer is closed")
}

// No hashes is not a reason to touch the queue.
func TestMarkFlushedContextEmptyIsANoop(t *testing.T) {
	w, _ := pausedRotationWriter(t)
	require.NoError(t, w.MarkFlushedContext(context.Background(), nil))
}

// A deadline that fires AFTER the entry is admitted must not abandon the reply.
// The writer loop writes the checkpoint either way, so returning early would
// skip releasePending: the parent's sequence stays in pendingSeqs,
// MinUnflushedSequence holds the purge floor down, and PurgeFlushed stops at
// the first retained file — so nothing rotated afterwards is ever reclaimed
// again for the life of the process (#676).
//
// It does not self-heal. A later pass SKIPS the entry because its checkpoint is
// durable, so CheckpointRecovered is never called for that parent again; this
// test asserts that too, because the claim that it heals is what made the hole
// look acceptable.
func TestMarkFlushedContextDoesNotLeakAnAdmittedCheckpoint(t *testing.T) {
	w, resume := pausedRotationWriter(t)
	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)
	require.Equal(t, 1, w.PendingUnflushedCount())

	// The queue has capacity, so admission succeeds immediately and the
	// deadline can only fire while waiting for the writer loop's reply.
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- w.MarkFlushedContext(ctx, ids) }()
	time.Sleep(150 * time.Millisecond)
	resume()
	select {
	case err := <-result:
		require.NoError(t, err, "an admitted checkpoint must report the writer's own outcome, not the deadline")
	case <-time.After(5 * time.Second):
		t.Fatal("MarkFlushedContext never returned after the writer loop resumed")
	}

	require.Zero(t, w.PendingUnflushedCount(),
		"the identity was not released, so MinUnflushedSequence pins the purge floor for the life of the process")
	hashes, err := w.CurrentCheckpointHashes()
	require.NoError(t, err)
	require.Contains(t, hashes, ids[0], "the checkpoint was written, which is why abandoning the reply leaks")

	require.NoError(t, w.Rotate())
	deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Positive(t, deleted, "the purge made no progress, so the WAL will grow without bound")
}
