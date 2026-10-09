package main

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// The production wiring, not the bare method value: this is the only new code
// path that reads a config key, and a key only ever exercised at its default is
// the shape that shipped #534.
func TestNewCheckpointRecoveredHonorsANonDefaultTimeout(t *testing.T) {
	w, err := wal.NewWriter(&wal.WriterConfig{
		WALDir: t.TempDir(), SyncMode: wal.SyncModeFsync, MaxSizeBytes: 100 * 1024 * 1024,
		BufferSize: 1, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)

	// A real, small, non-default budget. The checkpoint is admitted and
	// written, so the hook succeeds and releases the identity.
	hook := newCheckpointRecovered(w, 2*time.Second)
	require.NoError(t, hook(context.Background(), ids))
	require.Zero(t, w.PendingUnflushedCount())
}

// A non-positive value takes the 30 s fallback every other consumer of
// ingest.flush_timeout_seconds uses, rather than producing an already-expired
// context that would retain every WAL file forever.
func TestNewCheckpointRecoveredFallsBackOnANonPositiveTimeout(t *testing.T) {
	w, err := wal.NewWriter(&wal.WriterConfig{
		WALDir: t.TempDir(), SyncMode: wal.SyncModeFsync, MaxSizeBytes: 100 * 1024 * 1024,
		Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)

	for _, timeout := range []time.Duration{0, -5 * time.Second} {
		require.NoError(t, newCheckpointRecovered(w, timeout)(context.Background(), ids),
			"timeout %s must fall back, not expire immediately", timeout)
	}
}

// A caller whose context is already cancelled must fail, so the file is
// retained for a later pass rather than reported as checkpointed.
func TestNewCheckpointRecoveredRefusesACancelledCaller(t *testing.T) {
	w, err := wal.NewWriter(&wal.WriterConfig{
		WALDir: t.TempDir(), SyncMode: wal.SyncModeFsync, MaxSizeBytes: 100 * 1024 * 1024,
		Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })

	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, newCheckpointRecovered(w, time.Second)(ctx, ids), context.Canceled)
}
