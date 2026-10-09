package main

import (
	"context"
	"time"

	"github.com/basekick-labs/arc/internal/wal"
)

// newCheckpointRecovered builds the RecoveryOptions.CheckpointRecovered hook.
//
// It carries its own deadline because the startup recovery pass runs on
// context.Background(): threading only the pass context would bound nothing.
// The budget comes from ingest.flush_timeout_seconds, matching the forced
// maintenance rotation, and a non-positive value falls back to 30 s the way
// every other consumer of that key does.
//
// The deadline bounds ADMISSION to the WAL queue. Once an entry is admitted the
// checkpoint is written and MarkFlushedContext waits for the reply regardless,
// because abandoning it would skip the pending-identity release and pin the
// purge floor for the life of the process.
func newCheckpointRecovered(w *wal.Writer, timeout time.Duration) func(context.Context, []string) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return func(ctx context.Context, parents []string) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return w.MarkFlushedContext(ctx, parents)
	}
}
