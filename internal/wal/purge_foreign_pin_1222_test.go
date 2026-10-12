package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// #1222: every crash replay writes a checkpoint for an earlier instance's
// token into the new process's first file. When the foreign-proof pin lasted
// for the life of the process, PurgeFlushed stopped at that file and reclaimed
// nothing until a clean shutdown.

func pinTestWriter(t *testing.T, dir string) *Writer {
	t.Helper()
	w, err := NewWriter(&WriterConfig{WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 << 20, Logger: zerolog.Nop()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// ingestAndFlush appends one tracked entry to a fresh file and checkpoints it.
func ingestAndFlush(t *testing.T, w *Writer) {
	t.Helper()
	require.NoError(t, w.Rotate())
	ids, err := w.AppendTracked([]map[string]interface{}{{"v": 1}})
	require.NoError(t, err)
	require.NoError(t, w.MarkFlushed(ids))
}

func walFiles(t *testing.T, dir string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	return files
}

func TestPurgeFlushedReclaimsAfterACrashReplay(t *testing.T) {
	// Whole-entry tokens, and row ranges (an entry larger than the recovery
	// batch size), whose checkpoints are foreign proof too.
	for _, batch := range []int{0, 1} {
		t.Run(fmt.Sprintf("batch_size_%d", batch), func(t *testing.T) { crashReplayThenPurge(t, batch) })
	}
}

func crashReplayThenPurge(t *testing.T, batchSize int) {
	dir := t.TempDir()

	// The crashed process: tracked entries, never checkpointed.
	crashed := crashRecoveryWriter(t, dir)
	_, err := crashed.AppendTracked([]map[string]interface{}{{"index": 0}, {"index": 1}})
	require.NoError(t, err)
	require.NoError(t, crashed.Close())

	// The restarted process replays them; the flush checkpoints the inherited
	// identity, as the ingest buffer's replay flush does.
	w := pinTestWriter(t, dir)
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		SkipActiveFile: w.CurrentFile(),
		BatchSize:      batchSize,
		TrackedRowCallback: func(_ context.Context, _ []map[string]interface{}, identity string) error {
			return w.MarkFlushed([]string{identity})
		},
		CheckpointRecovered: func(ctx context.Context, parents []string) error {
			return w.MarkFlushedContext(ctx, parents)
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, stats.RecoveredFiles, "recovery should replay and delete the crashed file")

	for i := 0; i < 3; i++ {
		ingestAndFlush(t, w)
	}
	require.NoError(t, w.Rotate())
	deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Equal(t, 4, deleted, "every flushed file before the active one is reclaimable once the replayed file is gone")
	require.Equal(t, []string{w.CurrentFile()}, walFiles(t, dir))
}

// Quarantine renames the file out of the *.wal glob, the collision form
// included: its entries are no longer replayed, so the proof is moot.
func TestForeignPinLiftsWhenTheEarlierFileIsQuarantined(t *testing.T) {
	for _, suffix := range []string{".failed", ".1700000000000000000.failed"} {
		t.Run(suffix, func(t *testing.T) {
			dir := t.TempDir()
			earlier := filepath.Join(dir, "arc-19700101_000000.000000000.wal")
			require.NoError(t, os.WriteFile(earlier, []byte("ARCW"), 0600))
			w := pinTestWriter(t, dir)
			require.NoError(t, w.MarkFlushed([]string{"ffffffffffffffff0000000000000007"}))
			ingestAndFlush(t, w)
			require.NoError(t, w.Rotate())

			deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
			require.NoError(t, err)
			require.Zero(t, deleted, "pinned while the earlier file can still be replayed")

			require.NoError(t, os.Rename(earlier, earlier+suffix))
			deleted, err = w.PurgeFlushed(w.MinUnflushedSequence())
			require.NoError(t, err)
			require.Equal(t, 2, deleted)
		})
	}
}

// A file this process did not create pins again even if it appears after
// start (an operator restoring a quarantined file): only recovery may decide
// what its entries need.
func TestForeignPinHoldsForAFileThatAppearsLater(t *testing.T) {
	dir := t.TempDir()
	w := pinTestWriter(t, dir)
	require.NoError(t, w.MarkFlushed([]string{"ffffffffffffffff0000000000000007"}))
	require.NoError(t, w.Rotate())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "arc-19700101_000000.000000000.wal"), []byte("ARCW"), 0600))

	deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Zero(t, deleted)
}

// An own file that rotated out empty never reaches fileSeqs, and must not be
// taken for an earlier process's file.
func TestForeignPinIgnoresOwnEmptyFiles(t *testing.T) {
	dir := t.TempDir()
	w := pinTestWriter(t, dir)
	require.NoError(t, w.MarkFlushed([]string{"ffffffffffffffff0000000000000007"}))
	require.NoError(t, w.Rotate())
	require.NoError(t, w.Rotate()) // empty
	empty := w.CurrentFile()
	require.NoError(t, w.Rotate())

	deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Equal(t, 1, deleted, "the proof file is reclaimable: no earlier file exists")
	require.FileExists(t, empty, "never written, so not in the purge order; recovery reclaims it")
}

// A directory listing that fails must keep the pin: it cannot show that no
// earlier file remains.
func TestForeignPinFailsClosedWhenTheListingFails(t *testing.T) {
	dir := t.TempDir()
	w := pinTestWriter(t, dir)
	require.NoError(t, w.MarkFlushed([]string{"ffffffffffffffff0000000000000007"}))
	ingestAndFlush(t, w)
	require.NoError(t, w.Rotate())

	orig := listWALFilesFn
	listWALFilesFn = func(string) ([]string, error) { return nil, errors.New("too many open files") }
	t.Cleanup(func() { listWALFilesFn = orig })
	deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Zero(t, deleted, "an unlisted directory cannot show that no earlier file remains")

	listWALFilesFn = orig
	deleted, err = w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Equal(t, 2, deleted)
}

// The listing sees only regular *.wal files, and reports a missing directory.
func TestListWALFiles(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.wal", "b.wal.failed", "c.wal.recovery", "d.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), nil, 0600))
	}
	require.NoError(t, os.Mkdir(filepath.Join(dir, "e.wal"), 0700))
	got, err := listWALFiles(dir)
	require.NoError(t, err)
	require.Equal(t, []string{filepath.Join(dir, "a.wal")}, got)
	_, err = listWALFiles(filepath.Join(dir, "missing"))
	require.Error(t, err)
}

// The second trigger, with no crash: the flush-failure recovery replays this
// process's own file in row ranges, whose checkpoints are foreign proof. The
// parent's sequence stays pending until recovery checkpoints it, so its file
// stops the walk without any pin; once it is checkpointed and deleted,
// everything is reclaimed.
func TestOwnRowRangeReplayDoesNotPinTheWAL(t *testing.T) {
	dir := t.TempDir()
	w := pinTestWriter(t, dir)
	_, err := w.AppendTracked([]map[string]interface{}{{"index": 0}, {"index": 1}, {"index": 2}, {"index": 3}})
	require.NoError(t, err)
	parentFile := w.CurrentFile()
	require.NoError(t, w.Rotate())

	// Pass 1: the first range lands, the second fails; the file is kept.
	failSecond := true
	var replayed []int
	pass := func() *RecoveryStats {
		stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
			SkipActiveFile: w.CurrentFile(),
			BatchSize:      2,
			TrackedRowCallback: func(_ context.Context, records []map[string]interface{}, identity string) error {
				if _, start, _, ok := ParseRecoveryRowIdentity(identity); ok && start == 2 && failSecond {
					return errors.New("flush failed")
				}
				for _, r := range records {
					replayed = append(replayed, int(toInt64(t, r["index"])))
				}
				return w.MarkFlushed([]string{identity})
			},
			CheckpointRecovered: func(ctx context.Context, parents []string) error {
				return w.MarkFlushedContext(ctx, parents)
			},
			MaxReplayFailures: 10,
		})
		require.NoError(t, err)
		return stats
	}
	pass()
	require.FileExists(t, parentFile)
	proofFile := w.CurrentFile()
	require.NoError(t, w.Rotate())

	_, err = w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.FileExists(t, parentFile, "the parent is still pending")
	require.FileExists(t, proofFile, "the walk stops at the pending parent before the proof")

	// Pass 2: only the uncovered rows replay; the parent is checkpointed and
	// its file deleted.
	failSecond = false
	replayed = nil
	pass()
	require.Equal(t, []int{2, 3}, replayed)
	require.NoFileExists(t, parentFile)
	require.NoError(t, w.Rotate())

	deleted, err := w.PurgeFlushed(w.MinUnflushedSequence())
	require.NoError(t, err)
	require.Positive(t, deleted)
	require.Equal(t, []string{w.CurrentFile()}, walFiles(t, dir), "nothing pins the WAL once the parent is gone")
}
