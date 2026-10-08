package wal

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func testRecoveryCheckpointOnlyBarrier(t *testing.T, rows []map[string]interface{}) {
	t.Helper()
	t.Run("already_checkpointed", func(t *testing.T) {
		dir := t.TempDir()
		w := crashRecoveryWriter(t, dir)
		var ids, paths []string
		for i := 0; i < 3; i++ {
			entryIDs, err := w.AppendTracked(rows)
			require.NoError(t, err)
			ids = append(ids, entryIDs...)
			paths = append(paths, w.CurrentFile())
			require.NoError(t, w.Rotate())
		}
		paths = append(paths, w.CurrentFile())
		require.NoError(t, w.MarkFlushed(ids))
		require.NoError(t, w.Close())
		barriers := 0
		stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
			TrackedRowCallback: func(context.Context, []map[string]interface{}, string) error {
				t.Fatal("checkpointed data replayed")
				return nil
			},
			BeforeDelete: func(context.Context) error { barriers++; return errors.New("unrelated storage outage") },
		})
		require.NoError(t, err)
		require.Zero(t, barriers, "durable files must not require another flush")
		require.Zero(t, stats.RecoveredEntries)
		require.Zero(t, stats.KeptFiles)
		require.Equal(t, len(paths), stats.RecoveredFiles)
		for _, path := range paths {
			require.NoFileExists(t, path)
		}
	})

	t.Run("earlier_replay_still_needs_barrier", func(t *testing.T) {
		dir := t.TempDir()
		w := crashRecoveryWriter(t, dir)
		pending, err := w.AppendTracked(rows)
		require.NoError(t, err)
		paths := []string{w.CurrentFile()}
		require.NoError(t, w.Rotate())
		covered, err := w.AppendTracked(rows)
		require.NoError(t, err)
		paths = append(paths, w.CurrentFile())
		require.NoError(t, w.Rotate())
		paths = append(paths, w.CurrentFile())
		require.NoError(t, w.MarkFlushed(covered))
		require.NoError(t, w.Close())
		outage := errors.New("replayed rows not durable")
		for _, fail := range []bool{true, false} {
			barriers, replayed := 0, 0
			stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
				BarrierBatchFiles: 16,
				TrackedRowCallback: func(_ context.Context, batch []map[string]interface{}, id string) error {
					require.Equal(t, pending[0], id)
					replayed += len(batch)
					return nil
				},
				BeforeDelete: func(context.Context) error {
					barriers++
					require.Equal(t, 1, barriers, "redundant barrier after pending replay was already fenced")
					for _, path := range paths {
						require.FileExists(t, path, "deleted before barrier")
					}
					if fail {
						return outage
					}
					return nil
				},
			})
			require.Equal(t, len(rows), replayed)
			require.Equal(t, 1, barriers)
			if fail {
				require.ErrorIs(t, err, outage)
				require.Equal(t, len(paths), stats.KeptFiles)
				for _, path := range paths {
					require.FileExists(t, path)
				}
			} else {
				require.NoError(t, err)
				require.Zero(t, stats.KeptFiles)
				for _, path := range paths {
					require.NoFileExists(t, path)
				}
			}
		}
	})

	t.Run("covered_ranges_still_finalize_parent", func(t *testing.T) {
		dir := t.TempDir()
		w := crashRecoveryWriter(t, dir)
		ids, err := w.AppendTracked(rows)
		require.NoError(t, err)
		path := w.CurrentFile()
		require.NoError(t, w.Rotate())
		require.NoError(t, w.MarkFlushed([]string{recoveryRowIdentity(ids[0], 0, len(rows))}))
		proof, err := w.CurrentCheckpointHashes()
		require.NoError(t, err)
		outage := errors.New("parent checkpoint failed")
		for _, fail := range []bool{true, false} {
			finalized := 0
			stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
				SkipActiveFile: w.CurrentFile(), AdditionalCheckpointHashes: proof,
				TrackedRowCallback: func(context.Context, []map[string]interface{}, string) error {
					t.Fatal("covered range replayed")
					return nil
				},
				BeforeDelete: func(context.Context) error { t.Fatal("covered ranges triggered a flush"); return nil },
				CheckpointRecovered: func(parents []string) error {
					finalized++
					require.Equal(t, ids, parents)
					require.FileExists(t, path)
					if fail {
						return outage
					}
					return w.MarkFlushed(parents)
				},
			})
			require.Equal(t, 1, finalized)
			require.Zero(t, stats.RecoveredEntries)
			if fail {
				require.ErrorIs(t, err, outage)
				require.FileExists(t, path)
				require.Equal(t, 1, w.PendingUnflushedCount())
			} else {
				require.NoError(t, err)
				require.NoFileExists(t, path)
				require.Zero(t, w.PendingUnflushedCount())
			}
		}
	})
}

func TestRecoveryCheckpointOnlyBarrier(t *testing.T) {
	testRecoveryCheckpointOnlyBarrier(t, []map[string]interface{}{{"index": 0}, {"index": 1}})
}
