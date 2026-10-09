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

// The callback bound must also apply to the tracked path used by startup and
// periodic recovery, including the last, shorter batch.
func testTrackedRecoveryBatchBound(t *testing.T, rows []map[string]interface{}, limit int) {
	t.Helper()
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	ids, err := w.AppendTracked(rows)
	require.NoError(t, err)
	require.Len(t, ids, 1)
	require.Len(t, ids[0], 32)
	require.NoError(t, w.Close())
	var sizes []int
	total := 0
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		BatchSize: limit,
		TrackedRowCallback: func(_ context.Context, batch []map[string]interface{}, identity string) error {
			sizes = append(sizes, len(batch))
			total += len(batch)
			require.LessOrEqual(t, len(batch), limit, "tracked recovery ignored BatchSize")
			require.NotEmpty(t, identity)
			require.NotEqual(t, ids[0], identity, "a partial batch must not checkpoint its parent")
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, len(rows), total)
	require.Len(t, sizes, (len(rows)+limit-1)/limit)
	require.Equal(t, total, stats.RecoveredEntries)
	require.Equal(t, len(sizes), stats.RecoveredBatches)
}

func TestRecoveryRowRangesResumeAcrossBatchSize(t *testing.T) {
	for _, restart := range []string{"in_process", "restart", "quarantined_checkpoint"} {
		for _, limit := range []int{1, 2, 4, 0} {
			t.Run(fmt.Sprintf("%s/limit=%d", restart, limit), func(t *testing.T) {
				dir := t.TempDir()
				w := crashRecoveryWriter(t, dir)
				rows := make([]map[string]interface{}, 7)
				for i := range rows {
					rows[i] = map[string]interface{}{"index": i}
				}
				ids, err := w.AppendTracked(rows)
				require.NoError(t, err)
				path := w.CurrentFile()
				require.NoError(t, w.Rotate())
				seen := make(map[string]int)
				calls := 0
				stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
					SkipActiveFile: w.CurrentFile(), BatchSize: 3,
					TrackedRowCallback: func(_ context.Context, batch []map[string]interface{}, id string) error {
						calls++
						if calls == 2 {
							return errors.New("reject remaining batch")
						}
						for _, row := range batch {
							seen[fmt.Sprint(row["index"])]++
						}
						require.NoError(t, w.MarkFlushed([]string{id}))
						return nil
					},
					CheckpointRecovered: func(context.Context, []string) error { t.Fatal("partial entry finalized"); return nil },
				})
				require.NoError(t, err)
				require.Equal(t, 2, stats.KeptFiles, "retained source plus unvisited active file")
				require.Equal(t, 1, w.PendingUnflushedCount(), "partial range must keep parent floor pinned")
				require.FileExists(t, path)
				if restart != "in_process" {
					require.NoError(t, w.Close())
					if restart == "quarantined_checkpoint" {
						require.NoError(t, os.Rename(w.CurrentFile(), w.CurrentFile()+".failed"))
					}
					w = crashRecoveryWriter(t, dir)
				}
				checkpoints, err := w.CurrentCheckpointHashes()
				require.NoError(t, err)
				finalized := 0
				stats, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
					SkipActiveFile: w.CurrentFile(), AdditionalCheckpointHashes: checkpoints, BatchSize: limit,
					TrackedRowCallback: func(_ context.Context, batch []map[string]interface{}, id string) error {
						if limit > 0 {
							require.LessOrEqual(t, len(batch), limit)
						}
						parent, start, end, ok := ParseRecoveryRowIdentity(id)
						require.True(t, ok)
						require.Equal(t, ids[0], parent)
						require.GreaterOrEqual(t, start, 3)
						require.Equal(t, len(batch), end-start)
						for _, row := range batch {
							seen[fmt.Sprint(row["index"])]++
						}
						return w.MarkFlushed([]string{id})
					},
					BeforeDelete: func(context.Context) error { require.Len(t, seen, 7); return nil },
					CheckpointRecovered: func(_ context.Context, parents []string) error {
						finalized++
						require.Equal(t, ids, parents)
						return w.MarkFlushed(parents)
					},
				})
				require.NoError(t, err)
				require.Equal(t, 4, stats.RecoveredEntries)
				require.Equal(t, 1, finalized)
				require.Equal(t, 0, w.PendingUnflushedCount())
				_, err = os.Stat(path)
				require.True(t, os.IsNotExist(err))
				require.Len(t, seen, 7)
				for i := 0; i < 7; i++ {
					require.Equal(t, 1, seen[fmt.Sprint(i)], "row %d", i)
				}
			})
		}
	}
}

func TestRecoveryRowRangeFinalizationFailure(t *testing.T) {
	for _, stage := range []string{"barrier", "checkpoint"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			w := crashRecoveryWriter(t, dir)
			ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}, {"index": 1}})
			require.NoError(t, err)
			path := w.CurrentFile()
			require.NoError(t, w.Rotate())
			opts := &RecoveryOptions{
				SkipActiveFile: w.CurrentFile(), BatchSize: 1,
				TrackedRowCallback: func(_ context.Context, _ []map[string]interface{}, id string) error {
					return w.MarkFlushed([]string{id})
				},
				BeforeDelete: func(context.Context) error {
					if stage == "barrier" {
						return errors.New("storage unavailable")
					}
					return nil
				},
				CheckpointRecovered: func(context.Context, []string) error {
					require.Equal(t, "checkpoint", stage, "finalized before successful barrier")
					return errors.New("checkpoint unavailable")
				},
			}
			stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, opts)
			require.Error(t, err)
			require.Equal(t, 1, stats.BarrierFailures)
			require.Equal(t, 1, stats.KeptFiles)
			require.FileExists(t, path)
			require.Equal(t, 1, w.PendingUnflushedCount())
			opts.AdditionalCheckpointHashes, err = w.CurrentCheckpointHashes()
			require.NoError(t, err)
			opts.TrackedRowCallback = func(context.Context, []map[string]interface{}, string) error {
				t.Fatal("durable ranges replayed")
				return nil
			}
			opts.BeforeDelete = func(context.Context) error { return nil }
			opts.CheckpointRecovered = func(_ context.Context, parents []string) error {
				require.Equal(t, ids, parents)
				return w.MarkFlushed(parents)
			}
			stats, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, opts)
			require.NoError(t, err)
			require.Zero(t, stats.RecoveredEntries)
			require.Equal(t, 1, stats.RecoveredFiles)
			require.Zero(t, w.PendingUnflushedCount())
		})
	}
}

func TestRecoveryRowRangesLegacyIdenticalEntries(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	rows := []map[string]interface{}{{"index": 0}, {"index": 1}, {"index": 2}}
	require.NoError(t, w.Append(rows))
	require.NoError(t, w.Append(rows))
	require.NoError(t, w.Close())
	total := 0
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		BatchSize: 2,
		TrackedRowCallback: func(_ context.Context, batch []map[string]interface{}, id string) error {
			require.Empty(t, id, "content hash must never deduplicate legitimate identical writes")
			require.LessOrEqual(t, len(batch), 2)
			total += len(batch)
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, 6, total)
	require.Equal(t, 4, stats.RecoveredBatches)
}

func TestRecoveryRowRangeEncodingAndCoverage(t *testing.T) {
	parent := "0123456789abcdef0123456789abcdef"
	for _, suffix := range []string{"-1:2", "0:0", "3:2", "00:2", "0:+2", "0:999999999999999999999999", "0:2:3"} {
		_, _, _, ok := ParseRecoveryRowIdentity(recoveryRowPrefix + parent + ":" + suffix)
		require.False(t, ok, suffix)
	}
	_, _, _, ok := ParseRecoveryRowIdentity(recoveryRowPrefix + parent + parent + ":0:2")
	require.False(t, ok, "legacy 64-character hash")
	proof := make(map[string]struct{})
	for _, span := range []recoveryRowRange{{3, 5}, {0, 2}, {1, 3}, {7, 8}} {
		proof[recoveryRowIdentity(parent, span.start, span.end)] = struct{}{}
	}
	covered := recoveryRowCoverage(proof)[parent]
	require.Equal(t, []recoveryRowRange{{0, 5}, {7, 8}}, covered)
	missing, err := uncoveredRecoveryRows(9, covered)
	require.NoError(t, err)
	require.Equal(t, []recoveryRowRange{{5, 7}, {8, 9}}, missing)
	_, err = uncoveredRecoveryRows(7, covered)
	require.Error(t, err)
}

func TestRecoveryRowBatchCancellationKeepsWAL(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	_, err := w.AppendTracked([]map[string]interface{}{{"index": 0}, {"index": 1}})
	require.NoError(t, err)
	path := w.CurrentFile()
	require.NoError(t, w.Close())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(ctx, nil, &RecoveryOptions{
		BatchSize:           1,
		TrackedRowCallback:  func(context.Context, []map[string]interface{}, string) error { calls++; cancel(); return nil },
		CheckpointRecovered: func(context.Context, []string) error { t.Fatal("cancelled entry finalized"); return nil },
	})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, stats.KeptFiles)
	require.FileExists(t, path)
	_, err = os.Stat(path + ".recovery")
	require.True(t, os.IsNotExist(err), "cancellation must not count as a poison strike")
}

// A checkpoint range that ends past its entry's row count is a POISON ENTRY,
// and takes the strike path. It used to abort the whole pass with
// `return stats, err` and record no strike, so MaxReplayFailures never
// advanced, the file was never quarantined, and every later file was left
// unread — permanently, with no operator exit.
func TestRecoveryRowCheckpointOutOfBoundsKeepsWAL(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	ids, err := w.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)
	path := w.CurrentFile()
	require.NoError(t, w.MarkFlushed([]string{recoveryRowIdentity(ids[0], 0, 2)}))
	require.NoError(t, w.Close())
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		TrackedRowCallback: func(context.Context, []map[string]interface{}, string) error {
			t.Fatal("invalid coverage accepted")
			return nil
		},
	})
	require.NoError(t, err, "a poison entry must not abandon the pass")
	require.Equal(t, 1, stats.KeptFiles)
	require.Zero(t, stats.QuarantinedFiles)
	require.FileExists(t, path)
	require.FileExists(t, path+replayAttemptsSuffix,
		"the strike must be recorded, or the quarantine threshold never advances")
}

func TestTrackedRecoveryBatchBound(t *testing.T) {
	rows := make([]map[string]interface{}, 7)
	for i := range rows {
		rows[i] = map[string]interface{}{"index": i}
	}
	testTrackedRecoveryBatchBound(t, rows, 3)
}

// The point of taking the strike path rather than returning: MaxReplayFailures
// now advances, so the poison file is quarantined and the operator has an exit.
// Before this, the pass returned with no strike recorded, the condition was
// permanent, and every later file was left unread forever.
func TestRecoveryRowCheckpointOutOfBoundsEventuallyQuarantines(t *testing.T) {
	dir := t.TempDir()
	poisonWriter := crashRecoveryWriter(t, dir)
	ids, err := poisonWriter.AppendTracked([]map[string]interface{}{{"index": 0}})
	require.NoError(t, err)
	poison := poisonWriter.CurrentFile()
	require.NoError(t, poisonWriter.MarkFlushed([]string{recoveryRowIdentity(ids[0], 0, 2)}))
	require.NoError(t, poisonWriter.Close())

	// A healthy file that sorts AFTER the poison one, so reaching it is
	// evidence the pass did not stop at the poison file.
	laterWriter := crashRecoveryWriter(t, dir)
	require.NoError(t, laterWriter.Append([]map[string]interface{}{{"index": 99}}))
	later := laterWriter.CurrentFile()
	require.Greater(t, filepath.Base(later), filepath.Base(poison))
	require.NoError(t, laterWriter.Close())

	recovery := NewRecovery(dir, zerolog.Nop())
	replayed := 0
	// Row entries all route through TrackedRowCallback when it is set, so this
	// counts the LATER file's replay. The poison entry never reaches it: the
	// coverage check fails first, which passes 1 and 2 assert.
	opts := func() *RecoveryOptions {
		return &RecoveryOptions{
			TrackedRowCallback: func(_ context.Context, records []map[string]interface{}, _ string) error {
				replayed += len(records)
				return nil
			},
			MaxReplayFailures: 3,
		}
	}

	// Strikes 1 and 2 keep the file. The pass still stops at it, as it does for
	// every other poison shape, so the later file is not reached yet.
	for pass := 1; pass <= 2; pass++ {
		stats, err := recovery.RecoverWithOptions(context.Background(), nil, opts())
		require.NoError(t, err, "pass %d", pass)
		require.Zero(t, stats.QuarantinedFiles, "pass %d", pass)
		require.FileExists(t, poison, "pass %d", pass)
		require.FileExists(t, later, "pass %d", pass)
		require.Zero(t, replayed, "pass %d stopped at the poison file, as every poison shape does", pass)
	}

	// Strike 3 quarantines, and the pass then carries on to the later file.
	stats, err := recovery.RecoverWithOptions(context.Background(), nil, opts())
	require.NoError(t, err)
	require.Equal(t, 1, stats.QuarantinedFiles)
	require.NoFileExists(t, poison, "the poison file is renamed, not deleted")
	require.FileExists(t, poison+".failed", "quarantine must preserve the original bytes")
	require.Equal(t, 1, replayed, "the later file must be reached once the poison file is quarantined")
	require.NoFileExists(t, later)
}
