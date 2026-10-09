package wal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// The downgrade hazard is docs-only by design — a WALVersion gate would refuse
// every downgrade to defend a usually-empty state — so the operator needs a way
// to CHECK whether they are in it. PartialRowEntries is that check: a retained
// file holding row-range checkpoints for part of a parent entry is exactly what
// an older binary replays in full.
//
// This is the case a count taken after the replay loop misses, and it is the
// dangerous one: earlier batches are durable, a later batch fails, and the file
// is retained with its parent uncheckpointed.
func TestPartialRowEntriesReportsAFileKeptByALaterBatchFailure(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	rows := make([]map[string]interface{}, 5)
	for i := range rows {
		rows[i] = map[string]interface{}{"index": i}
	}
	_, err := w.AppendTracked(rows)
	require.NoError(t, err)
	path := w.CurrentFile()
	require.NoError(t, w.Close())

	calls := 0
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		// BatchSize < row count is what SPLITS the entry into row ranges.
		BatchSize: 2,
		TrackedRowCallback: func(context.Context, []map[string]interface{}, string) error {
			calls++
			if calls == 1 {
				return nil
			}
			return errors.New("storage is down")
		},
		CheckpointRecovered: func(context.Context, []string) error {
			t.Fatal("a partially replayed entry must not be finalized")
			return nil
		},
	})
	require.NoError(t, err)
	require.Positive(t, stats.KeptFiles)
	require.FileExists(t, path)
	require.Equal(t, 1, stats.PartialRowEntries,
		"a file retained after a split entry's later batch failed is the downgrade hazard and must be reported")
}

// A pass that only skips a too-recent file must NOT fire: MinFileAge is always
// 5 s on the periodic path, so gating on KeptFiles would have reported the
// hazard on essentially every tick.
func TestPartialRowEntriesIgnoresAFileSkippedForAge(t *testing.T) {
	dir, files := writeRecoveryFiles(t, 1)
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&RecoveryOptions{MinFileAge: time.Hour})
	require.NoError(t, err)
	require.FileExists(t, files[0])
	require.Positive(t, stats.SkippedFiles+stats.KeptFiles)
	require.Zero(t, stats.PartialRowEntries,
		"a file nobody read holds no partial ranges this pass created")
}

// A split entry that replays and flushes completely leaves no residue: the file
// is deleted, so there is nothing for an older binary to replay.
func TestPartialRowEntriesSilentWhenTheSplitFileIsReclaimed(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	rows := make([]map[string]interface{}, 4)
	for i := range rows {
		rows[i] = map[string]interface{}{"index": i}
	}
	_, err := w.AppendTracked(rows)
	require.NoError(t, err)
	path := w.CurrentFile()
	require.NoError(t, w.Close())

	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		BatchSize:           2,
		TrackedRowCallback:  func(context.Context, []map[string]interface{}, string) error { return nil },
		CheckpointRecovered: func(context.Context, []string) error { return nil },
		BeforeDelete:        func(context.Context) error { return nil },
	})
	require.NoError(t, err)
	require.NoFileExists(t, path)
	require.Zero(t, stats.PartialRowEntries,
		"a reclaimed file carries no residue, so the gauge must clear")
}

// A FIRST batch that fails hands off nothing, so no row range exists anywhere
// and the parent entry is wholly uncheckpointed — which is exactly the state an
// older binary replays CORRECTLY. Reporting it would block a safe downgrade and
// contradict the gauge's own documented meaning.
func TestPartialRowEntriesSilentWhenNoRangeWasHandedOff(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	rows := make([]map[string]interface{}, 5)
	for i := range rows {
		rows[i] = map[string]interface{}{"index": i}
	}
	_, err := w.AppendTracked(rows)
	require.NoError(t, err)
	path := w.CurrentFile()
	require.NoError(t, w.Close())

	calls := 0
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		BatchSize: 2,
		TrackedRowCallback: func(context.Context, []map[string]interface{}, string) error {
			calls++
			return errors.New("storage is down")
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Positive(t, stats.KeptFiles)
	require.FileExists(t, path)
	require.Zero(t, stats.PartialRowEntries,
		"nothing was handed off, so there is no partial residue and a downgrade is safe")
}
