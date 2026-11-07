package wal

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func columnarRowsPayload(t *testing.T, timeRows, vRows int) []byte {
	t.Helper()
	times := make([]interface{}, timeRows)
	vals := make([]interface{}, vRows)
	for i := range times {
		times[i] = int64(1700000000000000 + i)
	}
	for i := range vals {
		vals[i] = float64(i)
	}
	raw, err := msgpack.Marshal(map[string]interface{}{"m": "ticks", "columns": map[string]interface{}{"time": times, "v": vals}})
	require.NoError(t, err)
	return raw
}

// #1492: a columnar entry some of whose rows a multi-hour flush checkpointed
// replays only the other rows, each range under its own identity; the parent
// is finalized once, and a later pass replays nothing.
func TestRecoveryColumnarEntryReplaysOnlyUncoveredRows(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	ids, err := w.AppendRawWithMetaTracked("fx", columnarRowsPayload(t, 7, 7))
	require.NoError(t, err)
	require.Len(t, ids, 1)
	// The flush stored the hour files holding rows 0-2 and 5-6 before the crash.
	require.NoError(t, w.MarkFlushed([]string{RecoveryRowIdentity(ids[0], 0, 3), RecoveryRowIdentity(ids[0], 5, 7)}))
	path := w.CurrentFile()
	require.NoError(t, w.Rotate())

	var replayed [][2]int
	finalized := 0
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		SkipActiveFile: w.CurrentFile(),
		ColumnarCallbackWithTagColumns: func(_ context.Context, db, m string, cols map[string][]interface{}, id string, _ []string) error {
			parent, s, e, ok := ParseRecoveryRowIdentity(id)
			require.True(t, ok, id)
			require.Equal(t, ids[0], parent)
			require.Equal(t, "fx", db)
			require.Equal(t, "ticks", m)
			require.Len(t, cols["time"], e-s)
			require.Len(t, cols["v"], e-s)
			require.Equal(t, int64(1700000000000000+s), cols["time"][0])
			replayed = append(replayed, [2]int{s, e})
			return w.MarkFlushed([]string{id})
		},
		CheckpointRecovered: func(_ context.Context, parents []string) error {
			finalized++
			require.Equal(t, ids, parents)
			return w.MarkFlushed(parents)
		},
	})
	require.NoError(t, err)
	require.Equal(t, [][2]int{{3, 5}}, replayed)
	require.Equal(t, 1, finalized)
	require.Equal(t, 2, stats.RecoveredEntries, "rows, not entries, so the barrier counts them")
	require.Equal(t, 0, w.PendingUnflushedCount())
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "the file was replayed in full and deleted")

	// Nothing left: the parent is checkpointed.
	calls := 0
	_, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		SkipActiveFile: w.CurrentFile(),
		ColumnarCallbackWithTagColumns: func(context.Context, string, string, map[string][]interface{}, string, []string) error {
			calls++
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, 0, calls)
}

// A failed range leaves the parent unfinalized and the file kept.
func TestRecoveryColumnarRangeFailureKeepsTheFile(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	ids, err := w.AppendRawWithMetaTracked("fx", columnarRowsPayload(t, 7, 7))
	require.NoError(t, err)
	require.NoError(t, w.MarkFlushed([]string{RecoveryRowIdentity(ids[0], 2, 4)}))
	path := w.CurrentFile()
	require.NoError(t, w.Rotate())
	calls := 0
	_, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		SkipActiveFile: w.CurrentFile(),
		ColumnarCallbackWithTagColumns: func(_ context.Context, _, _ string, _ map[string][]interface{}, id string, _ []string) error {
			calls++
			if calls == 2 {
				return context.DeadlineExceeded
			}
			return w.MarkFlushed([]string{id})
		},
		CheckpointRecovered: func(context.Context, []string) error { t.Fatal("partial entry finalized"); return nil },
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "rows 0-1, then 4-6 which fails")
	require.FileExists(t, path)
	require.Equal(t, 1, w.PendingUnflushedCount(), "the parent's sequence still pins the floor")
}

// A row checkpoint beyond the entry, and ragged columns, take the strike path:
// the file is kept, nothing is replayed from it.
func TestRecoveryColumnarRowCoverageGuards(t *testing.T) {
	for name, tc := range map[string]struct {
		payload func(t *testing.T) []byte
		rng     [2]int
	}{
		"checkpoint beyond the entry": {func(t *testing.T) []byte { return columnarRowsPayload(t, 7, 7) }, [2]int{0, 9}},
		"ragged columns":              {func(t *testing.T) []byte { return columnarRowsPayload(t, 7, 5) }, [2]int{0, 3}},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w := crashRecoveryWriter(t, dir)
			ids, err := w.AppendRawWithMetaTracked("fx", tc.payload(t))
			require.NoError(t, err)
			require.NoError(t, w.MarkFlushed([]string{RecoveryRowIdentity(ids[0], tc.rng[0], tc.rng[1])}))
			path := w.CurrentFile()
			require.NoError(t, w.Rotate())
			calls := 0
			stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
				SkipActiveFile: w.CurrentFile(),
				ColumnarCallbackWithTagColumns: func(context.Context, string, string, map[string][]interface{}, string, []string) error {
					calls++
					return nil
				},
				CheckpointRecovered: func(context.Context, []string) error { t.Fatal("finalized"); return nil },
			})
			require.NoError(t, err)
			require.Equal(t, 0, calls)
			require.FileExists(t, path)
			require.GreaterOrEqual(t, stats.KeptFiles, 1)
		})
	}
}

// A row range of this process's own entry is not foreign proof: its parent's
// pending sequence protects it, so the checkpoint's file is not pinned.
func TestOwnRowRangeCheckpointIsNotForeignProof(t *testing.T) {
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	ids, err := w.AppendRawWithMetaTracked("fx", columnarRowsPayload(t, 4, 4))
	require.NoError(t, err)
	require.False(t, w.coversForeignIdentity([]string{RecoveryRowIdentity(ids[0], 0, 2)}))
	require.True(t, w.coversForeignIdentity([]string{RecoveryRowIdentity("ffffffffffffffff0000000000000001", 0, 2)}))
	require.True(t, w.coversForeignIdentity([]string{recoveryRowPrefix + "garbage"}))
}
