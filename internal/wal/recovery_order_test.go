package wal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Changing mtimes must not make recovery reclaim a
// later checkpoint while an earlier data file still depends on its proof.
func testRecoveryFileOrderPreservesCheckpoint(t *testing.T, rows []map[string]interface{}) {
	t.Helper()
	dir := t.TempDir()
	w := crashRecoveryWriter(t, dir)
	flushed, err := w.AppendTracked(rows)
	require.NoError(t, err)
	pending, err := w.AppendTracked(rows)
	require.NoError(t, err)
	dataPath := w.CurrentFile()
	require.NoError(t, w.Rotate())
	proofPath := w.CurrentFile()
	require.NoError(t, w.MarkFlushed(flushed))
	require.NoError(t, w.Close())
	require.Less(t, dataPath, proofPath)
	now := time.Now()
	require.NoError(t, os.Chtimes(proofPath, now.Add(-2*time.Hour), now.Add(-2*time.Hour)))
	require.NoError(t, os.Chtimes(dataPath, now.Add(-time.Hour), now.Add(-time.Hour)))
	pendingReplayed := false
	outage := errors.New("storage outage")
	_, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		BarrierBatchFiles: 1,
		TrackedRowCallback: func(_ context.Context, _ []map[string]interface{}, id string) error {
			require.Equal(t, pending[0], id)
			pendingReplayed = true
			return nil
		},
		BeforeDelete: func(context.Context) error {
			if pendingReplayed {
				return outage
			}
			return nil
		},
	})
	require.ErrorIs(t, err, outage)
	require.True(t, pendingReplayed)
	require.FileExists(t, dataPath)
	if _, err := os.Stat(proofPath); os.IsNotExist(err) {
		t.Error("later checkpoint reclaimed before retained data dependency")
	}
	duplicates := 0
	retriedRows := 0
	_, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		TrackedRowCallback: func(_ context.Context, replayed []map[string]interface{}, id string) error {
			if id == flushed[0] {
				duplicates++
			} else {
				require.Equal(t, pending[0], id)
				retriedRows += len(replayed)
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.Zero(t, duplicates, "durable rows replayed after checkpoint-first reclamation")
	require.Equal(t, len(rows), retriedRows, "pending rows must still recover on retry")
}

func TestRecoveryFileOrderPreservesCheckpoint(t *testing.T) {
	testRecoveryFileOrderPreservesCheckpoint(t, []map[string]interface{}{{"index": 0}})
}

func TestRecoveryFileOrderIgnoresModificationTimes(t *testing.T) {
	for _, equal := range []bool{false, true} {
		name := "reversed"
		if equal {
			name = "equal"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			want := []string{
				filepath.Join(dir, "arc-20261008_100000.000000001.wal"),
				filepath.Join(dir, "arc-20261008_100000.000000002.wal"),
				filepath.Join(dir, "arc-20261008_100000.000000003.wal"),
			}
			stamp := time.Unix(1700000000, 0)
			for i := len(want) - 1; i >= 0; i-- {
				require.NoError(t, os.WriteFile(want[i], nil, 0600))
				mtime := stamp
				if !equal {
					mtime = stamp.Add(-time.Duration(i) * time.Hour)
				}
				require.NoError(t, os.Chtimes(want[i], mtime, mtime))
			}
			require.NoError(t, os.WriteFile(want[0]+".failed", nil, 0600))
			got, err := NewRecovery(dir, zerolog.Nop()).findWALFiles()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}
