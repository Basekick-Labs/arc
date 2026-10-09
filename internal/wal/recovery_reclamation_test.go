package wal

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// This tests the checkpoint-dependency condition in the operator procedure.
// The durable identity set models independently reconciled storage; the test
// does not claim that ordinary recovery can verify quarantined data itself.
func testQuarantineReclamation(t *testing.T, rows []map[string]interface{}) {
	t.Helper()
	for _, early := range []bool{true, false} {
		name := "drain_then_archive"
		if early {
			name = "early_archive_duplicates"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			w := crashRecoveryWriter(t, dir)
			appendRows := func() string {
				ids, err := w.AppendTracked(rows)
				require.NoError(t, err)
				require.Len(t, ids, 1)
				return ids[0]
			}
			dataPath := w.CurrentFile()
			flushedID := appendRows()
			pendingID := appendRows()
			require.NoError(t, w.Rotate())
			quarantinePath := w.CurrentFile() + ".failed"
			require.NoError(t, w.MarkFlushed([]string{flushedID}))
			quarantinedID := appendRows()
			require.NoError(t, w.Close())
			require.NoError(t, os.Rename(w.CurrentFile(), quarantinePath))
			original, err := os.ReadFile(quarantinePath)
			require.NoError(t, err)
			archivePath := filepath.Join(t.TempDir(), filepath.Base(quarantinePath))
			archive := func() {
				t.Helper()
				// Model a verified archival copy before removing the source.
				require.NoError(t, os.WriteFile(archivePath, original, 0600))
				copyBytes, err := os.ReadFile(archivePath)
				require.NoError(t, err)
				require.Equal(t, sha256.Sum256(original), sha256.Sum256(copyBytes))
				require.NoError(t, os.Remove(quarantinePath))
			}
			durable := map[string]bool{flushedID: true, quarantinedID: true}
			replayed := make(map[string]int)
			callback := func(_ context.Context, _ []map[string]interface{}, id string) error {
				replayed[id]++
				return nil
			}
			if early {
				archive()
			}
			if !early {
				// A barrier outage leaves an earlier dependency; the procedure
				// must stop here even though its replay callback succeeded.
				stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil,
					&RecoveryOptions{TrackedRowCallback: callback, BeforeDelete: func(context.Context) error {
						return errors.New("storage unavailable")
					}})
				require.Error(t, err)
				require.Equal(t, 1, stats.KeptFiles)
				require.FileExists(t, dataPath)
				require.FileExists(t, quarantinePath)
				require.Zero(t, replayed[flushedID])
				clear(replayed)
			}
			stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil,
				&RecoveryOptions{TrackedRowCallback: callback, BeforeDelete: func(context.Context) error {
					for id := range replayed {
						durable[id] = true
					}
					return nil
				}})
			require.NoError(t, err)
			require.Zero(t, stats.KeptFiles)
			require.Equal(t, 1, replayed[pendingID])
			require.Zero(t, replayed[quarantinedID], "normal recovery does not account for quarantined data")
			if early {
				require.Equal(t, 1, replayed[flushedID], "removing the sole checkpoint causes duplicate replay")
				return
			}
			require.Zero(t, replayed[flushedID])
			pending, err := filepath.Glob(filepath.Join(dir, "*.wal"))
			require.NoError(t, err)
			require.Empty(t, pending, "ordinary dependencies must be drained before archiving quarantine")
			entries, err := NewReader(quarantinePath, zerolog.Nop()).ReadAll()
			require.NoError(t, err)
			for _, entry := range entries {
				if len(entry.Records) > 0 {
					require.True(t, durable[entry.PayloadHash], "quarantined records need independent accounting")
				}
			}
			archive()
			clear(replayed)
			_, err = NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil,
				&RecoveryOptions{TrackedRowCallback: callback})
			require.NoError(t, err)
			require.Empty(t, replayed, "no old data can replay after the dependency set is drained")
			archived, err := os.ReadFile(archivePath)
			require.NoError(t, err)
			require.Equal(t, original, archived)
		})
	}
}

func TestQuarantineReclamationCheckpointDependencies(t *testing.T) {
	testQuarantineReclamation(t, []map[string]interface{}{{"_measurement": "events", "value": 1}})
}
