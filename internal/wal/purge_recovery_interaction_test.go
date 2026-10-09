package wal

import (
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// PurgeFlushed and recovery were never driven together by any test in this
// package, and the interaction is where the row-range proof hazard lives: a
// flush writes a row-range checkpoint into the CURRENT file while the entries
// it describes sit in an EARLIER file that recovery retained. The purge walk
// stops at the first file it must retain, but a retained file from a previous
// process is not in the writer's own file order at all — so the walk never
// sees it, and the file carrying its only proof is an ordinary candidate.
//
// Deleting that proof makes the next pass replay rows already in storage:
// permanent duplicate rows for a measurement without tags.
func TestPurgeFlushedKeepsTheProofForAFileRecoveryRetained(t *testing.T) {
	dir := t.TempDir()

	// Pass 1: a tracked entry whose replay is only PARTIALLY flushed, so the
	// file is retained and a row-range checkpoint is written for the part that
	// landed. The writer that writes the checkpoint is a NEW instance, exactly
	// as it would be after a restart, so the checkpoint lands in its own file.
	dataWriter := crashRecoveryWriter(t, dir)
	ids, err := dataWriter.AppendTracked([]map[string]interface{}{
		{"index": 0}, {"index": 1}, {"index": 2}, {"index": 3},
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)
	dataFile := dataWriter.CurrentFile()
	require.NoError(t, dataWriter.Close())

	proofWriter, err := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 * 1024 * 1024, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	defer proofWriter.Close()
	proofFile := proofWriter.CurrentFile()

	// The durable half: rows [0,2) of the previous process's entry reached
	// storage, and its proof is written here, in this process's file.
	require.NoError(t, proofWriter.MarkFlushed([]string{recoveryRowIdentity(ids[0], 0, 2)}))

	// Rotate so the proof file is closed and therefore an ordinary purge
	// candidate, and give the writer a sequence floor that clears it.
	require.NoError(t, proofWriter.Rotate())

	deleted, err := proofWriter.PurgeFlushed(proofWriter.MinUnflushedSequence())
	require.NoError(t, err)
	require.FileExists(t, proofFile,
		"the purge deleted the only proof that the retained file's rows [0,2) were flushed (deleted=%d); the next pass will replay them", deleted)
	require.FileExists(t, dataFile, "the retained data file must survive the purge too")

	// Pass 2: recovery must replay only the UNCOVERED rows. If the proof had
	// been purged it would replay all four.
	var replayed []int
	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		SkipActiveFile: proofWriter.CurrentFile(),
		TrackedRowCallback: func(_ context.Context, records []map[string]interface{}, _ string) error {
			for _, record := range records {
				index, ok := record["index"]
				require.True(t, ok, "replayed record has no index: %v", record)
				replayed = append(replayed, int(toInt64(t, index)))
			}
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, []int{2, 3}, replayed,
		"recovery replayed rows the row-range checkpoint already covers")
	require.Positive(t, stats.RecoveredFiles)
}

// The inverse direction, so the test above cannot pass for the wrong reason: a
// file whose rows are FULLY covered replays nothing, and both it and its proof
// become reclaimable.
func TestPurgeFlushedAfterFullCoverageReclaimsBothFiles(t *testing.T) {
	dir := t.TempDir()

	dataWriter := crashRecoveryWriter(t, dir)
	ids, err := dataWriter.AppendTracked([]map[string]interface{}{{"index": 0}, {"index": 1}})
	require.NoError(t, err)
	dataFile := dataWriter.CurrentFile()
	require.NoError(t, dataWriter.Close())

	proofWriter, err := NewWriter(&WriterConfig{
		WALDir: dir, SyncMode: SyncModeFsync, MaxSizeBytes: 100 * 1024 * 1024, Logger: zerolog.Nop(),
	})
	require.NoError(t, err)
	defer proofWriter.Close()
	require.NoError(t, proofWriter.MarkFlushed([]string{recoveryRowIdentity(ids[0], 0, 2)}))
	require.NoError(t, proofWriter.Rotate())
	_, err = proofWriter.PurgeFlushed(proofWriter.MinUnflushedSequence())
	require.NoError(t, err)

	stats, err := NewRecovery(dir, zerolog.Nop()).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		SkipActiveFile: proofWriter.CurrentFile(),
		TrackedRowCallback: func(context.Context, []map[string]interface{}, string) error {
			t.Fatal("a fully covered entry was replayed")
			return nil
		},
	})
	require.NoError(t, err)
	require.Zero(t, stats.PartialRowEntries,
		"a fully covered entry is not a partial-range residue")
	_, statErr := os.Stat(dataFile)
	require.True(t, os.IsNotExist(statErr), "a fully covered file should be reclaimed")
}

func toInt64(t *testing.T, value interface{}) int64 {
	t.Helper()
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int8:
		return int64(typed)
	case int16:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case uint8:
		return int64(typed)
	case uint16:
		return int64(typed)
	case uint32:
		return int64(typed)
	case uint64:
		return int64(typed)
	case float32:
		return int64(typed)
	case float64:
		return int64(typed)
	}
	t.Fatalf("unexpected numeric type %T", value)
	return 0
}
