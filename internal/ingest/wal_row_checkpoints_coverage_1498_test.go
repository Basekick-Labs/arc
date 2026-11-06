package ingest

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const fxKey = "fx/ticks"

func floorOf(t *testing.T, b *ArrowBuffer) KeyRegistration {
	t.Helper()
	_, keys, ok := b.RegistrationProgress()
	require.True(t, ok)
	return keys[fxKey]
}

// #1498: on a coverage-enabled writer a multi-hour flush registers each hour
// file with the group's coverage as it is stored and checkpoints its rows;
// the group finishes as before once every file settled.
func TestCoveredMultiHourFlushRegistersEachHourAndCheckpointsItsRows(t *testing.T) {
	reg := &coverageRegistrar{}
	f := newRowCheckpointFixtureCov(t, 0, reg)
	require.NoError(t, f.buffer.FlushAll(context.Background()))

	files := reg.snapshot()
	require.Len(t, files, 4)
	for _, cf := range files {
		require.NotNil(t, cf.cov)
		require.Equal(t, 4, cf.cov.Files)
		require.NotNil(t, cf.cov.File, "per-file coverage, as the deferred registration produced")
		ranges, partial, exact := cf.cov.FilePieces()
		require.True(t, exact)
		require.Empty(t, partial, "live batches are whole pieces")
		require.NotEmpty(t, ranges)
	}
	ranges, whole := f.coveredRows(t)
	for tok, times := range f.rows {
		require.True(t, whole[tok])
		require.Len(t, ranges[tok], len(times))
	}
	// The group finishes when the files settle: tokens released, floor advanced.
	require.Equal(t, 3, f.writer.PendingUnflushedCount())
	reg.settleAll(CoverageRegistered)
	require.Equal(t, 0, f.writer.PendingUnflushedCount())
	require.Equal(t, uint64(3), floorOf(t, f.buffer).Floor)
}

// A covered multi-hour flush that fails part-way: the stored files' rows are
// checkpointed, the group settles the tracker with them only once their
// outcomes arrive, and recovery's replay of the rest completes the batches.
func TestCoveredPartialFlushSettlesStoredRowsOnOutcome(t *testing.T) {
	reg := &coverageRegistrar{}
	f := newRowCheckpointFixtureCov(t, 3, reg)
	err := f.buffer.FlushAll(context.Background())
	var partial *flushPartialError
	require.True(t, errors.As(err, &partial), "%v", err)
	require.Equal(t, 2, partial.StoredFiles)
	require.Len(t, reg.snapshot(), 2)
	stored := f.storage.storedHours(t)
	ranges, whole := f.coveredRows(t)
	for tok, times := range f.rows {
		require.False(t, whole[tok])
		for r, ts := range times {
			require.Equal(t, stored[hourBucketOfMicros(ts)], ranges[tok][r])
		}
	}

	// Nothing settled yet: the files have no outcome.
	kr := floorOf(t, f.buffer)
	require.Equal(t, uint64(0), kr.Floor)
	require.Empty(t, kr.Settled)
	require.Empty(t, kr.Lost)
	require.Equal(t, 3, f.writer.PendingUnflushedCount(), "the parents stay pending: their other hours are only in the WAL")

	// The outcomes arrive: every batch straddles every hour, so each is
	// partly stored; the floor does not move, nothing is lost or done.
	reg.settleAll(CoverageRegistered)
	kr = floorOf(t, f.buffer)
	require.Equal(t, uint64(0), kr.Floor)
	require.Empty(t, kr.Settled)
	require.Empty(t, kr.Lost)
	require.Equal(t, 3, f.writer.PendingUnflushedCount())

	// Recovery replays the complement into the same buffer, under the
	// entries' coverage; its flush registers the other two hours.
	require.NoError(t, f.writer.Rotate())
	extra, err := f.writer.CurrentCheckpointHashes()
	require.NoError(t, err)
	replayedRows := 0
	_, err = wal.NewRecovery(filepath.Dir(f.writer.CurrentFile()), zerolog.Nop()).RecoverWithOptions(context.Background(),
		func(context.Context, []map[string]interface{}) error { return nil },
		&wal.RecoveryOptions{
			SkipActiveFile:             f.writer.CurrentFile(),
			AdditionalCheckpointHashes: extra,
			ColumnarCallbackWithTagColumns: func(ctx context.Context, db, m string, cols map[string][]interface{}, id string, tags []string) error {
				_, s, e, ok := wal.ParseRecoveryRowIdentity(id)
				require.True(t, ok, id)
				replayedRows += e - s
				return f.buffer.WriteColumnarDirectReplayWithTagColumns(ctx, db, m, cols, id, tags)
			},
			CheckpointRecovered: func(_ context.Context, parents []string) error { return f.writer.MarkFlushed(parents) },
			BeforeDelete:        f.buffer.NewRecoveryFlushBarrier(),
		})
	require.NoError(t, err)
	total := 0
	for _, times := range f.rows {
		total += len(times)
	}
	require.Equal(t, total-partial.StoredRows, replayedRows)
	require.NoError(t, f.buffer.FlushAll(context.Background()))
	files := reg.snapshot()
	require.Greater(t, len(files), 2)
	for _, cf := range files[2:] {
		_, partialPieces, _ := cf.cov.FilePieces()
		require.NotEmpty(t, partialPieces, "the replayed rows register as partial pieces")
	}
	reg.settleAll(CoverageRegistered)
	kr = floorOf(t, f.buffer)
	require.Equal(t, uint64(3), kr.Floor, "stored rows plus the replayed complement complete every batch")
	require.Empty(t, kr.Lost)
	require.Equal(t, 0, f.writer.PendingUnflushedCount(), "recovery finalized the parents")
}

// A stored file that then settles lost or refused leaves its rows
// unregistered: the floor does not pass them and Lost names them. This is why
// the tracker is settled on outcome, never at failure time.
func TestCoveredPartialFlushLostFileKeepsTheFloor(t *testing.T) {
	for _, outcome := range []CoverageOutcome{CoverageLost, CoverageRefused} {
		t.Run(outcome.String(), func(t *testing.T) {
			reg := &coverageRegistrar{}
			f := newRowCheckpointFixtureCov(t, 3, reg)
			require.Error(t, f.buffer.FlushAll(context.Background()))
			files := reg.snapshot()
			require.Len(t, files, 2)
			files[0].cov.Settle(files[0].path, outcome)
			kr := floorOf(t, f.buffer)
			require.Equal(t, uint64(0), kr.Floor)
			require.Empty(t, kr.Lost, "not settled until every stored file has its outcome")
			files[1].cov.Settle(files[1].path, CoverageRegistered)
			kr = floorOf(t, f.buffer)
			require.Equal(t, uint64(0), kr.Floor)
			require.ElementsMatch(t, []uint64{1, 2, 3}, kr.Lost)
			require.Equal(t, 3, f.writer.PendingUnflushedCount(), "no token released")
			// A refused file resolved (its rows found in other registered
			// files) releases a complete group's entries; not a partial
			// flush's, whose other hours are still only in the WAL.
			files[0].cov.Settle(files[0].path, CoverageResolved)
			require.Equal(t, 3, f.writer.PendingUnflushedCount(), "a resolved refusal must not release a partial flush's entries")
		})
	}
}

// failed() leaves a piece the partial flush stored whole to its file's
// outcome, even without a WAL copy; a partly stored one without a copy is
// lost. The group is finished once the stored files settled.
func TestFailedLeavesStoredWholePiecesToTheirOutcome(t *testing.T) {
	cs := &coverageState{instance: 9, tracker: newCoverageTracker(9)}
	b := NewArrowBuffer(testCoverageIngestConfig(), &mockStorageBackend{}, zerolog.Nop())
	defer b.Close()
	whole := CoveragePiece{Instance: 9, CSeq: 1}  // no WAL copy, all rows stored
	partly := CoveragePiece{Instance: 9, CSeq: 2} // no WAL copy, half stored
	g := &flushGroup{b: b, cs: cs, key: fxKey, pieces: []CoveragePiece{whole, partly}, settled: map[string]struct{}{}}
	g.coverage(2) // announced
	g.storedPartially([]CoveragePiece{whole, {Instance: 9, CSeq: 2, RowLo: 0, RowHi: 2, RowTotal: 4}}, 1)
	g.failed()
	kr := cs.tracker.progress()[fxKey]
	require.ElementsMatch(t, []uint64{2}, kr.Lost, "the partly stored piece without a copy is lost; the whole one waits for its file")
	g.settle("f1", CoverageRegistered)
	g.mu.Lock()
	finished := g.finished
	g.mu.Unlock()
	require.True(t, finished)
	kr = cs.tracker.progress()[fxKey]
	require.Equal(t, uint64(1), kr.Floor, "the stored-whole piece settled on its outcome")
	require.ElementsMatch(t, []uint64{2}, kr.Lost)
}

func testCoverageIngestConfig() *config.IngestConfig {
	return &config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy", ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 4}
}

// storedPieces: whole pieces for a fully stored batch, one partial piece per
// stored run otherwise, composed on a replayed range's coordinates; nothing
// for a batch whose rows cannot be named.
func TestStoredPiecesByWrittenRows(t *testing.T) {
	live := CoveragePiece{Instance: 7, CSeq: 1}
	replayed := CoveragePiece{Instance: 7, CSeq: 2, RowLo: 10, RowHi: 14, RowTotal: 20}
	chunk := CoveragePiece{Instance: 7, CSeq: 3, Chunk: 1, Chunks: 2}
	g := &flushGroup{spans: []coverageSpan{
		{lo: 0, hi: 4, pieces: []CoveragePiece{live}},
		{lo: 4, hi: 8, pieces: []CoveragePiece{replayed}},
		{lo: 8, hi: 10, pieces: []CoveragePiece{chunk}}, // chunked WAL copy: unnameable
		{lo: 10, hi: 12, pieces: []CoveragePiece{{Instance: 7, CSeq: 4}}},
	}}
	spans := &walRowSpans{rows: 12, spans: []walRowSpan{
		{lo: 0, hi: 4, parent: tokA, copied: true},
		{lo: 4, hi: 8, parent: tokB, base: 10, copied: true},
		{lo: 8, hi: 10, copied: true},
		{lo: 10, hi: 12, parent: tokA, copied: true},
	}}
	written := make([]bool, 12)
	for _, i := range []int{0, 1, 2, 3, 4, 6, 7, 8, 9} {
		written[i] = true
	}
	got := g.storedPieces(spans, written)
	require.Equal(t, []CoveragePiece{
		live,
		{Instance: 7, CSeq: 2, RowLo: 10, RowHi: 11, RowTotal: 20},
		{Instance: 7, CSeq: 2, RowLo: 12, RowHi: 14, RowTotal: 20},
	}, got)
	require.Nil(t, g.storedPieces(nil, written))
	require.Nil(t, (*flushGroup)(nil).storedPieces(spans, written))
}
