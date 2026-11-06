package ingest

import (
	"io"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const (
	tokA = "0123456789abcdef0000000000000001"
	tokB = "0123456789abcdef0000000000000002"
)

func spanBatch(rows int, hashes []string, copied bool) *TypedColumnBatch {
	times := make([]int64, rows)
	for i := range times {
		times[i] = int64(i)
	}
	return &TypedColumnBatch{Data: map[string]interface{}{"time": times}, WALHashes: hashes, hasWALCopy: copied}
}

// #1492: which records get a row span, and the offsets they get.
func TestWALRowSpansOfNamesOnlyExactlyKnownRows(t *testing.T) {
	records := []interface{}{
		spanBatch(3, []string{tokA}, true),                                  // tracked: rows 0-2 are tokA 0-2
		spanBatch(2, nil, false),                                            // no WAL copy
		spanBatch(4, []string{wal.RecoveryRowIdentity(tokB, 10, 14)}, true), // replayed range: rows 5-8 are tokB 10-13
		spanBatch(2, []string{tokA}, false),                                 // one token, no copy: a chunked payload whose later chunk was dropped (the adversarial B1 shape)
		spanBatch(2, []string{tokA, tokB}, true),                            // chunked: two entries
		spanBatch(1, []string{"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}, true), // a content hash
		spanBatch(2, []string{wal.RecoveryRowIdentity(tokB, 0, 3)}, true),                                // a range of the wrong length
		spanBatch(1, []string{"0123456789ABCDEF0000000000000001"}, true),                                 // upper case is not a token
		map[string]interface{}{"time": []int64{1, 2}},                                                    // a legacy map record
	}
	s := walRowSpansOf(records)
	require.NotNil(t, s)
	require.Equal(t, 19, s.rows)
	require.Len(t, s.spans, len(records))
	want := []walRowSpan{
		{lo: 0, hi: 3, parent: tokA, base: 0, copied: true},
		{lo: 3, hi: 5},
		{lo: 5, hi: 9, parent: tokB, base: 10, copied: true},
		{lo: 9, hi: 11},
		{lo: 11, hi: 13, copied: true},
		{lo: 13, hi: 14, copied: true},
		{lo: 14, hi: 16, copied: true},
		{lo: 16, hi: 17, copied: true},
		{lo: 17, hi: 19},
	}
	require.Equal(t, want, s.spans)

	// A record whose row count is unknown gives no map at all.
	require.Nil(t, walRowSpansOf([]interface{}{spanBatch(1, []string{tokA}, true), &TypedColumnBatch{Data: map[string]interface{}{"v": []float64{1}}}}))
	require.Nil(t, walRowSpansOf([]interface{}{"not a record"}))
}

func TestWALRowSpansRangesIn(t *testing.T) {
	s := walRowSpansOf([]interface{}{
		spanBatch(4, []string{tokA}, true),                                 // 0-3
		spanBatch(2, nil, false),                                           // 4-5: nothing to name
		spanBatch(3, []string{wal.RecoveryRowIdentity(tokB, 7, 10)}, true), // 6-8 are tokB 7-9
	})
	// One file holding rows 1,2 of A, both unnamed rows, and rows 6 and 8 of B.
	got := s.rangesIn([]int{1, 2, 4, 5, 6, 8})
	require.Equal(t, []string{
		wal.RecoveryRowIdentity(tokA, 1, 3),
		wal.RecoveryRowIdentity(tokB, 7, 8),
		wal.RecoveryRowIdentity(tokB, 9, 10),
	}, got)
	// A run that crosses a span boundary is two ranges.
	require.Equal(t, []string{wal.RecoveryRowIdentity(tokA, 3, 4), wal.RecoveryRowIdentity(tokB, 7, 8)}, s.rangesIn([]int{3, 6}))
	// Every row of one span: one range.
	require.Equal(t, []string{wal.RecoveryRowIdentity(tokA, 0, 4)}, s.rangesIn([]int{0, 1, 2, 3}))
	require.Nil(t, s.rangesIn([]int{4, 5}))
	require.Nil(t, s.rangesIn(nil))
	require.Nil(t, (*walRowSpans)(nil).rangesIn([]int{0}))

	// Rows that interleave hours name one range each; past the cap the file is
	// left to the parent checkpoint.
	big := walRowSpansOf([]interface{}{spanBatch(4*maxRowCheckpointRanges, []string{tokA}, true)})
	var even []int
	for i := 0; i < 2*maxRowCheckpointRanges; i += 2 {
		even = append(even, i)
	}
	require.Len(t, big.rangesIn(even[:maxRowCheckpointRanges]), maxRowCheckpointRanges)
	require.Nil(t, big.rangesIn(even[:maxRowCheckpointRanges+1]))
}

func TestWALRowSpansLostWithoutCopy(t *testing.T) {
	s := walRowSpansOf([]interface{}{
		spanBatch(2, []string{tokA}, true), // 0-1
		spanBatch(3, nil, false),           // 2-4: no WAL copy
		spanBatch(1, nil, false),           // 5
	})
	written := make([]bool, 6)
	written[2], written[3] = true, true
	require.Equal(t, 2, s.lostWithoutCopy(written), "rows 4 and 5 have no copy and are in no file")
	require.Equal(t, 4, s.lostWithoutCopy(nil))
	require.Equal(t, 0, (*walRowSpans)(nil).lostWithoutCopy(nil))
}

// mergeBatches sets the map; the sorted merge, a slice and a sort do not.
func TestMergeSetsWALRowSpansOnlyWhenPositional(t *testing.T) {
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 60000, FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, Compression: "snappy"}, &mockStorageBackend{}, zerolog.New(io.Discard))
	defer b.Close()
	hour := int64(3_600_000_000)
	mk := func(tok string, times ...int64) *TypedColumnBatch {
		vals := make([]float64, len(times))
		return &TypedColumnBatch{Data: map[string]interface{}{"time": times, "v": vals}, WALHashes: []string{tok}, hasWALCopy: true}
	}
	// Two hours: the positional merge.
	merged, err := b.mergeBatchesForFlush("m", []interface{}{mk(tokA, 1, 2), mk(tokB, hour+1, 3)})
	require.NoError(t, err)
	require.NotNil(t, merged.walSpans)
	require.Equal(t, 4, merged.walSpans.rows)
	require.Equal(t, []string{wal.RecoveryRowIdentity(tokA, 0, 2), wal.RecoveryRowIdentity(tokB, 1, 2)}, merged.walSpans.rangesIn([]int{0, 1, 3}))
	// One hour, two batches: the sorted merge reorders rows and names nothing.
	merged, err = b.mergeBatchesForFlush("m", []interface{}{mk(tokA, 5, 6), mk(tokB, 1, 2)})
	require.NoError(t, err)
	require.Nil(t, merged.walSpans)
	// One batch is returned as it is, with its own span.
	merged, err = b.mergeBatchesForFlush("m", []interface{}{mk(tokA, 1, hour+1)})
	require.NoError(t, err)
	require.NotNil(t, merged.walSpans)
	require.Equal(t, []string{wal.RecoveryRowIdentity(tokA, 1, 2)}, merged.walSpans.rangesIn([]int{1}))
	// A slice and a sort start over.
	require.Nil(t, sliceTypedColumnBatchByIndices(merged, []int{0}).walSpans)
	sorted := b.sortFlushBatch("db", "m", merged, []string{"time"})
	if sorted != merged {
		require.Nil(t, sorted.walSpans)
	}
}
