package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/basekick-labs/arc/pkg/models"
	"github.com/stretchr/testify/require"
)

// #1342: a WAL entry an older binary accepted with a name Arc cannot read
// back is replayed without that column: the rows and other columns are kept.
func TestWALReplayDropsUnstorableColumns(t *testing.T) {
	r := newFlushRig(t, nil)
	ts := int64(1760000000000000)
	err := r.buf.WriteColumnarDirectReplay(context.Background(), "d", "m", map[string][]interface{}{
		"time": {ts, ts + 1}, "v": {1.0, 2.0}, "Time": {3.0, 4.0}, "t\xb0": {5.0, 6.0}, "Temp": {7.0, 8.0}, "temp": {9.0, 10.0},
	}, "")
	require.NoError(t, err)
	files := r.files(t)
	require.Len(t, files, 1)
	for _, gone := range []string{"Time", "t\xb0"} {
		require.NotContains(t, files[0].types, gone)
	}
	// Rows kept; case twins are not replay's to choose between.
	for _, kept := range []string{"time", "v", "Temp", "temp"} {
		require.Contains(t, files[0].types, kept)
	}
	require.Len(t, files[0].cols["v"], 2)
}

// The buffer refuses such a name from any path that did not (the backstop),
// before the WAL sees it.
func TestBufferBackstopRefusesUnstorableNames(t *testing.T) {
	r := newFlushRig(t, nil)
	err := r.buf.WriteColumnarRecord(context.Background(), "d", &models.ColumnarRecord{Measurement: "m", Columnar: true,
		Columns: map[string][]interface{}{"time": {int64(1760000000000000)}, "TIME": {1.0}}})
	require.True(t, errors.Is(err, ErrUnstorableColumnName), "err = %v", err)
	batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1760000000000000}, "\xb0": []float64{1}}}
	err = r.buf.WriteTypedColumnarDirect(context.Background(), "d", "m", batch, 1)
	require.True(t, errors.Is(err, ErrUnstorableColumnName), "err = %v", err)
}

// Claimed and republished stream records are replayed the same way.
func TestClaimedReplayDropsUnstorableColumns(t *testing.T) {
	reg := &coverageRegistrar{}
	b, _ := newCoverageBuffer(t, 1000, reg)
	entry := columnarEntry("db", "cpu", []int64{usAt(rcH1, 1)}, []interface{}{1.0})
	entry.ColumnarData.Columns["Time"] = []interface{}{2.0}
	subs, out := b.claimedRecordSubs(0, ClaimedRecord{CSeq: 1, Chunks: 1, Entry: entry, Keep: keepAll},
		ClaimedReplay{Database: "db", Measurement: "cpu", Instance: 1, Epoch: 1})
	require.Empty(t, out)
	require.NotEmpty(t, subs)
	for _, s := range subs {
		require.NotContains(t, s.batch.Data, "Time")
		require.Contains(t, s.batch.Data, "value")
	}
}
