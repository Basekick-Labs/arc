package ingest

// #1424: a columnar record whose values its columns cannot hold (an integer
// column with a string in it) failed the write with an unclassified error,
// which the MessagePack handler answered 500. The conversion runs before the
// WAL append (#1438), so it is a refusal for the record's content: the record
// is dropped and the rest of the batch written.

import (
	"context"
	"errors"
	"testing"

	"github.com/basekick-labs/arc/pkg/models"
	"github.com/stretchr/testify/require"
)

func mixedTypeRecord(measurement string) *models.ColumnarRecord {
	rec := stopTestRecord(measurement, "v", 2)
	rec.Columns["v"] = []interface{}{int64(1), "oops"}
	return rec
}

func TestWriteColumnValueTheColumnCannotHoldIsAContentRefusal(t *testing.T) {
	b := newStopTestBuffer(t)
	err := b.Write(context.Background(), "db", []interface{}{mixedTypeRecord("bbb")})
	require.ErrorIs(t, err, ErrUnstorableColumnValue)
	require.True(t, IsContentRefusal(err), "err = %v", err)
	require.Contains(t, err.Error(), `measurement "bbb"`)
	require.Contains(t, err.Error(), "cannot convert string to int64 in column 'v'")
	require.Zero(t, b.bufferedRecordsFor("db", "bbb"))
}

// What convertColumnsToTyped refuses is the record's content, whichever
// column it is in, so every kind is classified the same way. A decimal value
// outside its precision is covered through line protocol in the api package.
func TestColumnConversionFailuresAreContentRefusals(t *testing.T) {
	cases := map[string][]interface{}{
		"float column with a string":  {1.5, "oops"},
		"string column with a bool":   {"a", true},
		"bool column with an int":     {true, int64(1)},
		"binary column with a string": {[]byte{1}, "a"},
		"unsupported type":            {map[string]interface{}{"a": 1}, nil},
	}
	for name, col := range cases {
		t.Run(name, func(t *testing.T) {
			b := newStopTestBuffer(t)
			rec := stopTestRecord("m", "v", 2)
			rec.Columns["v"] = col
			err := b.Write(context.Background(), "db", []interface{}{rec})
			require.ErrorIs(t, err, ErrUnstorableColumnValue)
		})
	}
	for name, times := range map[string][]interface{}{
		"string time": {"2026-11-02", "2026-11-03"},
		"null time":   {int64(1700000000000000), nil},
	} {
		t.Run(name, func(t *testing.T) {
			b := newStopTestBuffer(t)
			rec := stopTestRecord("m", "v", 2)
			rec.Columns["time"] = times
			require.ErrorIs(t, b.Write(context.Background(), "db", []interface{}{rec}), ErrUnstorableColumnValue)
		})
	}
}

// The record is dropped and the records around it are written: a partial
// write, as for the other content refusals.
func TestWriteDropsAColumnValueRefusalAndWritesTheRest(t *testing.T) {
	b := newStopTestBuffer(t)
	batch := []interface{}{stopTestRecord("aaa", "v", 1), mixedTypeRecord("bbb"), stopTestRecord("ccc", "v", 3)}

	err := b.Write(context.Background(), "db", batch)
	var pd *PointsDroppedError
	require.True(t, errors.As(err, &pd), "err = %v (%T), want PointsDroppedError", err, err)
	require.Len(t, pd.Drops, 1)
	require.Equal(t, "bbb", pd.Drops[0].Measurement)
	require.Equal(t, 2, pd.Drops[0].Points)
	require.Equal(t, 1, pd.Records)
	require.Contains(t, pd.Drops[0].Reason, "cannot convert string to int64 in column 'v'")
	require.Equal(t, 1, b.bufferedRecordsFor("db", "aaa"))
	require.Equal(t, 3, b.bufferedRecordsFor("db", "ccc"))
	require.Zero(t, b.bufferedRecordsFor("db", "bbb"))
}

// A dropped record must not come back from the WAL at the next start: that is
// what makes the drop a content refusal rather than a failure part-way.
func TestColumnValueRefusalLeavesNothingInTheWAL(t *testing.T) {
	for _, mode := range walModes {
		t.Run(string(mode), func(t *testing.T) {
			buf, w, dir := newWALRefusalRig(t, mode, nil)
			ok := &models.ColumnarRecord{Measurement: "aaa", Columnar: true, Columns: refusalTestColumns()}
			err := buf.Write(context.Background(), "d", []interface{}{ok, mixedTypeRecord("bbb")})
			var pd *PointsDroppedError
			require.True(t, errors.As(err, &pd), "err = %v", err)
			require.Equal(t, 1, replayedWALEntries(t, w, dir), "only the written record is in the WAL")
			_ = buf.Close()
		})
	}
}
