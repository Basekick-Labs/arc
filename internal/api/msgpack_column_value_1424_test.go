package api

// #1424: a columnar MessagePack record with a value its column cannot hold
// was answered 500, while the same conflict over line protocol or in row
// records is a 400 partial write. A 500 tells a client the server failed, so
// an SDK treats bad input as an outcome it cannot know.

import (
	"net/http"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/stretchr/testify/require"
)

func columnarPayload(measurement string, columns map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{"m": measurement, "columns": columns}
}

// The request from the issue: the only record is refused, nothing is written.
func TestMsgPackColumnarTypeConflictIs400(t *testing.T) {
	r, post := newMsgPackRowsRig(t)
	status, out := post(columnarPayload("cpu", map[string]interface{}{
		"time": []int64{1700000000000, 1700000001000},
		"v":    []interface{}{1, "oops"},
	}))
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "column value cannot be stored")
	require.Contains(t, out, "cannot convert string to int64 in column 'v'")
	require.NotContains(t, out, "the other points were written")
	rows, _ := r.stored(t)
	require.Empty(t, rows)
}

// In a batch, the record is dropped and the others are written: the partial
// write the row format and line protocol answer.
func TestMsgPackColumnarTypeConflictInABatchIsAPartialWrite(t *testing.T) {
	r, post := newMsgPackRowsRig(t)
	status, out := post([]interface{}{
		columnarPayload("cpu", map[string]interface{}{
			"time": []int64{1700000000000, 1700000001000},
			"v":    []float64{1.5, 2.5},
		}),
		columnarPayload("mem", map[string]interface{}{
			"time": []int64{1700000000000, 1700000001000, 1700000002000},
			"free": []interface{}{10, 11, "oops"},
		}),
	})
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 3 points dropped")
	require.Contains(t, out, "the other points were written")
	require.Contains(t, out, "cannot convert string to int64 in column 'free'")
	rows, _ := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 2}, rows)
	require.Contains(t, r.log.String(), "MessagePack points dropped (partial write)")
	require.NotContains(t, r.log.String(), "Failed to write to Arrow buffer")
}

// Line protocol's record path (decimal deployments) runs the same conversion:
// a value outside the decimal column's precision is a dropped point there too,
// not a 500.
func TestLPWriteDecimalOutOfPrecisionIsAPartialWrite(t *testing.T) {
	r := newLPWriteRig(t, &config.IngestConfig{DecimalColumns: []string{"trades:price=18,8"}})
	status, out := r.post(t, "cpu v=1 1700000000000000000\ntrades price=12345678901234.5 1700000000000000000\n")
	require.Equal(t, http.StatusBadRequest, status, out)
	require.Contains(t, out, "partial write: 1 points dropped")
	require.Contains(t, out, "decimal conversion error in column 'price'")
	require.Contains(t, out, "the other points were written")
	rows, _ := r.stored(t)
	require.Equal(t, map[string]int64{"cpu": 1}, rows)
}
