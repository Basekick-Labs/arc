package ingest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The WAL size helpers must add up to what encodeTypedWALRows writes, header
// widths included: a row with 16+ cells, strings past 31 and 255 bytes, an
// entry of 16+ rows, and NULL cells left out.
func TestWALRowSizesMatchEncoder(t *testing.T) {
	long := strings.Repeat("x", 300)
	mid := strings.Repeat("y", 40)
	for _, tc := range []struct {
		name string
		rows int
		tags int
	}{{"small", 1, 2}, {"wide", 3, 20}, {"many rows", 70000, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			db, m := "prod", strings.Repeat("m", 40)
			batch := &TypedColumnBatch{Data: map[string]interface{}{}, Validity: map[string][]bool{}}
			times := make([]int64, tc.rows)
			values := make([]float64, tc.rows)
			batch.Data["time"], batch.Data["value"] = times, values
			want := WALRowsHeaderBytes(tc.rows)
			rowCells := make([]int, tc.rows)
			rowBytes := make([]int, tc.rows)
			for i := range rowCells {
				rowCells[i] = 2
				rowBytes[i] = WALStringBytes("time") + WALNumberBytes + WALStringBytes("value") + WALNumberBytes
			}
			for k := 0; k < tc.tags; k++ {
				name := "tag" + strings.Repeat("n", k)
				col := make([]string, tc.rows)
				valid := make([]bool, tc.rows)
				for i := range col {
					col[i] = []string{"a", mid, long}[(i+k)%3]
					valid[i] = (i+k)%4 != 0
					if valid[i] {
						rowCells[i]++
						rowBytes[i] += WALStringBytes(name) + WALStringBytes(col[i])
					}
				}
				batch.Data[name] = col
				batch.Validity[name] = valid
			}
			for i := range rowCells {
				want += WALRowBytes(db, m, rowCells[i], rowBytes[i])
			}
			got, err := encodeTypedWALRows(db, m, batch, tc.rows, nil)
			require.NoError(t, err)
			require.Equal(t, len(got), want)
		})
	}
}
