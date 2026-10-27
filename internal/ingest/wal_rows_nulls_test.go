package ingest

import (
	"bytes"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/decimal128"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/stretchr/testify/require"
)

// typedBatchToWALRecords decodes encodeTypedWALRows' entry, as WAL replay
// reads it.
func typedBatchToWALRecords(database, measurement string, batch *TypedColumnBatch, numRecords int, decimalCols map[string]config.DecimalSpec) []map[string]interface{} {
	payload, err := encodeTypedWALRows(database, measurement, batch, numRecords, decimalCols)
	if err != nil {
		panic(err)
	}
	if payload == nil {
		return nil
	}
	var rows []map[string]interface{}
	if err := msgpack.Unmarshal(payload, &rows); err != nil {
		panic(err)
	}
	return rows
}

// The time column is never left out of a WAL row, even if a Validity entry
// marks it: a row without time is a poison entry on replay.
func TestTypedBatchToWALRecordsKeepsTime(t *testing.T) {
	batch := &TypedColumnBatch{
		Data:     map[string]interface{}{"time": []int64{5, 6}, "v": []int64{1, 2}},
		Validity: map[string][]bool{"time": {false, false}, "v": {false, true}},
	}
	rows := typedBatchToWALRecords("d", "m", batch, 2, nil)
	require.Equal(t, int64(5), rows[0]["time"])
	_, has := rows[0]["v"]
	require.False(t, has, "a NULL cell was written")
	require.Equal(t, int64(2), rows[1]["v"])
	require.Equal(t, "d", rows[0]["_database"])
	require.Equal(t, "m", rows[0]["_measurement"])
}

// A user column cannot replace the target keys of a WAL row.
func TestWALRowTargetKeysWin(t *testing.T) {
	batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{5}, "_database": []string{"evil"}}}
	rows := typedBatchToWALRecords("d", "m", batch, 1, nil)
	db, m := WALRowTarget(rows[0])
	require.Equal(t, "d", db)
	require.Equal(t, "m", m)

	// One key each: the user column is left out, not shadowed by a later
	// duplicate (which a decoder may resolve either way).
	payload, err := encodeTypedWALRows("d", "m", batch, 1, nil)
	require.NoError(t, err)
	dec := msgpack.NewDecoder(bytes.NewReader(payload))
	n, err := dec.DecodeArrayLen()
	require.NoError(t, err)
	require.Equal(t, 1, n)
	keys, err := dec.DecodeMapLen()
	require.NoError(t, err)
	require.Equal(t, len(rows[0]), keys, "a WAL row has duplicate keys")
}

// Rows from older clients name their target with "measurement" / "database"
// (no "_measurement"): those keys stay targets there.
func TestWALRowsLegacyTargetKeys(t *testing.T) {
	db, m, cols, err := WALRowsToColumns([]map[string]interface{}{
		{"measurement": "cpu", "database": "prod", "time": int64(1), "v": nil},
		{"measurement": "cpu", "database": "prod", "time": int64(2), "v": int64(3)},
	})
	require.NoError(t, err)
	require.Equal(t, "prod", db)
	require.Equal(t, "cpu", m)
	_, has := cols["measurement"]
	require.False(t, has)
	require.Equal(t, []interface{}{nil, int64(3)}, cols["v"])
}

// A NULL cell is left out of a columnar WAL row too.
func TestColumnarToWALRecordsOmitsNulls(t *testing.T) {
	b := &ArrowBuffer{}
	rows := b.columnarToWALRecords("d", &models.ColumnarRecord{Measurement: "m", Columns: map[string][]interface{}{
		"time": {int64(1), int64(2)}, "v": {nil, int64(3)},
	}})
	_, has := rows[0]["v"]
	require.False(t, has)
	require.Equal(t, int64(3), rows[1]["v"])
}

// A column NULL in every row keeps explicit nils (replay writes it as the
// live write did, #337); a sparse column leaves its NULL cells out.
func TestWALNullEncodingAllNullVsSparse(t *testing.T) {
	b := &ArrowBuffer{}
	rows := b.columnarToWALRecords("d", &models.ColumnarRecord{Measurement: "m", Columns: map[string][]interface{}{
		"time": {int64(1), int64(2)}, "sparse": {nil, int64(3)}, "empty": {nil, nil},
	}})
	_, has := rows[0]["sparse"]
	require.False(t, has, "a sparse column's NULL cell was written")
	v, has := rows[0]["empty"]
	require.True(t, has, "an all-NULL column was dropped")
	require.Nil(t, v)

	_, _, cols, err := WALRowsToColumns(rows)
	require.NoError(t, err)
	require.Equal(t, []interface{}{nil, nil}, cols["empty"])
	require.Equal(t, []interface{}{nil, int64(3)}, cols["sparse"])

	// A typed entry leaves even an all-NULL column out: its live write kept
	// the column's type, and explicit nils would replay as a string column.
	typed := typedBatchToWALRecords("d", "m", &TypedColumnBatch{
		Data:     map[string]interface{}{"time": []int64{1, 2}, "sparse": []int64{0, 3}, "empty": []int64{0, 0}},
		Validity: map[string][]bool{"sparse": {false, true}, "empty": {false, false}},
	}, 2, nil)
	_, has = typed[0]["sparse"]
	require.False(t, has)
	_, has = typed[1]["empty"]
	require.False(t, has, "a typed all-NULL column was written as nils")
}

// In an Arc row (it has "_measurement") a "database" key is a column, even
// without "_database".
func TestWALRowDatabaseColumnIsAColumn(t *testing.T) {
	db, m, cols, err := WALRowsToColumns([]map[string]interface{}{
		{"_measurement": "cpu", "time": int64(1), "database": "prod"},
	})
	require.NoError(t, err)
	require.Equal(t, "default", db)
	require.Equal(t, "cpu", m)
	require.Equal(t, []interface{}{"prod"}, cols["database"])
}

// The typed WAL entry decodes as msgpack.Marshal of one map per row decoded
// (the form it replaced): every type, NULLs, decimals, a target-key column.
func TestEncodeTypedWALRowsMatchesRowMaps(t *testing.T) {
	dec := decimal128.FromI64(12345)
	batch := &TypedColumnBatch{
		Data: map[string]interface{}{
			"time":      []int64{1, 2, 3},
			"i":         []int64{-1, 0, math.MaxInt64},
			"f":         []float64{1.5, math.Inf(1), -0.0},
			"s":         []string{"", "x", "héllo"},
			"b":         []bool{true, false, true},
			"d":         []decimal128.Num{dec, dec, dec},
			"_database": []string{"evil", "evil", "evil"},
		},
		Validity: map[string][]bool{"i": {true, false, true}, "s": {false, false, false}},
	}
	decimals := map[string]config.DecimalSpec{"d": {Scale: 2}}
	want := make([]map[string]interface{}, 3)
	for i := range want {
		row := map[string]interface{}{
			"time": batch.Data["time"].([]int64)[i],
			"f":    batch.Data["f"].([]float64)[i],
			"b":    batch.Data["b"].([]bool)[i],
			"d":    123.45,
		}
		if i != 1 {
			row["i"] = batch.Data["i"].([]int64)[i]
		}
		row["_database"] = "d"
		row["_measurement"] = "m"
		want[i] = row
	}
	raw, err := msgpack.Marshal(want)
	require.NoError(t, err)
	var wantRows []map[string]interface{}
	require.NoError(t, msgpack.Unmarshal(raw, &wantRows))
	require.Equal(t, wantRows, typedBatchToWALRecords("d", "m", batch, 3, decimals))
}
