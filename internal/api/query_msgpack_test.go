//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// TestArrowMsgPackQueryFunc_RegisteredWithTag asserts the build-tag init
// hook ran and the dispatch symbol is set. The companion non-tag test in
// query_msgpack_no_tag_test.go asserts the inverse — that
// arrowMsgPackQueryFunc is nil when compiled without duckdb_arrow.
func TestArrowMsgPackQueryFunc_RegisteredWithTag(t *testing.T) {
	if arrowMsgPackQueryFunc == nil {
		t.Fatal("arrowMsgPackQueryFunc must be non-nil when built with -tags=duckdb_arrow")
	}
}

// msgpackStreamToBytes runs the columnar streamer end-to-end (drain +
// encode) against the provided reader and returns the encoded msgpack
// bytes plus the rowCount the streamer reported. Mirrors the production
// pipeline: drainArrowBatches first (sync, can fail), then
// streamMsgPackFromBatches (encode-only). Tests that exercise drain
// errors should call drainArrowBatches directly.
func msgpackStreamToBytes(
	reader array.RecordReader,
	governanceMaxRows int,
) ([]byte, int) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	start := time.Now()
	ctx := context.Background()
	schema := reader.Schema()
	batches, rowCount, drainErr := drainArrowBatches(ctx, reader, governanceMaxRows)
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	if drainErr != nil {
		w.Flush()
		return buf.Bytes(), rowCount
	}
	rc, _ := streamMsgPackFromBatches(ctx, w, schema, batches, rowCount, nil, start, "2024-01-15T12:00:00Z")
	w.Flush()
	return buf.Bytes(), rc
}

func decodeMsgpack(t *testing.T, data []byte) map[string]interface{} {
	t.Helper()
	var out map[string]interface{}
	if err := msgpack.Unmarshal(data, &out); err != nil {
		t.Fatalf("invalid msgpack: %v\nbytes: %x", err, data)
	}
	return out
}

// colsOf returns the per-column slices from a decoded columnar response.
// data is the outer array (length numCols); each element is a per-column
// array (length numRows). Callers use cols[colIdx][rowIdx].
func colsOf(t *testing.T, result map[string]interface{}) [][]interface{} {
	t.Helper()
	outer, ok := result["data"].([]interface{})
	if !ok {
		t.Fatalf("expected 'data' to be an array, got %T", result["data"])
	}
	cols := make([][]interface{}, len(outer))
	for i, col := range outer {
		inner, ok := col.([]interface{})
		if !ok {
			t.Fatalf("expected data[%d] to be an array, got %T", i, col)
		}
		cols[i] = inner
	}
	return cols
}

func TestStreamArrowMsgPack_BasicTypes(t *testing.T) {
	alloc := memory.NewGoAllocator()
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		{Name: "host", Type: arrow.BinaryTypes.String},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean},
		{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us},
	}, nil)

	batch := buildArrowBatch(alloc, schema, [][]interface{}{
		{int64(42), 72.5, "server1", true, ts},
		{int64(43), 73.1, "server2", false, ts.Add(time.Hour)},
	})

	reader := newSimpleRecordReader(schema, []arrow.Record{batch})
	data, rowCount := msgpackStreamToBytes(reader, 0)

	if rowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", rowCount)
	}

	result := decodeMsgpack(t, data)

	if result["success"] != true {
		t.Errorf("expected success=true, got %v", result["success"])
	}
	if !equalsUint(result["row_count"], 2) {
		t.Errorf("expected row_count=2, got %v (%T)", result["row_count"], result["row_count"])
	}

	colNames := result["columns"].([]interface{})
	if len(colNames) != 5 {
		t.Fatalf("expected 5 column names, got %d", len(colNames))
	}
	typeNames := result["types"].([]interface{})
	if len(typeNames) != 5 {
		t.Fatalf("expected 5 type names, got %d", len(typeNames))
	}
	// Spot-check a few type names — the streamer emits arrow.DataType.String().
	if typeNames[0].(string) != "int64" {
		t.Errorf("expected types[0]='int64', got %v", typeNames[0])
	}
	if typeNames[3].(string) != "bool" {
		t.Errorf("expected types[3]='bool', got %v", typeNames[3])
	}

	cols := colsOf(t, result)
	if len(cols) != 5 {
		t.Fatalf("expected 5 column arrays in data, got %d", len(cols))
	}
	// Verify per-column lengths.
	for i, col := range cols {
		if len(col) != 2 {
			t.Fatalf("expected col %d to have 2 values, got %d", i, len(col))
		}
	}

	// id column: int64
	if !equalsInt(cols[0][0], 42) {
		t.Errorf("cols[0][0] expected 42, got %v (%T)", cols[0][0], cols[0][0])
	}
	if !equalsInt(cols[0][1], 43) {
		t.Errorf("cols[0][1] expected 43, got %v (%T)", cols[0][1], cols[0][1])
	}
	// value column: float64
	if cols[1][0].(float64) != 72.5 {
		t.Errorf("cols[1][0] expected 72.5, got %v", cols[1][0])
	}
	// host column: string
	if cols[2][0].(string) != "server1" {
		t.Errorf("cols[2][0] expected 'server1', got %v", cols[2][0])
	}
	// active column: bool
	if cols[3][0].(bool) != true {
		t.Errorf("cols[3][0] expected true, got %v", cols[3][0])
	}
	// time column: msgpack timestamp ext → decoded as time.Time
	if _, ok := cols[4][0].(time.Time); !ok {
		t.Errorf("cols[4][0] expected time.Time (msgpack timestamp ext), got %T (%v)", cols[4][0], cols[4][0])
	}
}

func TestStreamArrowMsgPack_NullValues(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Float64},
		{Name: "c", Type: arrow.BinaryTypes.String},
	}, nil)

	b0 := array.NewInt64Builder(alloc)
	b0.AppendNull()
	b0.Append(1)

	b1 := array.NewFloat64Builder(alloc)
	b1.AppendNull()
	b1.Append(2.0)

	b2 := array.NewStringBuilder(alloc)
	b2.AppendNull()
	b2.Append("hello")

	cols := []arrow.Array{b0.NewArray(), b1.NewArray(), b2.NewArray()}
	batch := array.NewRecord(schema, cols, 2)

	reader := newSimpleRecordReader(schema, []arrow.Record{batch})
	data, rowCount := msgpackStreamToBytes(reader, 0)

	if rowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", rowCount)
	}
	result := decodeMsgpack(t, data)

	dataCols := colsOf(t, result)
	// Row 0 is null in every column.
	for c, col := range dataCols {
		if col[0] != nil {
			t.Errorf("cols[%d][0] expected nil, got %v (%T)", c, col[0], col[0])
		}
	}
	// Row 1 has values.
	if !equalsInt(dataCols[0][1], 1) {
		t.Errorf("cols[0][1] expected 1, got %v", dataCols[0][1])
	}
	if dataCols[2][1].(string) != "hello" {
		t.Errorf("cols[2][1] expected 'hello', got %v", dataCols[2][1])
	}
}

func TestStreamArrowMsgPack_EmptyResult(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	reader := newSimpleRecordReader(schema, []arrow.Record{})
	data, rowCount := msgpackStreamToBytes(reader, 0)

	if rowCount != 0 {
		t.Fatalf("expected 0 rows, got %d", rowCount)
	}
	result := decodeMsgpack(t, data)

	// Columnar: data is array-of-columns. Empty result still has
	// numCols outer entries, each an empty array.
	cols := colsOf(t, result)
	if len(cols) != 2 {
		t.Fatalf("expected 2 column arrays (one per schema field), got %d", len(cols))
	}
	for c, col := range cols {
		if len(col) != 0 {
			t.Errorf("col %d expected empty (0-row), got %d values", c, len(col))
		}
	}
	colNames := result["columns"].([]interface{})
	if len(colNames) != 2 {
		t.Errorf("expected 2 column names even for empty result, got %d", len(colNames))
	}
}

func TestStreamArrowMsgPack_GovernanceMaxRows(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	rows := make([][]interface{}, 100)
	for i := range rows {
		rows[i] = []interface{}{int64(i)}
	}

	batch := buildArrowBatch(alloc, schema, rows)
	reader := newSimpleRecordReader(schema, []arrow.Record{batch})

	data, rowCount := msgpackStreamToBytes(reader, 10)
	if rowCount != 10 {
		t.Fatalf("expected 10 rows after governance cap, got %d", rowCount)
	}
	result := decodeMsgpack(t, data)
	cols := colsOf(t, result)
	if len(cols[0]) != 10 {
		t.Errorf("expected col 0 length 10, got %d", len(cols[0]))
	}
}

func TestStreamArrowMsgPack_BinaryColumnUsesBinType(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "blob", Type: arrow.BinaryTypes.Binary},
	}, nil)

	b := array.NewBinaryBuilder(alloc, arrow.BinaryTypes.Binary)
	b.Append([]byte{0x00, 0x01, 0x02, 0xff})

	batch := array.NewRecord(schema, []arrow.Array{b.NewArray()}, 1)
	reader := newSimpleRecordReader(schema, []arrow.Record{batch})

	data, _ := msgpackStreamToBytes(reader, 0)
	result := decodeMsgpack(t, data)
	cols := colsOf(t, result)

	// msgpack native bin type decodes to []byte in Go; the JSON path
	// would have decoded to a string. This test asserts the columnar
	// path preserves the native bin encoding.
	got, ok := cols[0][0].([]byte)
	if !ok {
		t.Fatalf("expected []byte for Binary column, got %T (%v)", cols[0][0], cols[0][0])
	}
	want := []byte{0x00, 0x01, 0x02, 0xff}
	if !bytes.Equal(got, want) {
		t.Errorf("expected %x, got %x", want, got)
	}
}

// TestStreamArrowMsgPack_GovernanceCapTrimsTrailingBatch verifies that
// when a batch arrives that would push us past the governance cap, the
// batch is trimmed (NewSlice) before retention. The signal is that `data`
// contains EXACTLY the cap, not "the next batch boundary up from the cap".
func TestStreamArrowMsgPack_GovernanceCapTrimsTrailingBatch(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Two batches of 100 rows each. Cap at 150 — the second batch
	// should be trimmed to 50 rows.
	mkBatch := func(off int) arrow.Record {
		rows := make([][]interface{}, 100)
		for i := range rows {
			rows[i] = []interface{}{int64(off + i)}
		}
		return buildArrowBatch(alloc, schema, rows)
	}
	reader := newSimpleRecordReader(schema, []arrow.Record{mkBatch(0), mkBatch(100)})

	data, rowCount := msgpackStreamToBytes(reader, 150)
	if rowCount != 150 {
		t.Fatalf("expected rowCount=150 after trim, got %d", rowCount)
	}
	result := decodeMsgpack(t, data)
	cols := colsOf(t, result)
	if len(cols[0]) != 150 {
		t.Fatalf("expected col 0 to have 150 values, got %d", len(cols[0]))
	}
}

// TestStreamArrowMsgPack_TypesArrayParallelToColumns asserts that the
// types array is parallel to columns — same length, ordered the same.
// Clients use this to interpret each per-column array's elements.
func TestStreamArrowMsgPack_TypesArrayParallelToColumns(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String},
		{Name: "f", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	batch := buildArrowBatch(alloc, schema, [][]interface{}{
		{int64(1), "a", 1.0},
	})
	reader := newSimpleRecordReader(schema, []arrow.Record{batch})

	data, _ := msgpackStreamToBytes(reader, 0)
	result := decodeMsgpack(t, data)

	colNames := result["columns"].([]interface{})
	typeNames := result["types"].([]interface{})
	if len(colNames) != len(typeNames) {
		t.Fatalf("columns and types must be parallel: len(columns)=%d, len(types)=%d", len(colNames), len(typeNames))
	}
	wantTypes := []string{"int64", "utf8", "float64"}
	for i, want := range wantTypes {
		if got := typeNames[i].(string); got != want {
			t.Errorf("types[%d]: expected %q, got %q", i, want, got)
		}
	}
}

// equalsUint accepts the various integer types a msgpack decoder might
// surface and compares to an expected uint64. The Basekick-Labs msgpack
// library picks the smallest fitting integer type at decode time, so a
// uint encoded as fixint (0–127) round-trips as int8, not uint64.
func equalsUint(v interface{}, want uint64) bool {
	switch x := v.(type) {
	case uint64:
		return x == want
	case uint32:
		return uint64(x) == want
	case uint16:
		return uint64(x) == want
	case uint8:
		return uint64(x) == want
	case int64:
		return x >= 0 && uint64(x) == want
	case int32:
		return x >= 0 && uint64(x) == want
	case int16:
		return x >= 0 && uint64(x) == want
	case int8:
		return x >= 0 && uint64(x) == want
	}
	return false
}

// equalsInt accepts the various integer types a msgpack decoder might
// surface and compares to an expected int64.
func equalsInt(v interface{}, want int64) bool {
	switch x := v.(type) {
	case int64:
		return x == want
	case int32:
		return int64(x) == want
	case int16:
		return int64(x) == want
	case int8:
		return int64(x) == want
	case uint64:
		return want >= 0 && x == uint64(want)
	case uint32:
		return want >= 0 && uint64(x) == uint64(want)
	case uint16:
		return want >= 0 && uint64(x) == uint64(want)
	case uint8:
		return want >= 0 && uint64(x) == uint64(want)
	}
	return false
}
