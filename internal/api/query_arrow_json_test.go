//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/basekick-labs/arc/internal/database"
)

// buildArrowBatch creates an Arrow record batch from Go slices for testing.
func buildArrowBatch(
	alloc memory.Allocator,
	schema *arrow.Schema,
	rows [][]interface{},
) arrow.Record {
	numCols := len(schema.Fields())
	builders := make([]array.Builder, numCols)
	for i, f := range schema.Fields() {
		builders[i] = array.NewBuilder(alloc, f.Type)
	}

	for _, row := range rows {
		for col, val := range row {
			if val == nil {
				builders[col].AppendNull()
				continue
			}
			switch b := builders[col].(type) {
			case *array.Int64Builder:
				b.Append(val.(int64))
			case *array.Float64Builder:
				b.Append(val.(float64))
			case *array.StringBuilder:
				b.Append(val.(string))
			case *array.BooleanBuilder:
				b.Append(val.(bool))
			case *array.TimestampBuilder:
				t := val.(time.Time)
				b.Append(arrow.Timestamp(t.UnixMicro()))
			}
		}
	}

	cols := make([]arrow.Array, numCols)
	for i, b := range builders {
		cols[i] = b.NewArray()
	}

	return array.NewRecord(schema, cols, int64(len(rows)))
}

// simpleRecordReader wraps a single record batch as an array.RecordReader.
type simpleRecordReader struct {
	schema  *arrow.Schema
	records []arrow.Record
	idx     int
	refCnt  int
}

func newSimpleRecordReader(schema *arrow.Schema, records []arrow.Record) *simpleRecordReader {
	return &simpleRecordReader{schema: schema, records: records, idx: -1, refCnt: 1}
}

func (r *simpleRecordReader) Schema() *arrow.Schema          { return r.schema }
func (r *simpleRecordReader) Next() bool                     { r.idx++; return r.idx < len(r.records) }
func (r *simpleRecordReader) Record() arrow.Record           { return r.records[r.idx] }
func (r *simpleRecordReader) RecordBatch() arrow.RecordBatch { return r.records[r.idx] }
func (r *simpleRecordReader) Err() error                     { return nil }
func (r *simpleRecordReader) Retain()                        { r.refCnt++ }
func (r *simpleRecordReader) Release() {
	r.refCnt--
	if r.refCnt == 0 {
		for _, rec := range r.records {
			rec.Release()
		}
	}
}

func arrowStreamToBytes(
	reader array.RecordReader,
	governanceMaxRows int,
	profile *database.QueryProfile,
) ([]byte, int) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	start := time.Now()
	rowCount, _ := streamArrowJSON(context.Background(), w, reader, governanceMaxRows, profile, start, "2024-01-15T12:00:00Z")
	w.Flush()
	return buf.Bytes(), rowCount
}

func TestStreamArrowJSON_BasicTypes(t *testing.T) {
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
	data, rowCount := arrowStreamToBytes(reader, 0, nil)

	if rowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", rowCount)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, string(data))
	}

	if result["success"] != true {
		t.Errorf("expected success=true, got %v", result["success"])
	}
	if result["row_count"].(float64) != 2 {
		t.Errorf("expected row_count=2, got %v", result["row_count"])
	}

	cols := result["columns"].([]interface{})
	if len(cols) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(cols))
	}

	rows := result["data"].([]interface{})
	if len(rows) != 2 {
		t.Fatalf("expected 2 data rows, got %d", len(rows))
	}

	row0 := rows[0].([]interface{})
	if row0[0].(float64) != 42 {
		t.Errorf("row[0][0] expected 42, got %v", row0[0])
	}
	if row0[1].(float64) != 72.5 {
		t.Errorf("row[0][1] expected 72.5, got %v", row0[1])
	}
	if row0[2].(string) != "server1" {
		t.Errorf("row[0][2] expected 'server1', got %v", row0[2])
	}
	if row0[3].(bool) != true {
		t.Errorf("row[0][3] expected true, got %v", row0[3])
	}
}

func TestStreamArrowJSON_NullValues(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64},
		{Name: "b", Type: arrow.PrimitiveTypes.Float64},
		{Name: "c", Type: arrow.BinaryTypes.String},
	}, nil)

	// Build a batch with nulls manually
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
	data, rowCount := arrowStreamToBytes(reader, 0, nil)

	if rowCount != 2 {
		t.Fatalf("expected 2 rows, got %d", rowCount)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	rows := result["data"].([]interface{})
	row0 := rows[0].([]interface{})
	for i, v := range row0 {
		if v != nil {
			t.Errorf("row[0][%d] expected null, got %v", i, v)
		}
	}
}

func TestStreamArrowJSON_EmptyResult(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_us},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	reader := newSimpleRecordReader(schema, []arrow.Record{})
	data, rowCount := arrowStreamToBytes(reader, 0, nil)

	if rowCount != 0 {
		t.Fatalf("expected 0 rows, got %d", rowCount)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	rows := result["data"].([]interface{})
	if len(rows) != 0 {
		t.Errorf("expected empty data array, got %d items", len(rows))
	}
}

func TestStreamArrowJSON_GovernanceMaxRows(t *testing.T) {
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

	_, rowCount := arrowStreamToBytes(reader, 10, nil)
	if rowCount != 10 {
		t.Fatalf("expected 10 rows (governance limit), got %d", rowCount)
	}
}

func TestStreamArrowJSON_WithProfile(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	batch := buildArrowBatch(alloc, schema, [][]interface{}{
		{int64(1)},
	})

	profile := &database.QueryProfile{
		TotalMs:     100.5,
		PlannerMs:   10.2,
		ExecutionMs: 90.3,
		RowsScanned: 50000,
		Latency:     100.0,
	}

	reader := newSimpleRecordReader(schema, []arrow.Record{batch})
	data, _ := arrowStreamToBytes(reader, 0, profile)

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, string(data))
	}

	p, ok := result["profile"].(map[string]interface{})
	if !ok {
		t.Fatal("expected profile in response")
	}
	if p["total_ms"].(float64) != 100.5 {
		t.Errorf("expected total_ms=100.5, got %v", p["total_ms"])
	}
}

func TestStreamArrowJSON_SpecialChars(t *testing.T) {
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "msg", Type: arrow.BinaryTypes.String},
	}, nil)

	batch := buildArrowBatch(alloc, schema, [][]interface{}{
		{`hello "world"`},
		{`back\slash`},
		{"line\nbreak"},
		{"tab\there"},
	})

	reader := newSimpleRecordReader(schema, []arrow.Record{batch})
	data, rowCount := arrowStreamToBytes(reader, 0, nil)

	if rowCount != 4 {
		t.Fatalf("expected 4 rows, got %d", rowCount)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid JSON: %v\noutput: %s", err, string(data))
	}

	rows := result["data"].([]interface{})
	expected := []string{
		`hello "world"`,
		`back\slash`,
		"line\nbreak",
		"tab\there",
	}
	for i, exp := range expected {
		row := rows[i].([]interface{})
		if row[0].(string) != exp {
			t.Errorf("row[%d] expected %q, got %q", i, exp, row[0])
		}
	}
}

// errAfterNBytes is an io.Writer that succeeds for the first n bytes then
// returns an error. Models a TCP connection that the client closed mid-stream.
type errAfterNBytes struct {
	limit int
	wrote int
	err   error
}

func (e *errAfterNBytes) Write(p []byte) (int, error) {
	if e.wrote >= e.limit {
		return 0, e.err
	}
	remaining := e.limit - e.wrote
	if len(p) <= remaining {
		e.wrote += len(p)
		return len(p), nil
	}
	e.wrote = e.limit
	return remaining, e.err
}

// TestStreamArrowJSON_FlushErrorBreaksLoop verifies that when the underlying
// connection fails (client disconnect mid-stream), streamArrowJSON stops
// iterating record batches instead of draining the entire result set into a
// buffer nobody is reading. Regression test for the fasthttp-RequestCtx.Done
// limitation: that channel only fires on server shutdown, so Flush errors are
// our only signal that the client has gone away.
func TestStreamArrowJSON_FlushErrorBreaksLoop(t *testing.T) {
	// Note: this test uses NewGoAllocator (not CheckedAllocator) to match
	// the rest of the test file. The buildArrowBatch helper creates Arrow
	// builders that aren't explicitly Released — CheckedAllocator would
	// flag those as leaks, but they're pre-existing and out of scope for
	// the disconnect-handling fix this test verifies. The actual code path
	// being tested (streamArrowJSON's break-on-Flush-error) does not itself
	// retain any Arrow memory beyond what simpleRecordReader.Release frees.
	alloc := memory.NewGoAllocator()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	// Need more than jsonFlushInterval rows total so the explicit Flush()
	// fires at least once. The errAfterNBytes failure point (256 bytes)
	// makes the failure happen sometime during the first batch, but
	// bufio's internal 4 KiB buffer absorbs the sticky error until the
	// next per-jsonFlushInterval Flush() returns it. With jsonFlushInterval
	// = 1000 (see query_json_writer.go), 6 × 1024 = 6144 rows is plenty.
	const (
		batchRows  = 1024
		numBatches = 6
	)
	records := make([]arrow.Record, numBatches)
	for b := 0; b < numBatches; b++ {
		rows := make([][]interface{}, batchRows)
		for i := range rows {
			rows[i] = []interface{}{int64(b*batchRows + i)}
		}
		records[b] = buildArrowBatch(alloc, schema, rows)
	}
	reader := newSimpleRecordReader(schema, records)
	defer reader.Release()

	sentinel := errors.New("client disconnected")
	failingWriter := &errAfterNBytes{limit: 256, err: sentinel}
	w := bufio.NewWriter(failingWriter)

	rowCount, err := streamArrowJSON(
		context.Background(), w, reader, 0, nil,
		time.Now(), "2024-01-15T12:00:00Z",
	)

	if err == nil {
		t.Fatalf("expected streamArrowJSON to return an error on client disconnect; got nil (rows=%d)", rowCount)
	}
	// The underlying I/O sentinel must be in the error chain so future
	// debugging can identify the actual cause.
	if !errors.Is(err, sentinel) {
		t.Errorf("expected error to wrap underlying I/O sentinel %v, got %v", sentinel, err)
	}
	// The errClientDisconnected sentinel must also be in the error chain
	// so callers can use errors.Is to disambiguate this from genuine
	// server-side stream failures (and log at Warn instead of Error).
	if !errors.Is(err, errClientDisconnected) {
		t.Errorf("expected error to wrap errClientDisconnected, got %v", err)
	}

	// The break must fire at the FIRST explicit Flush after the writer's
	// 256-byte limit is exceeded. bufio's internal 4 KiB auto-flush hits
	// the failing writer well before row jsonFlushInterval, sets the
	// sticky error, and subsequent WriteByte/WriteString calls silently
	// no-op. At row jsonFlushInterval the explicit Flush() returns the
	// stored error and the loop breaks. So rowCount should be EXACTLY
	// jsonFlushInterval.
	if rowCount != jsonFlushInterval {
		t.Errorf("loop did not break exactly at flush boundary: emitted %d rows, expected %d", rowCount, jsonFlushInterval)
	}
}
