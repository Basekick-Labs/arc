//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

type errorRecordReader struct {
	*simpleRecordReader
	err error
}

func (r *errorRecordReader) Err() error { return r.err }

func TestStreamArrowIPCFlushErrorBreaksLoop(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	records := make([]arrow.Record, 3)
	for batch := range records {
		rows := make([][]interface{}, 128)
		for row := range rows {
			rows[row] = []interface{}{int64(batch*len(rows) + row)}
		}
		records[batch] = buildArrowBatch(alloc, schema, rows)
	}
	reader := newSimpleRecordReader(schema, records)

	sentinel := errors.New("client disconnected")
	failingWriter := &errAfterNBytes{limit: 256, err: sentinel}
	w := bufio.NewWriterSize(failingWriter, 1<<20)

	rows, err := streamArrowIPC(
		context.Background(), w, reader, schema, nil, false, "", 0, zerolog.Nop(),
	)
	reader.Release()

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected error to wrap %v, got %v", sentinel, err)
	}
	if !errors.Is(err, errClientDisconnected) {
		t.Fatalf("expected error to wrap errClientDisconnected, got %v", err)
	}
	if rows != 128 {
		t.Fatalf("expected the first flush to stop at 128 rows, got %d", rows)
	}
	if reader.idx != 0 {
		t.Fatalf("expected one record to be consumed, reader index is %d", reader.idx)
	}
}

func TestStreamArrowIPCCancelledContextStopsBeforeWritingBatch(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := buildArrowBatch(memory.NewGoAllocator(), schema, [][]interface{}{{int64(1)}})
	reader := newSimpleRecordReader(schema, []arrow.Record{record})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var output bytes.Buffer
	rows, err := streamArrowIPC(
		ctx, bufio.NewWriter(&output), reader, schema, nil, false, "", 0, zerolog.Nop(),
	)
	reader.Release()

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if rows != 0 {
		t.Fatalf("expected no rows to be written, got %d", rows)
	}
}

func TestStreamArrowIPCSurfacesReaderError(t *testing.T) {
	sentinel := errors.New("reader failed")
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	reader := &errorRecordReader{
		simpleRecordReader: newSimpleRecordReader(schema, nil),
		err:                sentinel,
	}

	var output bytes.Buffer
	rows, err := streamArrowIPC(
		context.Background(), bufio.NewWriter(&output), reader, schema, nil, false, "", 0, zerolog.Nop(),
	)
	reader.Release()

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected error to wrap %v, got %v", sentinel, err)
	}
	if rows != 0 {
		t.Fatalf("expected no rows to be written, got %d", rows)
	}
}

func TestStreamArrowIPCReleasesCastedBatchOnFlushError(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	previousAllocator := memory.DefaultAllocator
	memory.DefaultAllocator = alloc
	defer func() { memory.DefaultAllocator = previousAllocator }()
	defer alloc.AssertSize(t, 0)

	decimalType := &arrow.Decimal128Type{Precision: 10, Scale: 2}
	sourceSchema := arrow.NewSchema([]arrow.Field{{Name: "price", Type: decimalType}}, nil)
	builder := array.NewDecimal128Builder(alloc, decimalType)
	builder.Append(decimal128.FromI64(123))
	values := builder.NewArray()
	builder.Release()
	record := array.NewRecord(sourceSchema, []arrow.Array{values}, 1)
	values.Release()
	reader := newSimpleRecordReader(sourceSchema, []arrow.Record{record})

	castInfo := normalizeDecimalSchema(sourceSchema)
	sentinel := errors.New("client disconnected")
	failingWriter := &errAfterNBytes{limit: 64, err: sentinel}
	w := bufio.NewWriterSize(failingWriter, 1<<20)

	rows, err := streamArrowIPC(
		context.Background(), w, reader, castInfo.schema, castInfo, false, "", 0, zerolog.Nop(),
	)
	reader.Release()

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected error to wrap %v, got %v", sentinel, err)
	}
	if rows != 1 {
		t.Fatalf("expected one casted row before the flush failure, got %d", rows)
	}
}

// TestStreamArrowIPCGovernanceRowCap covers the #702 row cap on the IPC path:
// the batch that crosses maxRows is sliced at the boundary, the stream stops
// there instead of draining the rest, and the sliced record is released (the
// checked allocator fails the test if it is not).
//
// Releases are deferred rather than called after the assertions so a failing
// assertion reports the cap problem instead of surfacing as allocator leak
// noise and an AssertSize panic on the way out.
func TestStreamArrowIPCGovernanceRowCap(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	records := make([]arrow.Record, 3)
	for batch := range records {
		rows := make([][]interface{}, 100)
		for row := range rows {
			rows[row] = []interface{}{int64(batch*len(rows) + row)}
		}
		records[batch] = buildArrowBatch(alloc, schema, rows)
	}
	reader := newSimpleRecordReader(schema, records)
	defer reader.Release()

	var output bytes.Buffer
	// 150 splits the second batch, so the cap exercises the slice path.
	rows, err := streamArrowIPC(
		context.Background(), bufio.NewWriter(&output), reader, schema, nil, false, "", 150, zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("streamArrowIPC() error = %v", err)
	}
	if rows != 150 {
		t.Errorf("rows streamed = %d, want the cap of 150", rows)
	}
	// The third batch must never be consumed: the loop stops at the cap
	// rather than materializing a batch it would only discard.
	if reader.idx != 1 {
		t.Errorf("reader consumed up to index %d, want 1 (third batch untouched)", reader.idx)
	}

	// The emitted stream must actually carry the capped rows, in order, with
	// no slice-offset corruption.
	ipcReader, err := ipc.NewReader(&output, ipc.WithAllocator(alloc))
	if err != nil {
		t.Fatalf("ipc.NewReader() error = %v", err)
	}
	defer ipcReader.Release()
	var decoded int64
	for ipcReader.Next() {
		rec := ipcReader.Record()
		col := rec.Column(0).(*array.Int64)
		for i := 0; i < col.Len(); i++ {
			if col.Value(i) != decoded {
				t.Fatalf("row %d has id %d, want %d (slice offset corruption)", decoded, col.Value(i), decoded)
			}
			decoded++
		}
	}
	if err := ipcReader.Err(); err != nil {
		t.Fatalf("reading back the IPC stream: %v", err)
	}
	if decoded != 150 {
		t.Errorf("decoded %d rows from the IPC stream, want 150", decoded)
	}
}

// TestStreamArrowIPCGovernanceCapExactBoundary pins the off-by-one edge: a cap
// landing exactly on a batch boundary stops there, consuming whole batches and
// leaving the next one untouched. reader.idx is what catches a cap comparison
// that is off by one in either direction. Whether the boundary batch is also
// passed through NewSlice is deliberately not asserted: a full-length slice
// would emit identical bytes and is released on the same path, so it is a
// spare allocation rather than a behavior worth pinning.
func TestStreamArrowIPCGovernanceCapExactBoundary(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	records := make([]arrow.Record, 3)
	for batch := range records {
		rows := make([][]interface{}, 50)
		for row := range rows {
			rows[row] = []interface{}{int64(batch*len(rows) + row)}
		}
		records[batch] = buildArrowBatch(alloc, schema, rows)
	}
	reader := newSimpleRecordReader(schema, records)
	defer reader.Release()

	var output bytes.Buffer
	rows, err := streamArrowIPC(
		context.Background(), bufio.NewWriter(&output), reader, schema, nil, false, "", 100, zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("streamArrowIPC() error = %v", err)
	}
	if rows != 100 {
		t.Errorf("rows streamed = %d, want exactly 100", rows)
	}
	// Two full batches consume the cap exactly, so the third is untouched.
	if reader.idx != 1 {
		t.Errorf("reader consumed up to index %d, want 1", reader.idx)
	}

	ipcReader, err := ipc.NewReader(&output, ipc.WithAllocator(alloc))
	if err != nil {
		t.Fatalf("ipc.NewReader() error = %v", err)
	}
	defer ipcReader.Release()
	var decoded int64
	batches := 0
	for ipcReader.Next() {
		batches++
		decoded += ipcReader.Record().NumRows()
	}
	if err := ipcReader.Err(); err != nil {
		t.Fatalf("reading back the IPC stream: %v", err)
	}
	if decoded != 100 {
		t.Errorf("decoded %d rows, want 100", decoded)
	}
	// Two whole batches reach the cap, so the stream must carry exactly two:
	// a third would mean the loop ran past the boundary.
	if batches != 2 {
		t.Errorf("stream carries %d batches, want 2", batches)
	}
}
