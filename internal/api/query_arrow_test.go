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
		context.Background(), w, reader, schema, nil, false, "", zerolog.Nop(),
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
		ctx, bufio.NewWriter(&output), reader, schema, nil, false, "", zerolog.Nop(),
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
		context.Background(), bufio.NewWriter(&output), reader, schema, nil, false, "", zerolog.Nop(),
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
		context.Background(), w, reader, castInfo.schema, castInfo, false, "", zerolog.Nop(),
	)
	reader.Release()

	if !errors.Is(err, sentinel) {
		t.Fatalf("expected error to wrap %v, got %v", sentinel, err)
	}
	if rows != 1 {
		t.Fatalf("expected one casted row before the flush failure, got %d", rows)
	}
}
