//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"github.com/valyala/fasthttp"
)

type cancelOnRecordReader struct {
	array.RecordReader
	cancel context.CancelFunc
}

func (r *cancelOnRecordReader) RecordBatch() arrow.RecordBatch {
	record := r.RecordReader.RecordBatch()
	r.cancel()
	return record
}

func writeArcxArrowResponse(
	t *testing.T,
	reader array.RecordReader,
	ctx context.Context,
	maxRows int,
	onRows func(int64),
	onDisconnect func(),
	onComplete func(int),
	onFail func(string),
) (*http.Response, []byte) {
	t.Helper()
	var requestCtx fasthttp.RequestCtx
	requestCtx.Response.Header.SetStatusCode(fasthttp.StatusOK)
	h := &QueryHandler{logger: zerolog.Nop()}
	h.serveArcxArrowIPCResult(
		&requestCtx, ctx, nil, reader, maxRows, time.Now(), onRows, onDisconnect, onComplete, onFail,
	)

	var wire bytes.Buffer
	w := bufio.NewWriter(&wire)
	if err := requestCtx.Response.Write(w); err != nil {
		t.Fatalf("write response: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("flush response: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(wire.Bytes())), nil)
	if err != nil {
		t.Fatalf("parse response: %v", err)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Errorf("close response body: %v", closeErr)
		}
		t.Fatalf("read response body: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close response body: %v", err)
	}
	return resp, body
}

func TestServeArcxArrowIPCResultAppliesGovernanceCap(t *testing.T) {
	metrics.Init(zerolog.Nop())
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	records := make([]arrow.Record, 3)
	for batch := range records {
		rows := make([][]interface{}, 100)
		for row := range rows {
			rows[row] = []interface{}{int64(batch*100 + row)}
		}
		records[batch] = buildArrowBatch(alloc, schema, rows)
	}
	reader := newSimpleRecordReader(schema, records)
	defer reader.Release()

	var rowsReported int64
	var completed int
	var failed bool
	resp, body := writeArcxArrowResponse(
		t, reader, context.Background(), 150,
		func(rows int64) { rowsReported = rows },
		nil,
		func(rows int) { completed = rows },
		func(string) { failed = true },
	)
	if rowsReported != 150 || completed != 150 || failed {
		t.Fatalf("callbacks: row count %d, completed %d, failed %v", rowsReported, completed, failed)
	}
	if reader.idx != 1 {
		t.Errorf("reader consumed through batch %d, want 1 (the third batch must remain unread)", reader.idx)
	}
	for _, name := range []string{arrowStreamTruncatedTrailer, arrowExecutionTimeTrailer, arrowRowsCappedTrailer} {
		if _, ok := resp.Trailer[http.CanonicalHeaderKey(name)]; !ok {
			t.Errorf("%s is absent from the response trailers", name)
		}
	}
	if got := resp.Trailer.Get(arrowRowsCappedTrailer); got != "150" {
		t.Errorf("%s = %q, want 150", arrowRowsCappedTrailer, got)
	}
	if got := resp.Trailer.Get(arrowExecutionTimeTrailer); got == "" {
		t.Error("execution-time trailer is empty")
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got != "" {
		t.Errorf("truncation trailer = %q on a valid capped result", got)
	}
	decoded, err := decodeStream(t, body)
	if err != nil {
		t.Fatalf("capped IPC body is not a valid stream: %v", err)
	}
	if decoded != 150 {
		t.Errorf("decoded %d rows, want 150", decoded)
	}
}

func TestServeArcxArrowIPCResultKeepsCapSuccessfulWhenContextExpiresAfterBatch(t *testing.T) {
	metrics.Init(zerolog.Nop())
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rows := make([][]interface{}, 20)
	for i := range rows {
		rows[i] = []interface{}{int64(i)}
	}
	base := newSimpleRecordReader(schema, []arrow.Record{buildArrowBatch(alloc, schema, rows)})
	defer base.Release()
	ctx, cancel := context.WithCancel(context.Background())
	reader := &cancelOnRecordReader{RecordReader: base, cancel: cancel}

	var failed bool
	resp, body := writeArcxArrowResponse(t, reader, ctx, 10, nil, nil, nil, func(string) {
		failed = true
	})
	if failed {
		t.Fatal("context expiry after the capped batch turned a valid result into a failure")
	}
	if got := resp.Trailer.Get(arrowRowsCappedTrailer); got != "10" {
		t.Errorf("%s = %q, want 10", arrowRowsCappedTrailer, got)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got != "" {
		t.Errorf("%s = %q on a valid capped stream", arrowStreamTruncatedTrailer, got)
	}
	if decoded, err := decodeStream(t, body); err != nil || decoded != 10 {
		t.Errorf("decoded rows = %d, error = %v; want 10 rows and a valid stream", decoded, err)
	}
}

func TestServeArcxArrowIPCResultLeavesCapTrailerEmptyWithoutCap(t *testing.T) {
	metrics.Init(zerolog.Nop())
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := buildArrowBatch(alloc, schema, [][]interface{}{{int64(1)}, {int64(2)}})
	reader := newSimpleRecordReader(schema, []arrow.Record{record})
	defer reader.Release()

	resp, body := writeArcxArrowResponse(t, reader, context.Background(), 0, nil, nil, nil, func(string) {
		t.Error("uncapped successful stream reported an error")
	})
	if got := resp.Trailer.Get(arrowRowsCappedTrailer); got != "" {
		t.Errorf("%s = %q without a governance cap, want empty", arrowRowsCappedTrailer, got)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got != "" {
		t.Errorf("%s = %q on a successful stream, want empty", arrowStreamTruncatedTrailer, got)
	}
	if got := resp.Trailer.Get(arrowExecutionTimeTrailer); got == "" {
		t.Error("successful stream has no execution-time trailer")
	}
	if decoded, err := decodeStream(t, body); err != nil || decoded != 2 {
		t.Errorf("decoded rows = %d, error = %v; want 2 rows and a valid stream", decoded, err)
	}
}

func TestServeArcxArrowIPCResultMarksCappedFailureAsTruncated(t *testing.T) {
	metrics.Init(zerolog.Nop())
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	rows := make([][]interface{}, 20)
	for i := range rows {
		rows[i] = []interface{}{int64(i)}
	}
	base := newSimpleRecordReader(schema, []arrow.Record{buildArrowBatch(alloc, schema, rows)})
	reader := &failingRecordReader{simpleRecordReader: base, err: errors.New("reader failed after cap")}
	defer reader.Release()

	var rowsReported int64
	var completed, failed bool
	resp, body := writeArcxArrowResponse(
		t, reader, context.Background(), 10,
		func(rows int64) { rowsReported = rows },
		nil,
		func(int) { completed = true },
		func(string) { failed = true },
	)
	if rowsReported != 10 || completed || !failed {
		t.Fatalf("callbacks: row count %d, completed %v, failed %v", rowsReported, completed, failed)
	}
	if got := resp.Trailer.Get(arrowRowsCappedTrailer); got != "" {
		t.Errorf("failed stream was marked as a valid capped result: %q", got)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got == "" {
		t.Error("failed capped stream has no truncation reason")
	}
	if got := resp.Trailer.Get(arrowExecutionTimeTrailer); got == "" {
		t.Error("failed stream has no execution-time trailer")
	}
	if _, err := decodeStream(t, body); err == nil {
		t.Error("failed capped stream decoded as a complete Arrow result")
	}
}

func TestServeArcxArrowIPCResultMarksCancellationAsTruncated(t *testing.T) {
	metrics.Init(zerolog.Nop())
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	record := buildArrowBatch(alloc, schema, [][]interface{}{{int64(1)}})
	reader := newSimpleRecordReader(schema, []arrow.Record{record})
	defer reader.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var rows int64
	var disconnects, completed, failed int
	resp, body := writeArcxArrowResponse(
		t, reader, ctx, 1,
		func(got int64) { rows = got },
		func() { disconnects++ },
		func(int) { completed++ },
		func(string) { failed++ },
	)
	if rows != 0 || disconnects != 1 || completed != 0 || failed != 1 {
		t.Fatalf("callbacks: rows %d, disconnects %d, completed %d, failed %d", rows, disconnects, completed, failed)
	}
	if got := resp.Trailer.Get(arrowRowsCappedTrailer); got != "" {
		t.Errorf("%s = %q after cancellation before reaching the cap", arrowRowsCappedTrailer, got)
	}
	if got := resp.Trailer.Get(arrowStreamTruncatedTrailer); got == "" {
		t.Error("cancelled response has no truncation trailer")
	}
	if _, err := decodeStream(t, body); err == nil {
		t.Error("cancelled stream decoded as complete")
	}
}
