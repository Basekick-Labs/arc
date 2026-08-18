//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// failingWriter fails every write after the first n bytes, simulating a
// client that hangs up mid-stream: fasthttp's stream writer surfaces the
// dead socket as a plain write error with no disconnect marker.
type failingWriter struct {
	budget int
	err    error
}

func (f *failingWriter) Write(p []byte) (int, error) {
	if f.budget <= 0 {
		return 0, f.err
	}
	if len(p) > f.budget {
		n := f.budget
		f.budget = 0
		return n, nil
	}
	f.budget -= len(p)
	return len(p), nil
}

// TestMsgPackStream_DisconnectIsClientError is the regression guard for the
// msgpack path logging a routine client disconnect at Error while the JSON
// path logged Warn for the identical condition.
//
// The severity decision is shared (streamErrEvent -> isClientError,
// query.go:143-156) and keys off errClientDisconnected. The JSON writer wraps
// its flush failures with that sentinel (query_json_writer.go:180); the
// msgpack emit loop returned the encoder's bare socket error, so
// isClientError returned false and the shared helper picked Error.
//
// Reverting wrapMsgPackWriteErr at the two emit-loop call sites fails this
// test: streamErr comes back as the bare write error and isClientError is
// false.
func TestMsgPackStream_DisconnectIsClientError(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewInt64Builder(alloc)
	defer b.Release()
	const rows = 50000 // large enough that the emit loop outruns the budget
	for i := 0; i < rows; i++ {
		b.Append(int64(i))
	}
	arr := b.NewArray()
	defer arr.Release()

	rec := array.NewRecord(schema, []arrow.Array{arr}, rows)
	defer rec.Release()

	// Let the header through, then break the pipe inside the column emit.
	fw := &failingWriter{budget: 256, err: io.ErrClosedPipe}
	bw := bufio.NewWriterSize(fw, 64)

	_, streamErr := streamMsgPackFromBatches(
		context.Background(), bw, schema, []arrow.Record{rec}, rows,
		nil, time.Now(), time.Now().UTC().Format(time.RFC3339),
	)

	if streamErr == nil {
		t.Fatal("expected a stream error when the writer fails mid-column")
	}
	if !errors.Is(streamErr, errClientDisconnected) {
		t.Errorf("streamErr = %v; want it to wrap errClientDisconnected so the "+
			"shared severity helper logs Warn, not Error", streamErr)
	}
	if !isClientError(streamErr) {
		t.Errorf("isClientError(%v) = false; a client hangup would be logged at "+
			"Error and page someone for what is routine ops noise", streamErr)
	}
}

// TestMsgPackStream_CtxCancelStaysCtxError guards the other half: a genuine
// timeout must NOT be relabelled as a client disconnect, or a real
// server-side deadline would be hidden as ops noise.
func TestMsgPackStream_CtxCancelStaysCtxError(t *testing.T) {
	alloc := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewInt64Builder(alloc)
	defer b.Release()
	const rows = 50000
	for i := 0; i < rows; i++ {
		b.Append(int64(i))
	}
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecord(schema, []arrow.Array{arr}, rows)
	defer rec.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already dead before the emit loop starts

	var sink discardWriter
	bw := bufio.NewWriterSize(&sink, 4096)

	_, streamErr := streamMsgPackFromBatches(
		ctx, bw, schema, []arrow.Record{rec}, rows,
		nil, time.Now(), time.Now().UTC().Format(time.RFC3339),
	)

	if streamErr == nil {
		t.Fatal("expected a stream error when ctx is already cancelled")
	}
	if !errors.Is(streamErr, context.Canceled) {
		t.Errorf("streamErr = %v; want context.Canceled preserved, not "+
			"relabelled — a real deadline must stay distinguishable", streamErr)
	}
	// isClientError covers ctx errors too, so the level is still Warn; the
	// point is that the cause is not rewritten.
	if !isClientError(streamErr) {
		t.Errorf("isClientError(%v) = false; ctx cancellation is client-side", streamErr)
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
