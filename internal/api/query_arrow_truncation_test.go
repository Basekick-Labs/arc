//go:build duckdb_arrow

package api

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/rs/zerolog"
)

// Tests for #721. A truncated Arrow IPC stream used to be indistinguishable
// from a complete one: cut on a record-batch boundary it decodes with no
// error, and the failure paths that still closed the writer emitted a valid
// end-of-stream marker on top of the short result. A client saw HTTP 200,
// a valid stream, and silently fewer rows than its query matched.

// failingRecordReader yields n good batches, then reports an error, which is
// the shape of a mid-stream server-side failure.
type failingRecordReader struct {
	*simpleRecordReader
	err error
}

func (r *failingRecordReader) Err() error { return r.err }

func decodeStream(t *testing.T, b []byte) (int64, error) {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	defer r.Release()
	var rows int64
	for r.Next() {
		rows += r.Record().NumRows()
	}
	return rows, r.Err()
}

func TestStreamArrowIPCMarksTruncatedStreams(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	newReader := func(batches int) *failingRecordReader {
		recs := make([]arrow.Record, batches)
		for i := range recs {
			rows := make([][]interface{}, 10)
			for j := range rows {
				rows[j] = []interface{}{int64(i*10 + j)}
			}
			recs[i] = buildArrowBatch(alloc, schema, rows)
		}
		return &failingRecordReader{simpleRecordReader: newSimpleRecordReader(schema, recs)}
	}

	t.Run("a complete stream still decodes cleanly", func(t *testing.T) {
		reader := newReader(3)
		var out bytes.Buffer
		rows, err := streamArrowIPC(context.Background(), bufio.NewWriter(&out), reader,
			schema, nil, false, "", 0, zerolog.Nop())
		reader.Release()
		if err != nil {
			t.Fatalf("streamArrowIPC: %v", err)
		}
		if rows != 30 {
			t.Fatalf("streamed %d rows, want 30", rows)
		}
		decoded, derr := decodeStream(t, out.Bytes())
		if derr != nil {
			t.Fatalf("a complete stream must decode without error, got %v", derr)
		}
		if decoded != 30 {
			t.Errorf("decoded %d rows, want 30", decoded)
		}
	})

	t.Run("a failed stream does not decode as complete", func(t *testing.T) {
		reader := newReader(2)
		reader.err = errors.New("simulated mid-stream failure")
		var out bytes.Buffer
		rows, err := streamArrowIPC(context.Background(), bufio.NewWriter(&out), reader,
			schema, nil, false, "", 0, zerolog.Nop())
		reader.Release()
		if err == nil {
			t.Fatal("expected the reader error to surface")
		}
		if rows != 20 {
			t.Fatalf("streamed %d rows, want 20 before the failure", rows)
		}
		// The whole point: the client must not read this as a valid result.
		decoded, derr := decodeStream(t, out.Bytes())
		if derr == nil {
			t.Fatalf("a truncated stream decoded cleanly as %d rows; the client cannot tell it lost data", decoded)
		}
	})

	t.Run("a zero-batch failure does not decode as an empty result", func(t *testing.T) {
		reader := &failingRecordReader{
			simpleRecordReader: newSimpleRecordReader(schema, nil),
			err:                errors.New("failed before the first batch"),
		}
		var out bytes.Buffer
		_, err := streamArrowIPC(context.Background(), bufio.NewWriter(&out), reader,
			schema, nil, false, "", 0, zerolog.Nop())
		reader.Release()
		if err == nil {
			t.Fatal("expected the reader error to surface")
		}
		// Without the marker this is a schema-only stream that reads as a
		// legitimate "no rows", which a dashboard shows as no data rather
		// than as a failure.
		if _, derr := decodeStream(t, out.Bytes()); derr == nil {
			t.Fatal("a zero-batch failure decoded as a valid empty result")
		}
	})

	t.Run("a client that already hung up is not written to", func(t *testing.T) {
		reader := newReader(1)
		reader.err = errClientDisconnected
		var out bytes.Buffer
		_, err := streamArrowIPC(context.Background(), bufio.NewWriter(&out), reader,
			schema, nil, false, "", 0, zerolog.Nop())
		reader.Release()
		if err == nil {
			t.Fatal("expected the disconnect to surface")
		}
		// Nobody is listening; the stream is closed normally rather than
		// marked, so this stays out of the truncation signal.
		if _, derr := decodeStream(t, out.Bytes()); derr != nil {
			t.Errorf("a disconnect should not poison the body, got %v", derr)
		}
	})
}
