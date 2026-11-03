package ingest

// #1437: ArrowBuffer.Write writes a MessagePack batch one record at a time. A
// failure after some records were buffered used to come back as the bare
// error, which callers answer as "nothing was written" (a refusal, a 503 to
// resend). It is now a WriteStoppedError naming what was written, and a record
// refused for its content is dropped while the others are written.

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

func newStopTestBuffer(t *testing.T) *ArrowBuffer {
	t.Helper()
	cfg := &config.IngestConfig{
		MaxBufferSize: 1_000_000, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 16,
	}
	b := NewArrowBuffer(cfg, &mockStorageBackend{}, zerolog.New(io.Discard))
	t.Cleanup(func() { b.Close() })
	return b
}

func stopTestRecord(measurement, column string, rows int) *models.ColumnarRecord {
	ts := make([]interface{}, rows)
	vs := make([]interface{}, rows)
	for i := range ts {
		ts[i] = time.Now().UTC().UnixMicro() + int64(i)
		vs[i] = float64(i)
	}
	return &models.ColumnarRecord{
		Measurement: measurement, Columnar: true, TimeUnit: "us",
		Columns: map[string][]interface{}{"time": ts, column: vs},
	}
}

// cancelAfter is a context whose Err turns to context.Canceled after its first
// n calls: the first records of a batch pass the buffer's cancellation check,
// a later one fails it. A real failure, not a test hook in the write path.
type cancelAfter struct {
	context.Context
	n     int64
	calls atomic.Int64
}

func (c *cancelAfter) Err() error {
	if c.calls.Add(1) > c.n {
		return context.Canceled
	}
	return nil
}

func TestWriteStopsPartWayWithWhatWasWritten(t *testing.T) {
	b := newStopTestBuffer(t)
	ctx := &cancelAfter{Context: context.Background(), n: 1}
	batch := []interface{}{stopTestRecord("aaa", "v", 2), stopTestRecord("bbb", "v", 3), stopTestRecord("ccc", "v", 1)}

	err := b.Write(ctx, "db", batch)
	ws, ok := AsWriteStopped(err)
	if !ok {
		t.Fatalf("err = %v (%T), want a WriteStoppedError", err, err)
	}
	if ws.StoppedAt != "bbb" || ws.RowsWritten != 2 || len(ws.Written) != 1 || ws.Written[0] != "aaa" {
		t.Fatalf("stop = %+v, want stopped at bbb after aaa's 2 rows", ws)
	}
	if !errors.Is(ws.Cause(), context.Canceled) {
		t.Fatalf("cause = %v, want context.Canceled", ws.Cause())
	}
	if got := b.bufferedRecordsFor("db", "aaa"); got != 2 {
		t.Fatalf("aaa buffered %d rows, want 2", got)
	}
	for _, m := range []string{"bbb", "ccc"} {
		if got := b.bufferedRecordsFor("db", m); got != 0 {
			t.Fatalf("%s buffered %d rows after the stop, want 0", m, got)
		}
	}
}

func TestWriteFailingFirstRecordIsUnchanged(t *testing.T) {
	b := newStopTestBuffer(t)
	ctx := &cancelAfter{Context: context.Background(), n: 0}
	err := b.Write(ctx, "db", []interface{}{stopTestRecord("aaa", "v", 1), stopTestRecord("bbb", "v", 1)})
	if _, ok := AsWriteStopped(err); ok {
		t.Fatalf("nothing was buffered, yet err is a WriteStoppedError: %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the cause itself", err)
	}
}

// A record refused for its content before the WAL (#1342: a column named
// like the time column) is dropped, and the records after it are written.
func TestWriteDropsAContentRefusalAndWritesTheRest(t *testing.T) {
	b := newStopTestBuffer(t)
	batch := []interface{}{stopTestRecord("aaa", "v", 1), stopTestRecord("bbb", "Time", 2), stopTestRecord("ccc", "v", 1)}

	err := b.Write(context.Background(), "db", batch)
	var pd *PointsDroppedError
	if !errors.As(err, &pd) {
		t.Fatalf("err = %v (%T), want PointsDroppedError", err, err)
	}
	if len(pd.Drops) != 1 || pd.Drops[0].Measurement != "bbb" || pd.Drops[0].Points != 2 {
		t.Fatalf("drops = %+v, want bbb's 2 points", pd.Drops)
	}
	if b.bufferedRecordsFor("db", "aaa") != 1 || b.bufferedRecordsFor("db", "ccc") != 1 {
		t.Fatal("aaa and ccc must be written around the dropped record")
	}
}

// With nothing else written, the content refusal is answered as before.
func TestWriteContentRefusalAloneIsUnchanged(t *testing.T) {
	b := newStopTestBuffer(t)
	err := b.Write(context.Background(), "db", []interface{}{stopTestRecord("bbb", "Time", 2)})
	if !errors.Is(err, ErrUnstorableColumnName) {
		t.Fatalf("err = %v, want ErrUnstorableColumnName", err)
	}
}

// The stop must not read as a refusal: the MQTT subscriber resends a refused
// message whole (subscriber.go), which would write the buffered part again.
func TestWriteStoppedIsNotARefusal(t *testing.T) {
	for _, cause := range []error{ErrWALNotDurable, ErrIngestFenced, ErrWALDiskPressure} {
		ws := &WriteStoppedError{Err: cause, Written: []string{"a"}, StoppedAt: "b"}
		if IsWriteRefusal(ws) {
			t.Fatalf("IsWriteRefusal(stop with cause %v) = true", cause)
		}
		var wrapped error = &WriteStoppedError{Err: cause}
		if errors.Is(wrapped, cause) {
			t.Fatalf("errors.Is sees the cause %v through the stop", cause)
		}
	}
}

// bufferedRecordsFor is how many rows of database/measurement sit in the
// buffer.
func (b *ArrowBuffer) bufferedRecordsFor(database, measurement string) int {
	key := database + "/" + measurement
	shard := b.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	return shard.bufferRecordCounts[key]
}

// A columnar record refused for its content is many points but one record in
// the decoder's unit, which the msgpack handler subtracts from its record
// count: counting its rows there drove the record counters negative.
func TestWriteContentDropCountsOneRecordPerColumnarRecord(t *testing.T) {
	b := newStopTestBuffer(t)
	err := b.Write(context.Background(), "db", []interface{}{stopTestRecord("aaa", "v", 1), stopTestRecord("bbb", "Time", 5)})
	var pd *PointsDroppedError
	if !errors.As(err, &pd) {
		t.Fatalf("err = %v, want PointsDroppedError", err)
	}
	if pd.Records != 1 || pd.Drops[0].Points != 5 {
		t.Fatalf("Records = %d, points = %d; want 1 record of 5 points", pd.Records, pd.Drops[0].Points)
	}
}

// Two records of one measurement: the second's failure stops the write after
// the first was buffered, so the measurement is both written and stopped at.
func TestWriteStoppedAtAMeasurementAlsoWritten(t *testing.T) {
	b := newStopTestBuffer(t)
	ctx := &cancelAfter{Context: context.Background(), n: 1}
	err := b.Write(ctx, "db", []interface{}{stopTestRecord("aaa", "v", 2), stopTestRecord("aaa", "v", 3)})
	ws, ok := AsWriteStopped(err)
	if !ok {
		t.Fatalf("err = %v, want a WriteStoppedError", err)
	}
	if ws.StoppedAt != "aaa" || len(ws.Written) != 1 || ws.Written[0] != "aaa" || ws.RowsWritten != 2 || ws.RecordsWritten != 1 {
		t.Fatalf("stop = %+v, want aaa written (2 rows, 1 record) and stopped at aaa", ws)
	}
}
