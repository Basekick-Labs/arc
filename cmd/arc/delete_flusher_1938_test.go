package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// failWritesBackend fails every write.
type failWritesBackend struct{ storage.Backend }

func (failWritesBackend) Write(context.Context, string, []byte) error {
	return errors.New("injected storage failure")
}

func (failWritesBackend) WriteReader(context.Context, string, io.Reader, int64) error {
	return errors.New("injected storage failure")
}

// Without a WAL a DELETE does not consult the latch, which nothing would ever
// clear, but it still holds its flush to the buffer's generation (#1938).
func TestNoWALDeleteFlusherReportsNoReasonButTheGeneration1938(t *testing.T) {
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	buf := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000000, MaxBufferAgeMS: 600000,
		Compression: "snappy", ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 8}, failWritesBackend{local}, zerolog.Nop())
	t.Cleanup(func() { _ = buf.Close() })

	ts := time.Date(2026, 10, 11, 10, 0, 0, 0, time.UTC).UnixMicro()
	if err := buf.WriteColumnarDirect(context.Background(), "db", "cpu", map[string][]interface{}{"time": {ts}, "value": {1.0}}); err != nil {
		t.Fatal(err)
	}
	if err := buf.FlushAllAndWait(context.Background()); err == nil {
		t.Fatal("the flush did not fail")
	}
	buf.MarkWALRecoveryPending()
	failed, pending, gen := buf.FlushFailureState()
	if !failed || !pending || gen == 0 {
		t.Fatalf("buffer state %v %v %d, want both reasons and a generation", failed, pending, gen)
	}

	f, ok := deleteFlusher(buf, nil).(noWALDeleteFlusher)
	if !ok {
		t.Fatalf("deleteFlusher without a WAL is %T", deleteFlusher(buf, nil))
	}
	gotFailed, gotPending, gotGen := f.FlushFailureState()
	if gotFailed || gotPending || gotGen != gen {
		t.Fatalf("FlushFailureState = %v %v %d, want false false %d", gotFailed, gotPending, gotGen, gen)
	}
	if _, err := f.FlushScopeAndWaitSince(context.Background(), "db", "cpu", gotGen); err != nil {
		t.Fatalf("a DELETE with no newer failure was refused: %v", err)
	}
	if _, err := f.FlushScopeAndWaitSince(context.Background(), "db", "cpu", gotGen-1); err == nil {
		t.Fatal("a DELETE whose baseline predates a failure went ahead")
	}
}
