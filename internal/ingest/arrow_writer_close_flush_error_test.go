package ingest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

// closeTestConfig returns an ingest config whose buffers never flush on their
// own, so the only flush is the one Close() performs. MaxBufferSize is large
// and MaxBufferAgeMS long enough that neither trigger fires during the test.
func closeTestConfig() *config.IngestConfig {
	return &config.IngestConfig{
		MaxBufferSize:       1_000_000,
		MaxBufferAgeMS:      3_600_000,
		Compression:         "snappy",
		ShardCount:          4,
		FlushWorkers:        1,
		FlushQueueSize:      16,
		FlushTimeoutSeconds: 5,
	}
}

func closeTestRecord(measurement string) *models.Record {
	return &models.Record{
		Measurement: measurement,
		Time:        time.Now().UTC(),
		Fields:      map[string]interface{}{"value": 1.0},
		Tags:        map[string]string{},
	}
}

// TestArrowBuffer_CloseReportsFlushFailure pins that Close() surfaces a flush
// failure instead of swallowing it.
//
// Regression test for #803: Close() logged per-buffer flush errors and then
// returned nil unconditionally. The shutdown WAL purge treated that nil as
// "everything reached storage" and deleted the WAL — destroying the only
// remaining copy of data that had never been persisted.
func TestArrowBuffer_CloseReportsFlushFailure(t *testing.T) {
	buf := NewArrowBuffer(
		closeTestConfig(),
		&failingStorageBackend{err: errors.New("storage unavailable")},
		zerolog.New(io.Discard),
	)

	if err := buf.Write(context.Background(), "default", []interface{}{closeTestRecord("close_flush_err")}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	err := buf.Close()
	if err == nil {
		t.Fatal("Close() returned nil after a failed buffer flush; it must report the failure so the shutdown WAL purge is skipped (#803)")
	}

	// The returned error must carry the underlying cause, so an operator reading
	// the shutdown log can tell why the flush failed.
	if !strings.Contains(err.Error(), "storage unavailable") {
		t.Fatalf("Close() error does not carry the underlying cause: %v", err)
	}
}

// TestArrowBuffer_CloseFlushedCleanlyFalseOnFailure pins the signal the
// shutdown WAL purge actually consults (#803).
func TestArrowBuffer_CloseFlushedCleanlyFalseOnFailure(t *testing.T) {
	buf := NewArrowBuffer(
		closeTestConfig(),
		&failingStorageBackend{err: errors.New("storage unavailable")},
		zerolog.New(io.Discard),
	)

	if err := buf.Write(context.Background(), "default", []interface{}{closeTestRecord("close_clean_false")}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is true before Close() ran; it must default to false so an interrupted shutdown retains the WAL")
	}

	_ = buf.Close()

	if buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is true after a failed flush; the WAL purge would delete unflushed data (#803)")
	}
}

// TestArrowBuffer_CloseFlushedCleanlyTrueOnSuccess pins the other direction:
// a clean shutdown must still purge the WAL, otherwise every restart replays
// data that is already durable.
func TestArrowBuffer_CloseFlushedCleanlyTrueOnSuccess(t *testing.T) {
	buf := NewArrowBuffer(
		closeTestConfig(),
		&mockStorageBackend{},
		zerolog.New(io.Discard),
	)

	if err := buf.Write(context.Background(), "default", []interface{}{closeTestRecord("close_clean_true")}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close() on a healthy backend returned an error: %v", err)
	}

	if !buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is false after a successful flush; the WAL would be retained forever and replayed on every start")
	}
}

// TestArrowBuffer_CloseFlushedCleanlyTrueWithNoBuffers covers the empty case:
// a process that shuts down with nothing buffered has, trivially, flushed
// everything, and its WAL (header-only files) should still be purged.
func TestArrowBuffer_CloseFlushedCleanlyTrueWithNoBuffers(t *testing.T) {
	buf := NewArrowBuffer(
		closeTestConfig(),
		&mockStorageBackend{},
		zerolog.New(io.Discard),
	)

	if err := buf.Close(); err != nil {
		t.Fatalf("Close() with no buffered data returned an error: %v", err)
	}

	if !buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is false with nothing buffered; an idle node would retain its WAL forever")
	}
}

// slowCloseBackend blocks writes long enough that flush tasks pile up in the
// queue, so Close() cancels the workers while tasks are still pending.
type slowCloseBackend struct{ mockStorageBackend }

func (s *slowCloseBackend) Write(ctx context.Context, path string, data []byte) error {
	time.Sleep(300 * time.Millisecond)
	return nil
}

func (s *slowCloseBackend) WriteReader(ctx context.Context, path string, r io.Reader, size int64) error {
	time.Sleep(300 * time.Millisecond)
	return nil
}

// TestArrowBuffer_CloseNotCleanWhenQueuedFlushesAbandoned pins the asynchronous
// half of #803.
//
// Close() cancels the flush workers, and any task still sitting in the queue is
// discarded "in favor of WAL replay". Those records were already removed from
// shard.buffers at enqueue time, so the synchronous flush loop never sees them.
// Before this was accounted for, Close() returned nil and CloseFlushedCleanly()
// reported true — and the shutdown purge then deleted the WAL those records
// depended on.
func TestArrowBuffer_CloseNotCleanWhenQueuedFlushesAbandoned(t *testing.T) {
	cfg := closeTestConfig()
	// Flush on every record so tasks queue up faster than the slow backend
	// can drain them.
	cfg.MaxBufferSize = 1
	cfg.ShardCount = 1
	cfg.FlushWorkers = 1
	cfg.FlushQueueSize = 64

	buf := NewArrowBuffer(cfg, &slowCloseBackend{}, zerolog.New(io.Discard))

	for i := 0; i < 12; i++ {
		if err := buf.Write(context.Background(), "default", []interface{}{closeTestRecord("async_drop")}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	// Give the writes time to land in the queue behind the slow backend.
	time.Sleep(50 * time.Millisecond)

	_ = buf.Close()

	if buf.CloseFlushedCleanly() {
		t.Fatal("CloseFlushedCleanly() is true although queued flush tasks were abandoned; the WAL purge would delete the only copy of those records (#803)")
	}
}

// TestArrowBuffer_CloseFlushedCleanlyDoesNotRecover pins that the clean flag
// latches: a second Close() finds no buffers left and no new errors, and must
// not report clean and re-enable the WAL purge.
func TestArrowBuffer_CloseFlushedCleanlyDoesNotRecover(t *testing.T) {
	buf := NewArrowBuffer(
		closeTestConfig(),
		&failingStorageBackend{err: errors.New("storage unavailable")},
		zerolog.New(io.Discard),
	)

	if err := buf.Write(context.Background(), "default", []interface{}{closeTestRecord("latch")}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	_ = buf.Close()
	if buf.CloseFlushedCleanly() {
		t.Fatal("first Close() reported clean after a failed flush")
	}

	_ = buf.Close()
	if buf.CloseFlushedCleanly() {
		t.Fatal("second Close() reported clean; the flag must latch so a repeat call cannot re-enable the WAL purge (#803)")
	}
}
