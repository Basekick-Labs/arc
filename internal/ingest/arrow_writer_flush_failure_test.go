package ingest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

type failingStorageBackend struct {
	err error
}

func (s *failingStorageBackend) Write(ctx context.Context, path string, data []byte) error {
	return s.err
}

func (s *failingStorageBackend) WriteReader(ctx context.Context, path string, reader io.Reader, size int64) error {
	return s.err
}

func (s *failingStorageBackend) Read(ctx context.Context, path string) ([]byte, error) {
	return nil, nil
}

func (s *failingStorageBackend) ReadTo(ctx context.Context, path string, writer io.Writer) error {
	return nil
}

func (s *failingStorageBackend) Delete(ctx context.Context, path string) error { return nil }
func (s *failingStorageBackend) Exists(ctx context.Context, path string) (bool, error) {
	return false, nil
}
func (s *failingStorageBackend) List(ctx context.Context, prefix string) ([]string, error) {
	return nil, nil
}
func (s *failingStorageBackend) Close() error       { return nil }
func (s *failingStorageBackend) Type() string       { return "mock-failing" }
func (s *failingStorageBackend) ConfigJSON() string { return "{}" }
func (s *failingStorageBackend) ReadToAt(_ context.Context, _ string, _ io.Writer, _ int64) error {
	return nil
}
func (s *failingStorageBackend) StatFile(_ context.Context, _ string) (int64, error) {
	return -1, nil
}
func (s *failingStorageBackend) AppendReader(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return nil
}

func flushFailureColumns() map[string][]interface{} {
	start := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	return map[string][]interface{}{
		"time":  {start, start + 1},
		"value": {1.0, 2.0},
	}
}

func TestArrowBuffer_FlushFailureMetricOnAsyncStorageError(t *testing.T) {
	cfg := &config.IngestConfig{
		MaxBufferSize:  1,
		MaxBufferAgeMS: 60000,
		Compression:    "snappy",
		ShardCount:     4,
		FlushWorkers:   1,
		FlushQueueSize: 16,
	}

	buf := NewArrowBuffer(
		cfg,
		&failingStorageBackend{err: errors.New("storage unavailable")},
		zerolog.New(io.Discard),
	)
	t.Cleanup(func() { _ = buf.Close() })

	before := metrics.Get().Snapshot()["buffer_flush_failures_total"].(int64)
	droppedBefore := metrics.Get().Snapshot()["buffer_records_dropped_total"].(int64)

	record := &models.Record{
		Measurement: "flush_failure_test",
		Time:        time.Now().UTC(),
		Fields: map[string]interface{}{
			"value": 1.0,
		},
		Tags: map[string]string{},
	}

	if err := buf.Write(context.Background(), "default", []interface{}{record}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if buf.HasFlushFailure() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !buf.HasFlushFailure() {
		t.Fatal("expected flush failure flag after storage write error")
	}

	after := metrics.Get().Snapshot()["buffer_flush_failures_total"].(int64)
	if after != before+1 {
		t.Fatalf("expected buffer_flush_failures_total=%d, got %d", before+1, after)
	}
	droppedAfter := metrics.Get().Snapshot()["buffer_records_dropped_total"].(int64)
	if droppedAfter != droppedBefore+1 {
		t.Fatalf("expected buffer_records_dropped_total=%d, got %d", droppedBefore+1, droppedAfter)
	}
}

func TestArrowBuffer_FlushFailureReportsRowsWithoutWALCopy(t *testing.T) {
	var logs bytes.Buffer
	buf := NewArrowBuffer(
		&config.IngestConfig{
			MaxBufferSize:  100,
			MaxBufferAgeMS: 60000,
			Compression:    "snappy",
			ShardCount:     1,
			FlushWorkers:   1,
			FlushQueueSize: 1,
		},
		&failingStorageBackend{err: errors.New("storage unavailable")},
		zerolog.New(zerolog.SyncWriter(&logs)),
	)
	t.Cleanup(func() { _ = buf.Close() })

	if err := buf.WriteColumnarDirect(context.Background(), "default", "flush_loss_test", flushFailureColumns()); err != nil {
		t.Fatalf("WriteColumnarDirect: %v", err)
	}
	droppedBefore, _ := metrics.Get().Snapshot()["buffer_records_dropped_total"].(int64)

	shard := buf.getShard("default/flush_loss_test")
	shard.mu.Lock()
	err := buf.flushBufferLocked(context.Background(), shard, "default/flush_loss_test", "default", "flush_loss_test")
	shard.mu.Unlock()
	if err == nil {
		t.Fatal("expected storage failure")
	}

	if err := buf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if got := logs.String(); !strings.Contains(got, `"records_without_wal":2`) || !strings.Contains(got, "rows without a WAL copy were lost") {
		t.Fatalf("flush failure should report the two rows without a WAL copy; logs:\n%s", got)
	}

	snapshot := metrics.Get().Snapshot()
	dropped, ok := snapshot["buffer_records_dropped_total"].(int64)
	if !ok || dropped != droppedBefore+2 {
		t.Fatalf("buffer_records_dropped_total = %v, want %d", snapshot["buffer_records_dropped_total"], droppedBefore+2)
	}
}

type failedAppendWAL struct{ err error }

func (w *failedAppendWAL) Append([]map[string]interface{}) error  { return w.err }
func (w *failedAppendWAL) AppendRaw([]byte) error                 { return w.err }
func (w *failedAppendWAL) AppendRawWithMeta(string, []byte) error { return w.err }
func (w *failedAppendWAL) Stats() map[string]interface{}          { return nil }
func (w *failedAppendWAL) Close() error                           { return nil }

func TestArrowBuffer_FlushFailureCountsOnlyRowsWithoutWALCopy(t *testing.T) {
	tests := []struct {
		name        string
		setWAL      func(*ArrowBuffer)
		write       func(*ArrowBuffer) error
		wantDropped int64
	}{
		{
			name:   "successful WAL append",
			setWAL: func(buf *ArrowBuffer) { buf.SetWAL(&checkpointRecordingWAL{}) },
			write: func(buf *ArrowBuffer) error {
				return buf.WriteColumnarDirect(context.Background(), "default", "flush_loss_wal_test", flushFailureColumns())
			},
			wantDropped: 0,
		},
		{
			name:   "WAL append failure",
			setWAL: func(buf *ArrowBuffer) { buf.SetWAL(&failedAppendWAL{err: errors.New("WAL unavailable")}) },
			write: func(buf *ArrowBuffer) error {
				return buf.WriteColumnarDirect(context.Background(), "default", "flush_loss_wal_test", flushFailureColumns())
			},
			wantDropped: 2,
		},
		{
			name:   "no WAL copy despite a configured WAL",
			setWAL: func(buf *ArrowBuffer) { buf.SetWAL(&checkpointRecordingWAL{}) },
			write: func(buf *ArrowBuffer) error {
				return buf.WriteColumnarDirectNoWAL(context.Background(), "default", "flush_loss_wal_test", flushFailureColumns())
			},
			wantDropped: 2,
		},
		{
			name:   "untracked replay still has its WAL copy",
			setWAL: func(buf *ArrowBuffer) { buf.SetWAL(&checkpointRecordingWAL{}) },
			write: func(buf *ArrowBuffer) error {
				return buf.WriteColumnarDirectReplay(context.Background(), "default", "flush_loss_wal_test", flushFailureColumns(), strings.Repeat("a", 64))
			},
			wantDropped: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := NewArrowBuffer(
				&config.IngestConfig{MaxBufferSize: 100, MaxBufferAgeMS: 60000, Compression: "snappy", ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 1},
				&failingStorageBackend{err: errors.New("storage unavailable")},
				zerolog.New(io.Discard),
			)
			t.Cleanup(func() { _ = buf.Close() })
			tt.setWAL(buf)
			if err := tt.write(buf); err != nil {
				t.Fatalf("write: %v", err)
			}
			before := metrics.Get().Snapshot()["buffer_records_dropped_total"].(int64)

			shard := buf.getShard("default/flush_loss_wal_test")
			shard.mu.Lock()
			err := buf.flushBufferLocked(context.Background(), shard, "default/flush_loss_wal_test", "default", "flush_loss_wal_test")
			shard.mu.Unlock()
			if err == nil {
				t.Fatal("expected storage failure")
			}

			after := metrics.Get().Snapshot()["buffer_records_dropped_total"].(int64)
			if after != before+tt.wantDropped {
				t.Fatalf("buffer_records_dropped_total delta = %d, want %d", after-before, tt.wantDropped)
			}
		})
	}
}
