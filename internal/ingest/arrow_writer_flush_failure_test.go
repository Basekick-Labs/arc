package ingest

import (
	"context"
	"errors"
	"io"
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
}
