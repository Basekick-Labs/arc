package ingest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
)

func TestRecoveryFlushBarrierRejectsCompletedAsyncFailure(t *testing.T) {
	buffer := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 4, FlushWorkers: 1, FlushQueueSize: 2,
	}, &failingStorageBackend{err: errors.New("storage unavailable")}, zerolog.Nop())
	t.Cleanup(func() { _ = buffer.Close() })
	barrier := buffer.NewRecoveryFlushBarrier()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := buffer.Write(ctx, "test", []interface{}{&models.Record{
		Measurement: "recovery", Time: time.Now().UTC(),
		Fields: map[string]interface{}{"value": 1.0},
	}}); err != nil {
		t.Fatal(err)
	}
	// No sleeps: the real async task has completed, and its extracted records
	// are no longer in a shard for the barrier's synchronous snapshot to see.
	if err := buffer.waitForAsyncFlushTasksThrough(ctx, buffer.asyncFlushFence()); err != nil {
		t.Fatal(err)
	}
	if !buffer.HasFlushFailure() {
		t.Fatal("test did not exercise an async storage failure")
	}
	if err := barrier(ctx); err == nil {
		t.Fatal("recovery barrier accepted an async flush that failed before the wait")
	}
}

func TestRecoveryFlushBarrierScopesFailuresToOnePass(t *testing.T) {
	buffer := &ArrowBuffer{}
	buffer.markFlushFailure() // Failure that caused this recovery attempt.
	barrier := buffer.NewRecoveryFlushBarrier()
	if err := barrier(context.Background()); err != nil {
		t.Fatalf("old failure blocked a new recovery attempt: %v", err)
	}
	buffer.markFlushFailure() // A later batch of the SAME attempt fails.
	if err := barrier(context.Background()); err == nil {
		t.Fatal("later batch failure was ignored")
	}
	if err := barrier(context.Background()); err == nil {
		t.Fatal("failed recovery attempt was allowed to delete on a second barrier")
	}
	if err := buffer.NewRecoveryFlushBarrier()(context.Background()); err != nil {
		t.Fatalf("historical failure permanently blocked a new attempt: %v", err)
	}
}

func TestRecoveryFlushBarrierPreservesCancellation(t *testing.T) {
	buffer := &ArrowBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := buffer.NewRecoveryFlushBarrier()(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("barrier error = %v, want context.Canceled", err)
	}
}
