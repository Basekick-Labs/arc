package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

type reserveTestWAL struct{}

func (*reserveTestWAL) Append([]map[string]interface{}) error  { return nil }
func (*reserveTestWAL) AppendRaw([]byte) error                 { return nil }
func (*reserveTestWAL) AppendRawWithMeta(string, []byte) error { return nil }
func (*reserveTestWAL) Stats() map[string]interface{}          { return nil }
func (*reserveTestWAL) Close() error                           { return nil }

func TestElasticReserveDrainsWhenWorkerFreesQueueSlot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buf := &ArrowBuffer{
		ctx:        ctx,
		flushQueue: make(chan flushTask, 1),
		logger:     zerolog.Nop(),
	}
	if err := buf.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 10}); err != nil {
		t.Fatal(err)
	}
	first := flushTask{bufferKey: "db/first", recordCount: 1}
	second := flushTask{bufferKey: "db/second", recordCount: 4}
	buf.flushQueue <- first
	buf.queueDepth.Store(1)

	if got := buf.tryEnqueueFlush(second, second.bufferKey, second.recordCount); got != flushElasticReserved {
		t.Fatalf("tryEnqueueFlush outcome = %v, want flushElasticReserved", got)
	}
	if got := buf.ElasticReserveUsedRecords(); got != 4 {
		t.Fatalf("reserve occupancy = %d, want 4", got)
	}

	// Model the worker's queue-receive event, which opens a slot before the
	// current task is sent to storage.
	if got := <-buf.flushQueue; got.bufferKey != first.bufferKey {
		t.Fatalf("first queue task = %q, want %q", got.bufferKey, first.bufferKey)
	}
	metrics.Get().SetBufferQueueDepth(buf.queueDepth.Add(-1))
	buf.drainElasticReserve()
	if got := <-buf.flushQueue; got.bufferKey != second.bufferKey {
		t.Fatalf("drained queue task = %q, want %q", got.bufferKey, second.bufferKey)
	}
	if got := buf.ElasticReserveUsedRecords(); got != 0 {
		t.Fatalf("reserve occupancy after drain = %d, want 0", got)
	}
	if got := buf.queueDepth.Load(); got != 1 {
		t.Fatalf("queue depth after reserve drain = %d, want 1", got)
	}
}

func TestElasticReserveExhaustionReportsUnprotectedFallbackWithoutWAL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buf := &ArrowBuffer{
		ctx:        ctx,
		flushQueue: make(chan flushTask, 1),
		logger:     zerolog.Nop(),
	}
	if err := buf.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 1}); err != nil {
		t.Fatal(err)
	}
	buf.flushQueue <- flushTask{bufferKey: "db/queued", recordCount: 1}
	first := flushTask{bufferKey: "db/reserved", recordCount: 1}
	if got := buf.tryEnqueueFlush(first, first.bufferKey, first.recordCount); got != flushElasticReserved {
		t.Fatalf("first overflow outcome = %v, want flushElasticReserved", got)
	}
	before := metrics.Get().Snapshot()
	second := flushTask{bufferKey: "db/unprotected", recordCount: 1}
	if got := buf.tryEnqueueFlush(second, second.bufferKey, second.recordCount); got != flushQueueFull {
		t.Fatalf("exhausted outcome = %v, want flushQueueFull", got)
	}
	after := metrics.Get().Snapshot()
	if got := after["buffer_unprotected_overflow_records_total"].(int64) - before["buffer_unprotected_overflow_records_total"].(int64); got != 1 {
		t.Fatalf("unprotected overflow delta = %d, want 1", got)
	}
	if got := after["wal_records_preserved"].(int64) - before["wal_records_preserved"].(int64); got != 0 {
		t.Fatalf("WAL-preserved metric delta = %d without a WAL, want 0", got)
	}
}

func TestConfiguredWALFallbackIsNotCountedAsConfirmedPreservation(t *testing.T) {
	buf := &ArrowBuffer{wal: &reserveTestWAL{}, logger: zerolog.Nop()}
	before := metrics.Get().Snapshot()
	buf.recordWALFallback(7, "test fallback")
	after := metrics.Get().Snapshot()
	if got := after["buffer_flush_fallback_wal_configured_records_total"].(int64) - before["buffer_flush_fallback_wal_configured_records_total"].(int64); got != 7 {
		t.Fatalf("WAL-configured fallback delta = %d, want 7", got)
	}
	if got := after["wal_records_preserved"].(int64) - before["wal_records_preserved"].(int64); got != 0 {
		t.Fatalf("confirmed WAL-preserved delta = %d, want 0 because this path cannot confirm appends", got)
	}
}

func TestElasticReservePersistenceFailureLeavesRuntimeUnchanged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buf := &ArrowBuffer{ctx: ctx, flushQueue: make(chan flushTask, 1), logger: zerolog.Nop()}
	if err := buf.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 8}); err != nil {
		t.Fatal(err)
	}
	want := buf.ElasticReserveConfig()
	persistErr := errors.New("sqlite unavailable")
	err := buf.ConfigureElasticReserveWithPersistence(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 16}, func() error {
		return persistErr
	})
	if !errors.Is(err, persistErr) {
		t.Fatalf("ConfigureElasticReserveWithPersistence error = %v, want %v", err, persistErr)
	}
	if got := buf.ElasticReserveConfig(); got != want {
		t.Fatalf("runtime config after failed persistence = %+v, want unchanged %+v", got, want)
	}
}

func TestElasticReserveMemoryPreflightRejectsWithoutPersisting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buf := &ArrowBuffer{ctx: ctx, flushQueue: make(chan flushTask, 1), logger: zerolog.Nop()}
	persistCalled := false
	err := buf.ConfigureElasticReserveWithPersistence(RuntimeElasticReserveConfig{
		Enabled:         true,
		CapacityRecords: 1 << 60,
	}, func() error {
		persistCalled = true
		return nil
	})
	if err == nil {
		t.Fatal("ConfigureElasticReserveWithPersistence accepted an impossible memory reservation")
	}
	if persistCalled {
		t.Fatal("persistence callback ran after memory preflight rejected the reservation")
	}
	if got := buf.ElasticReserveConfig(); got != (RuntimeElasticReserveConfig{}) {
		t.Fatalf("runtime config after rejected preflight = %+v, want defaults", got)
	}
}

func TestTakingElasticReserveTasksClearsOccupancyAndTransfersOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buf := &ArrowBuffer{ctx: ctx, flushQueue: make(chan flushTask, 1), logger: zerolog.Nop()}
	if err := buf.ConfigureElasticReserve(RuntimeElasticReserveConfig{Enabled: true, CapacityRecords: 5}); err != nil {
		t.Fatal(err)
	}
	buf.flushQueue <- flushTask{bufferKey: "db/queued", recordCount: 1}
	task := flushTask{bufferKey: "db/reserved", recordCount: 3}
	if got := buf.tryEnqueueFlush(task, task.bufferKey, task.recordCount); got != flushElasticReserved {
		t.Fatalf("tryEnqueueFlush outcome = %v, want flushElasticReserved", got)
	}
	owned := buf.takeElasticReserveTasks()
	if len(owned) != 1 || owned[0].bufferKey != task.bufferKey || owned[0].recordCount != task.recordCount {
		t.Fatalf("taken reserve tasks = %+v, want one task %+v", owned, task)
	}
	if got := buf.ElasticReserveUsedRecords(); got != 0 {
		t.Fatalf("reserve occupancy after ownership transfer = %d, want 0", got)
	}
}
