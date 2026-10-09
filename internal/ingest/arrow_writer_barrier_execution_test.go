package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// Gate actual Parquet writes without replacing serialization or checkpointing.
type barrierStorage struct {
	storage.Backend
	entered            chan string
	release            chan struct{}
	once               sync.Once
	calls              atomic.Int64
	active             atomic.Int64
	peak               atomic.Int64
	failAll            bool
	failAt             int64
	ignoreCancellation bool
}

func (s *barrierStorage) unblock() { s.once.Do(func() { close(s.release) }) }

func (s *barrierStorage) gate(ctx context.Context, path string) (func(), error) {
	if !strings.HasSuffix(path, ".parquet") {
		return func() {}, nil
	}
	call := s.calls.Add(1)
	active := s.active.Add(1)
	for old := s.peak.Load(); active > old; old = s.peak.Load() {
		if s.peak.CompareAndSwap(old, active) {
			break
		}
	}
	done := func() { s.active.Add(-1) }
	s.entered <- path
	if s.ignoreCancellation {
		<-s.release
		return done, nil
	}
	select {
	case <-s.release:
		if s.failAll || s.failAt == call {
			return done, errors.New("injected storage failure")
		}
		return done, nil
	case <-ctx.Done():
		return done, ctx.Err()
	}
}

func (s *barrierStorage) Write(ctx context.Context, path string, data []byte) error {
	done, err := s.gate(ctx, path)
	defer done()
	if err != nil {
		return err
	}
	return s.Backend.Write(ctx, path, data)
}

func (s *barrierStorage) WriteReader(ctx context.Context, path string, r io.Reader, n int64) error {
	done, err := s.gate(ctx, path)
	defer done()
	if err != nil {
		return err
	}
	return s.Backend.WriteReader(ctx, path, r, n)
}

func barrierFixture(t *testing.T, workers, maxBuffer int, failAll bool, failAt int64) (*ArrowBuffer, *wal.Writer, *barrierStorage) {
	t.Helper()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	require.NoError(t, err)
	s := &barrierStorage{Backend: backend, entered: make(chan string, 128), release: make(chan struct{}), failAll: failAll, failAt: failAt}
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: filepath.Join(t.TempDir(), "wal"), SyncMode: wal.SyncModeFsync, Logger: zerolog.Nop()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: maxBuffer, MaxBufferAgeMS: 600000, Compression: "snappy", ShardCount: 4, FlushWorkers: workers, FlushQueueSize: 1, FlushTimeoutSeconds: 2}, s, zerolog.Nop())
	b.SetWAL(w)
	t.Cleanup(func() { s.unblock(); _ = b.Close() })
	return b, w, s
}

func addBarrierRows(t *testing.T, b *ArrowBuffer, measurements, rows int, source []string) {
	t.Helper()
	for i := 0; i < measurements; i++ {
		columns := map[string][]interface{}{"time": make([]interface{}, rows), "value": make([]interface{}, rows)}
		if len(source) > 0 {
			columns["source_line"] = make([]interface{}, rows)
		}
		for j := 0; j < rows; j++ {
			columns["time"][j] = int64(1700000000000000 + j)
			columns["value"][j] = int64(j)
			if len(source) > 0 {
				columns["source_line"][j] = source[(i*rows+j)%len(source)]
			}
		}
		require.NoError(t, b.WriteColumnarDirect(context.Background(), "test", fmt.Sprintf("measurement_%02d", i), columns))
	}
}

func barrierResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("barrier did not finish")
		return nil
	}
}

func startBarrier(b *ArrowBuffer, ctx context.Context) <-chan error {
	result := make(chan error, 1)
	go func() { result <- b.FlushAllAndWait(ctx) }()
	return result
}

func barrierParquetRows(t *testing.T, s *barrierStorage) int64 {
	t.Helper()
	keys, err := s.List(context.Background(), "")
	require.NoError(t, err)
	var rows int64
	for _, key := range keys {
		if !strings.HasSuffix(key, ".parquet") {
			continue
		}
		data, err := s.Read(context.Background(), key)
		require.NoError(t, err)
		reader, err := file.NewParquetReader(bytes.NewReader(data))
		require.NoError(t, err)
		rows += reader.NumRows()
		require.NoError(t, reader.Close())
	}
	return rows
}

func TestRecoveryBarrierUsesBoundedWorkersAndPersistsSnapshot(t *testing.T) {
	testBarrierParallel(t, 2, nil)
}

func testBarrierParallel(t *testing.T, rows int, source []string) {
	t.Helper()
	b, w, s := barrierFixture(t, 2, 1000000, false, 0)
	addBarrierRows(t, b, 6, rows, source)
	result := startBarrier(b, context.Background())
	for i := 0; i < 2; i++ {
		select {
		case <-s.entered:
		case <-time.After(500 * time.Millisecond):
			t.Errorf("snapshot did not use both configured workers")
		}
	}
	require.LessOrEqual(t, s.peak.Load(), int64(2))
	s.unblock()
	require.NoError(t, barrierResult(t, result))
	require.LessOrEqual(t, s.peak.Load(), int64(2))
	require.Equal(t, int64(6*rows), barrierParquetRows(t, s))
	require.Zero(t, w.PendingUnflushedCount())
}

// Done signals the point where the barrier actually waits on an admitted task.
// A caller deadline shorter than the barrier budget keeps this context intact.
type observingBarrierContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *observingBarrierContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestRecoveryBarrierWaitDoesNotLockIngestion(t *testing.T) {
	b, _, s := barrierFixture(t, 1, 1, false, 0)
	addBarrierRows(t, b, 1, 1, nil)
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("async flush not started")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	observed := &observingBarrierContext{Context: ctx, waiting: make(chan struct{})}
	result := startBarrier(b, observed)
	select {
	case <-observed.waiting:
	case <-time.After(time.Second):
		t.Fatal("barrier did not wait")
	}
	written := make(chan error, 1)
	go func() {
		written <- b.WriteColumnarDirect(context.Background(), "test", "new_measurement", map[string][]interface{}{"time": {int64(1700000000000000)}, "value": {int64(9)}})
	}()
	select {
	case err := <-written:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Error("ingestion blocked behind the recovery barrier")
	}
	s.unblock()
	require.NoError(t, barrierResult(t, result))
}

func TestRecoveryBarrierDeadlineKeepsQueuedAndBufferedOwnership(t *testing.T) {
	b, w, s := barrierFixture(t, 1, 1000000, false, 0)
	addBarrierRows(t, b, 8, 1, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	result := startBarrier(b, ctx)
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(400 * time.Millisecond):
		t.Error("barrier ignored its caller deadline while writing a snapshot")
		s.unblock()
		_ = barrierResult(t, result)
		return
	}
	require.Equal(t, 8, w.PendingUnflushedCount(), "deadline must not release unflushed identities")
	require.Positive(t, b.currentBufferedRecords(), "unsubmitted snapshot records must stay owned by buffers")
	s.unblock()
	require.NoError(t, b.NewRecoveryFlushBarrier()(context.Background()))
	require.Equal(t, int64(8), barrierParquetRows(t, s))
	require.Zero(t, w.PendingUnflushedCount())
}

func TestRecoveryBarrierFailsFastAndLeavesUnsubmittedBuffers(t *testing.T) {
	b, w, s := barrierFixture(t, 1, 1000000, true, 0)
	addBarrierRows(t, b, 16, 1, nil)
	s.unblock()
	require.Error(t, b.FlushAllAndWait(context.Background()))
	require.Positive(t, b.currentBufferedRecords(), "first storage failure should stop admitting the rest of the snapshot")
	require.Equal(t, 16, w.PendingUnflushedCount(), "failed writes must not be checkpointed")
}

func TestRecoveryBarrierPartialSuccessKeepsFailedIdentity(t *testing.T) {
	b, w, s := barrierFixture(t, 1, 1000000, false, 2)
	addBarrierRows(t, b, 2, 1, nil)
	s.unblock()
	require.Error(t, b.NewRecoveryFlushBarrier()(context.Background()))
	require.Equal(t, int64(1), barrierParquetRows(t, s))
	require.Equal(t, 1, w.PendingUnflushedCount())
}

func TestRecoveryBarrierConcurrentWaitHonorsCancellation(t *testing.T) {
	b, _, s := barrierFixture(t, 1, 1000000, false, 0)
	addBarrierRows(t, b, 1, 1, nil)
	first := startBarrier(b, context.Background())
	select {
	case <-s.entered:
	case <-time.After(time.Second):
		t.Fatal("first barrier did not begin flushing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	second := startBarrier(b, ctx)
	select {
	case err := <-second:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(400 * time.Millisecond):
		t.Error("second barrier cannot cancel while waiting for the first")
		s.unblock()
		_ = barrierResult(t, second)
	}
	s.unblock()
	require.NoError(t, barrierResult(t, first))
}

func TestRecoveryBarrierStartupBudgetRetainsWALWithUnresponsiveStorage(t *testing.T) {
	testBarrierStartupBudget(t, 1, nil)
}

func testBarrierStartupBudget(t *testing.T, rows int, source []string) {
	t.Helper()
	b, w, s := barrierFixture(t, 1, 1000000, false, 0)
	// Set before any task is submitted. Local filesystems can ignore context
	// cancellation; the barrier must return without abandoning their tasks.
	b.flushTimeout = 200 * time.Millisecond
	s.ignoreCancellation = true
	for i := 0; i < 8; i++ {
		columns := map[string][]interface{}{"time": make([]interface{}, rows), "value": make([]interface{}, rows)}
		if len(source) > 0 {
			columns["source_line"] = make([]interface{}, rows)
		}
		for j := 0; j < rows; j++ {
			columns["time"][j] = int64(1700000000000000 + j)
			columns["value"][j] = int64(i*rows + j)
			if len(source) > 0 {
				columns["source_line"][j] = source[(i*rows+j)%len(source)]
			}
		}
		payload, err := msgpack.Marshal(map[string]interface{}{"m": fmt.Sprintf("replay_%d", i),
			"columns": columns})
		require.NoError(t, err)
		_, err = w.AppendRawWithMetaTracked("test", payload)
		require.NoError(t, err)
	}
	oldFile := w.CurrentFile()
	require.NoError(t, w.Rotate())
	recovery := wal.NewRecovery(filepath.Dir(oldFile), zerolog.Nop())
	type result struct {
		stats *wal.RecoveryStats
		err   error
	}
	finished := make(chan result, 1)
	go func() {
		stats, err := recovery.RecoverWithOptions(context.Background(), nil, &wal.RecoveryOptions{
			SkipActiveFile: w.CurrentFile(), ColumnarCallback: b.WriteColumnarDirectReplay,
			BeforeDelete: b.NewRecoveryFlushBarrier(),
		})
		finished <- result{stats, err}
	}()
	select {
	case got := <-finished:
		require.ErrorIs(t, got.err, context.DeadlineExceeded)
		require.Equal(t, 1, got.stats.BarrierFailures)
		require.Positive(t, got.stats.KeptFiles)
	case <-time.After(time.Second):
		t.Error("startup barrier has no independent total wait budget")
		s.unblock()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatal("recovery did not finish after releasing storage")
		}
		return
	}
	_, err := os.Stat(oldFile)
	require.NoError(t, err, "recovery removed a WAL file before its writes completed")
	require.Equal(t, 8, w.PendingUnflushedCount())
	s.unblock()
	// Finish already-owned records, then retry recovery with the active file's
	// checkpoints, just as maintenance does. No record may be replayed twice.
	//
	// Drain in a loop rather than one call. The 200ms flushTimeout above is
	// what makes the FIRST barrier expire while storage is unresponsive, and
	// FlushAllAndWait caps the caller's context to it - so a single call gave
	// real Parquet encoding and eight local writes 200ms to finish, which is
	// ample on a developer machine and marginal on a shared CI runner under
	// -race, where it failed. Raising flushTimeout here instead would be a
	// data race: the flush goroutines read it, which is why it is set once
	// before any task is submitted. A timed-out barrier discards nothing, so
	// repeated calls make progress until the drain completes.
	drained := false
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		if err := b.FlushAllAndWait(context.Background()); err == nil {
			drained = true
			break
		}
	}
	require.True(t, drained, "healthy storage never drained within 30s of repeated barriers")
	checkpoints, err := w.CurrentCheckpointHashes()
	require.NoError(t, err)
	stats, err := recovery.RecoverWithOptions(context.Background(), nil, &wal.RecoveryOptions{
		SkipActiveFile: w.CurrentFile(), AdditionalCheckpointHashes: checkpoints,
		ColumnarCallback: b.WriteColumnarDirectReplay, BeforeDelete: b.NewRecoveryFlushBarrier(),
	})
	require.NoError(t, err)
	require.Zero(t, stats.BarrierFailures)
	_, err = os.Stat(oldFile)
	require.True(t, os.IsNotExist(err), "durable replay file was not reclaimed")
	require.Equal(t, int64(8*rows), barrierParquetRows(t, s))
	require.Zero(t, w.PendingUnflushedCount())
}
