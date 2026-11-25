package ingest

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// pathFailStorage fails writes whose path contains fail, and records the
// rest. Each write waits for gate, when set, so a flush can be held in flight.
type pathFailStorage struct {
	failingStorageBackend
	mu    sync.Mutex
	fail  string
	gate  chan struct{}
	wrote []string
}

func (s *pathFailStorage) Write(ctx context.Context, path string, data []byte) error {
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != "" && strings.Contains(path, s.fail) {
		return errors.New("injected storage failure")
	}
	s.wrote = append(s.wrote, path)
	return nil
}

func outcomeBuffer(t *testing.T, st *pathFailStorage, maxBuffer int) *ArrowBuffer {
	t.Helper()
	buf := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: maxBuffer, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 16,
	}, st, zerolog.New(io.Discard))
	t.Cleanup(func() { _ = buf.Close() })
	return buf
}

func writeOutcomeBatch(t *testing.T, buf *ArrowBuffer, ctx context.Context, measurement string, rows int) {
	t.Helper()
	start := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	times := make([]int64, rows)
	vals := make([]float64, rows)
	for i := range times {
		times[i], vals[i] = start+int64(i), float64(i)
	}
	data := map[string]interface{}{"time": times, "value": vals}
	batch := &TypedColumnBatch{Data: data, Signature: getColumnSignature(data)}
	if err := buf.WriteTypedColumnarDirect(ctx, "db", measurement, batch, rows); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// Without a WAL every import row has no WAL copy. A size-triggered flush of
// the import's rows that fails is attributed to it, and WaitFlushes returns
// once that flush is done.
func TestFlushOutcomeCountsAFailedAsyncFlush1888(t *testing.T) {
	st := &pathFailStorage{fail: "/mimp/"}
	buf := outcomeBuffer(t, st, 5) // 5 rows: the write below triggers an async flush
	o := &FlushOutcome{}
	writeOutcomeBatch(t, buf, WithFlushOutcome(context.Background(), o), "mimp", 5)
	if o.NoWALRows() != 5 {
		t.Fatalf("no-WAL rows %d, want 5", o.NoWALRows())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := buf.FlushMeasurements(ctx, "db", []string{"mimp"}); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	if err := buf.WaitFlushes(ctx, "db", []string{"mimp"}); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if o.RowsAtRisk() != 5 {
		t.Fatalf("rows at risk %d, want 5", o.RowsAtRisk())
	}
}

// Another writer's failure on the same measurement, without the import's
// outcome, is not attributed to the import; the import's own rows were
// stored.
func TestFlushOutcomeIgnoresAnotherWritersFailure1888(t *testing.T) {
	st := &pathFailStorage{}
	buf := outcomeBuffer(t, st, 1000)
	o := &FlushOutcome{}
	writeOutcomeBatch(t, buf, WithFlushOutcome(context.Background(), o), "mshared", 3)
	ctx := context.Background()
	if err := buf.FlushMeasurements(ctx, "db", []string{"mshared"}); err != nil {
		t.Fatalf("import flush: %v", err)
	}
	// Now a live writer on the same measurement whose flush fails.
	st.mu.Lock()
	st.fail = "/mshared/"
	st.mu.Unlock()
	writeOutcomeBatch(t, buf, ctx, "mshared", 3)
	_ = buf.FlushMeasurements(ctx, "db", []string{"mshared"})
	if err := buf.WaitFlushes(ctx, "db", []string{"mshared"}); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if o.RowsAtRisk() != 0 {
		t.Fatalf("rows at risk %d: another writer's failure was attributed to the import", o.RowsAtRisk())
	}
}

// WaitFlushes waits for a flush still in flight, and only for the given
// measurements.
func TestWaitFlushesWaitsForAnInFlightFlush1888(t *testing.T) {
	gate := make(chan struct{})
	st := &pathFailStorage{gate: gate}
	buf := outcomeBuffer(t, st, 5)
	writeOutcomeBatch(t, buf, WithFlushOutcome(context.Background(), &FlushOutcome{}), "mslow", 5) // async flush, held at the gate
	done := make(chan error, 1)
	go func() { done <- buf.WaitFlushes(context.Background(), "db", []string{"mslow"}) }()
	select {
	case err := <-done:
		t.Fatalf("WaitFlushes returned (%v) while the flush was held", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := buf.WaitFlushes(context.Background(), "db", []string{"other"}); err != nil {
		t.Fatalf("wait for another measurement: %v", err)
	}
	close(gate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("WaitFlushes did not return after the flush finished")
	}
}

// A cancelled context ends the wait with its error: the outcome is unknown.
func TestWaitFlushesStopsWithItsContext1888(t *testing.T) {
	gate := make(chan struct{})
	st := &pathFailStorage{gate: gate}
	buf := outcomeBuffer(t, st, 5)
	defer close(gate)
	writeOutcomeBatch(t, buf, context.Background(), "mheld", 5)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := buf.WaitFlushes(ctx, "db", []string{"mheld"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait = %v, want deadline exceeded", err)
	}
}
