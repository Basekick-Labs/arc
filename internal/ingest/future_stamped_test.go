package ingest

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Only rows stamped past now+margin and still in now's hour are counted: a
// row within the margin is safe, and a row in a later hour lands in that
// hour's directory where its file is kept.
func TestCountFutureStamped(t *testing.T) {
	b := &ArrowBuffer{logger: zerolog.Nop()}
	b.SetFutureStampMargin(5 * time.Minute)
	now := time.Date(2026, 10, 22, 10, 20, 0, 0, time.UTC)
	us := func(d time.Duration) int64 { return now.Add(d).UnixMicro() }
	times := []int64{
		us(-time.Minute),     // past
		us(4 * time.Minute),  // within the margin
		us(6 * time.Minute),  // counted
		us(39 * time.Minute), // counted (10:59)
		us(41 * time.Minute), // next hour: not counted
	}
	key := "ingest_future_stamped_rows_total"
	before := metrics.Get().Snapshot()[key].(int64)
	b.countFutureStamped("db", "cpu", times, now)
	if got := metrics.Get().Snapshot()[key].(int64) - before; got != 2 {
		t.Fatalf("counted %d rows, want 2", got)
	}
	// Margin past the end of the hour: nothing can be hidden.
	before = metrics.Get().Snapshot()[key].(int64)
	b.countFutureStamped("db", "cpu", times, time.Date(2026, 10, 22, 10, 58, 0, 0, time.UTC))
	if got := metrics.Get().Snapshot()[key].(int64) - before; got != 0 {
		t.Fatalf("counted %d rows with the margin past the hour, want 0", got)
	}
	// A margin under a second is raised to one, matching the pruner.
	b.SetFutureStampMargin(0)
	if b.futureMargin() != time.Second {
		t.Fatalf("margin %v, want the 1s floor", b.futureMargin())
	}
}

// Through a real flush: the count runs on the flush path, and both future
// warnings are logged once a minute per measurement, not once per flush.
// The >1h warning reads the batch's latest row.
func TestFlushCountsAndRateLimitsFutureStamps(t *testing.T) {
	now := time.Now().UTC()
	hourEnd := now.Truncate(time.Hour).Add(time.Hour)
	if hourEnd.Sub(now) < 30*time.Second {
		t.Skip("too close to the end of the hour")
	}
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	b := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 100000, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"}, backend, zerolog.New(logs))
	defer b.Close()
	b.SetFutureStampMargin(time.Second)
	ahead := now.Add(10 * time.Minute)
	if ahead.After(hourEnd.Add(-5 * time.Second)) {
		ahead = hourEnd.Add(-5 * time.Second)
	}
	key := "ingest_future_stamped_rows_total"
	before := metrics.Get().Snapshot()[key].(int64)
	for i := 0; i < 2; i++ {
		// One row now, one ahead in this hour, one more than an hour ahead.
		if err := b.WriteTypedColumnarDirect(context.Background(), "db", "cpu", &TypedColumnBatch{
			Data: map[string]interface{}{
				"time": []int64{now.UnixMicro(), ahead.UnixMicro(), now.Add(2 * time.Hour).UnixMicro()},
				"v":    []float64{1, 2, 3},
			},
		}, 3); err != nil {
			t.Fatal(err)
		}
		if err := b.FlushAllAndWait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := metrics.Get().Snapshot()[key].(int64) - before; got != 2 {
		t.Errorf("counted %d rows over two flushes, want 2", got)
	}
	out := logs.String()
	if n := strings.Count(out, "Data timestamp is >1 hour in future"); n != 1 {
		t.Errorf(">1h future warning logged %d times over two flushes, want 1 (rate-limited, and read from the latest row)", n)
	}
	if n := strings.Count(out, "Rows stamped more than query.file_time_pruning_margin_seconds"); n != 1 {
		t.Errorf("margin warning logged %d times over two flushes, want 1", n)
	}
}

// lockedBuffer is a log sink the flush workers write while the test reads.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}
