package ingest

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
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
