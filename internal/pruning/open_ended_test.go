package pruning

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// A start-only range asks for every row after its start. Rows stamped beyond
// the pruner's old assumed end (now + 24h: forecasts, devices whose clocks
// run ahead) must be read, not silently left out of the pruned list.
func TestOpenEndedRangeKeepsFuturePartitions(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	future := now.Add(48 * time.Hour)
	writeHour(t, root, now)
	writeHour(t, root, future)
	sql := "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"
	r, ok := p.OptimizeTablePath(context.Background(), glob, sql)
	if ok && !hasHour(asList(r), future) {
		t.Fatalf("the partition 48 h ahead was dropped: %v", r)
	}
}

// A start-only window with no partition at or after its start is proven empty
// instead of falling back to reading the whole measurement.
func TestOpenEndedEmptyWindowIsProvenEmpty(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	glob := filepath.Join(root, "db", "cpu") + "/**/*.parquet"
	p := livePruner(now)
	writeHour(t, root, now.Add(-5*time.Hour))
	sql := "SELECT * FROM t WHERE time > NOW() - INTERVAL '1 hour'"
	r, optimized, empty := p.OptimizeTablePathVerdict(context.Background(), glob, sql)
	if !empty {
		t.Fatalf("not proven empty: optimized=%v result=%v", optimized, r)
	}
}
