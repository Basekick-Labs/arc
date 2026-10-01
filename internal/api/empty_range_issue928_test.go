package api

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/fieldschema"
)

// anchorOnly reports whether the transformed SQL scans exactly one Parquet
// path and that path is a materialized anchor.
func anchorOnly(sql string) bool {
	return strings.Count(sql, ".parquet'") == 1 && strings.Contains(sql, "/schema/")
}

func TestEmptyRangeAnchorScanIssue928(t *testing.T) {
	e := newFieldSchemaEnv(t, fieldschema.Options{}, true)
	e.h.SetEmptyRangeAnchorScan(true)
	seedCustomerScenario(e) // first flush registers a complete anchor (no files existed)
	ctx := context.Background()

	empty := rangeSQL("*", feb1, feb1.Add(24*time.Hour), "")
	transformed := e.h.convertSQLToStoragePaths(ctx, empty)
	if !anchorOnly(transformed) {
		t.Fatalf("expected an anchor-only scan: %s", transformed)
	}
	out := e.mustRows(empty)
	if out.RowCount != 0 || !hasAll(out.Columns, "time", "source", "stable", "retired_field", "weeks_later") {
		t.Fatalf("empty range: rows=%d cols=%v", out.RowCount, out.Columns)
	}
	// Data flushed into February is seen once the listing is stale.
	e.write("sch", "multiday", feb1.Add(3*time.Hour), map[string]interface{}{"source": []string{"mid"}, "stable": []int64{3}})
	time.Sleep(2100 * time.Millisecond)
	out = e.mustRows(empty)
	if out.RowCount != 1 {
		t.Fatalf("February data hidden after flush: %+v", out)
	}

	// A JOIN against a dimension measurement outside the range keeps the
	// glob for the dimension and still joins.
	e.write("sch", "hosts", jan1, map[string]interface{}{"source": []string{"early"}, "region": []string{"eu"}})
	join := "SELECT m.stable, h.region FROM sch.multiday m JOIN sch.hosts h ON m.source = h.source WHERE m.time >= '2026-03-01T00:00:00Z' AND m.time < '2026-03-02T00:00:00Z'"
	if tr := e.h.convertSQLToStoragePaths(ctx, join); anchorOnly(tr) || strings.Count(tr, "/schema/") != 2 {
		t.Fatalf("join must keep both globs with anchors: %s", tr)
	}
	// hosts only holds "early" and March's source is "late", so zero joined
	// rows is the right answer here; what matters is that the dimension
	// measurement was scanned (no shortcut) and the query bound.
	if out = e.mustRows(join); out.RowCount != 0 {
		t.Fatalf("unexpected join rows: %+v", out.Data)
	}
	joinJan := strings.ReplaceAll(strings.ReplaceAll(join, "2026-03-01", "2026-01-01"), "2026-03-02", "2026-01-02")
	out = e.mustRows(joinJan)
	if out.RowCount != 1 || out.Data[0][1] != "eu" {
		t.Fatalf("join lost rows: %+v", out.Data)
	}

	// Disabled: the glob is back.
	e.h.SetEmptyRangeAnchorScan(false)
	e.h.InvalidateCaches()
	if tr := e.h.convertSQLToStoragePaths(ctx, rangeSQL("*", feb1.Add(48*time.Hour), feb1.Add(72*time.Hour), "")); anchorOnly(tr) {
		t.Fatalf("disabled flag must keep the glob: %s", tr)
	}
}

func TestEmptyRangeIncompleteAnchorKeepsGlobIssue928(t *testing.T) {
	// Files written before the registry existed: the anchor ingest creates
	// on the next flush cannot claim to know every column, so an empty
	// range keeps the glob and still advertises the union of all files.
	e := newFieldSchemaEnv(t, fieldschema.Options{}, false)
	e.write("sch", "legacy", jan1, map[string]interface{}{"old_only": []int64{1}})
	e.buf.SetFieldSchema(e.reg)
	e.write("sch", "legacy", mar1, map[string]interface{}{"recent": []int64{2}})
	e.h.SetEmptyRangeAnchorScan(true)
	if e.reg.IsComplete("sch", "legacy") {
		t.Fatal("anchor over pre-existing files must be incomplete")
	}
	empty := fmt.Sprintf("SELECT * FROM sch.legacy WHERE time >= '%s' AND time < '%s'", feb1.Format(time.RFC3339), feb1.Add(24*time.Hour).Format(time.RFC3339))
	if tr := e.h.convertSQLToStoragePaths(context.Background(), empty); anchorOnly(tr) {
		t.Fatalf("incomplete anchor must keep the glob: %s", tr)
	}
	out := e.mustRows(empty)
	if out.RowCount != 0 || !hasAll(out.Columns, "old_only", "recent") {
		t.Fatalf("empty range must still advertise every column: %v", out.Columns)
	}
}
