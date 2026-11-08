// res-1 companion: the SQL builders append validated aliases.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"strings"
	"testing"
)

func TestBuildersEmitAliases(t *testing.T) {
	d := Decision{Shape: ShapeScanAgg, AggItems: []string{"avg(v)", "count(*)"}, AggAliases: []string{"value", ""}}
	sql, ok := buildScanAggSQL(d, "['/p.parquet']")
	if !ok || !strings.HasPrefix(sql, "SELECT avg(v) AS value, count(*) FROM") {
		t.Fatalf("ungrouped: %q %v", sql, ok)
	}
	g := Decision{Shape: ShapeScanAggGrouped, GroupKey: "host", AggItems: []string{"host", "avg(v)"}, AggAliases: []string{"", "a"}}
	sql, ok = buildGroupedSQL(g, "['/p.parquet']")
	if !ok || !strings.HasPrefix(sql, "SELECT host, avg(v) AS a FROM") {
		t.Fatalf("grouped: %q %v", sql, ok)
	}
	// Defense in depth: a hand-built bad alias or an alias on a key is refused.
	for _, bad := range []Decision{
		{Shape: ShapeScanAgg, AggItems: []string{"avg(v)"}, AggAliases: []string{"a; DROP"}},
		{Shape: ShapeScanAgg, AggItems: []string{"avg(v)"}, AggAliases: []string{"from"}},
		{Shape: ShapeScanAggGrouped, GroupKey: "host", AggItems: []string{"host", "avg(v)"}, AggAliases: []string{"h", ""}},
	} {
		var ok bool
		if bad.Shape == ShapeScanAgg {
			_, ok = buildScanAggSQL(bad, "['/p.parquet']")
		} else {
			_, ok = buildGroupedSQL(bad, "['/p.parquet']")
		}
		if ok {
			t.Fatalf("must refuse %+v", bad)
		}
	}
}
