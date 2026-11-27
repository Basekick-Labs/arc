// res-1 companion: the SQL builders append validated aliases.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

// namesRec builds a 1-row record whose columns carry the given names.
func namesRec(t *testing.T, names ...string) arrow.Record {
	t.Helper()
	pool := memory.NewGoAllocator()
	fields := make([]arrow.Field, len(names))
	cols := make([]arrow.Array, len(names))
	for i, n := range names {
		b := array.NewInt64Builder(pool)
		b.Append(1)
		cols[i] = b.NewArray()
		fields[i] = arrow.Field{Name: n, Type: arrow.PrimitiveTypes.Int64}
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), cols, 1)
}

// Review H1/B1: the shadow name check and its name-based exemption.
func TestColumnNameDiff(t *testing.T) {
	rewritten := `make_timestamptz((946857600000000 + ((((epoch_us("time") - 946857600000000) // 3600000000)) * 3600000000)))`
	cases := []struct {
		name   string
		oracle []string
		arcx   []string
		differ bool
	}{
		{"identical", []string{"avg(v)", "n"}, []string{"avg(v)", "n"}, false},
		{"alias honoured", []string{"value"}, []string{"value"}, false},
		{"alias lost", []string{"value"}, []string{"avg(v)"}, true},
		{"grouped unaliased bucket (rewrite)", []string{rewritten, "avg(v)"}, []string{`date_trunc('hour', "time")`, "avg(v)"}, false},
		{"date_trunc_count unaliased (rewrite)", []string{rewritten, "count_star()"}, []string{`date_trunc('hour', "time")`, "count_star()"}, false},
		{"month bucket (not rewritten)", []string{`date_trunc('month', "time")`, "count_star()"}, []string{`date_trunc('month', "time")`, "count_star()"}, false},
		{"aliased bucket", []string{"time", "n"}, []string{"time", "n"}, false},
		{"rewrite exemption does not cover an aggregate", []string{"make_timestamptz(x)", "a"}, []string{"date_trunc(y)", "b"}, true},
		{"epoch-math bucket alias", []string{"t", "c"}, []string{"t", "c"}, false},
	}
	for _, c := range cases {
		diff := columnNameDiff(c.oracle, namesRec(t, c.arcx...))
		if (diff != "") != c.differ {
			t.Fatalf("%s: diff=%q, want differ=%v", c.name, diff, c.differ)
		}
	}
}
