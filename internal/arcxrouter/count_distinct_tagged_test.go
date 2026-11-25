// cd-1 companion: SQL-boundary re-validation of the `count(DISTINCT <col>)` item
// and its census classification live in the tagged router.

//go:build cgo && arcx_engine

package arcxrouter

import (
	"math/big"
	"testing"
)

func TestCountDistinctItemRevalidation(t *testing.T) {
	for _, item := range []string{"count(DISTINCT host)", "count(DISTINCT Host)", "count(DISTINCT tags['k'])"} {
		if !isAggItem(item) {
			t.Fatalf("isAggItem(%q) = false", item)
		}
	}
	for _, item := range []string{
		"count(DISTINCT host, id)", "count(DISTINCT (host))", "count(DISTINCT *)",
		"count(distinct host)", "count(DISTINCT  host)", "count(DISTINCT host) --",
		"count(DISTINCT host'))", "sum(DISTINCT host)",
	} {
		if isAggItem(item) {
			t.Fatalf("isAggItem(%q) = true", item)
		}
	}
	got, ok := buildScanAggSQL(Decision{AggItems: []string{"count(*)", "count(DISTINCT norad_id)"}}, "['/p.parquet']")
	if want := "SELECT count(*), count(DISTINCT norad_id) FROM read_parquet(['/p.parquet'])"; !ok || got != want {
		t.Fatalf("buildScanAggSQL = (%q, %t), want %q", got, ok, want)
	}
}

// A served count(DISTINCT x) no longer claims the select-DISTINCT census bucket; a
// declined one is filed by its other features.
func TestCensusCountDistinct(t *testing.T) {
	cases := []struct{ sql, want string }{
		{"SELECT count(DISTINCT host) FROM cpu", "eligible"},
		{"SELECT count(DISTINCT host, region) FROM cpu", "agg_fn"},
		{"SELECT DISTINCT host FROM cpu", "distinct"},
		{"SELECT count(DISTINCT host) FROM cpu UNION SELECT 1 FROM mem", "set_op"},
	}
	for _, c := range cases {
		if got, _ := CensusClassify(c.sql, "production"); got != c.want {
			t.Errorf("%q: got %q, want %q", c.sql, got, c.want)
		}
	}
}

// The shadow compare takes a DISTINCT count as an exact integer: no tolerance,
// and one off is a mismatch.
func TestCompareAggCountDistinctExact(t *testing.T) {
	items := []string{"count(DISTINCT norad_id)"}
	if aggItemTolerant(items[0]) {
		t.Fatal("count(DISTINCT x) must not be tolerant")
	}
	if diff := compareAgg(aggRec(t, int64(33001)), []aggCell{{isInt: true, i: big.NewInt(33001)}}, items); diff != "" {
		t.Fatalf("equal counts: unexpected diff %q", diff)
	}
	if diff := compareAgg(aggRec(t, int64(33001)), []aggCell{{isInt: true, i: big.NewInt(33000)}}, items); diff == "" {
		t.Fatal("counts one apart must differ")
	}
}

// decisionFor mirrors Decide's matchResult → Decision mapping for the agg shapes.
func decisionFor(t *testing.T, sql string) Decision {
	t.Helper()
	m, ok := eligibleShape(sql)
	if !ok {
		t.Fatalf("should be eligible: %s", sql)
	}
	return Decision{
		Shape: m.shape, WhereText: m.whereText, HavingText: m.havingText,
		HavingUnstable: m.havingUnstable, HavingHidden: m.havingHidden,
		AggItems: m.aggItems, AggAliases: m.aggAliases, GroupKey: m.groupKey,
		BucketUnit: m.bucketUnit, BucketCol: m.bucketCol, OrderKeys: m.orderKeys,
		Limit: m.limit, HasLimit: m.hasLimit, BucketAlias: m.bucketAlias,
		EpochWidthSecs: m.epochWidthSecs,
	}
}

// The router→engine contract: the exact engine SQL for the cd-1 shapes. The arcX
// differential harness executes these same strings (count_distinct_differential
// `router_emitted_sql`), so a router shape the engine would decline fails there.
func TestBuildCountDistinctEngineSQL(t *testing.T) {
	paths := "['/p.parquet']"
	cases := []struct{ sql, want string }{
		{"SELECT count(*), count(DISTINCT host) AS hosts FROM cpu WHERE v > 1",
			"SELECT count(*), count(DISTINCT host) AS hosts FROM read_parquet(['/p.parquet']) WHERE v > 1"},
		{"SELECT host, count(DISTINCT id) AS ids FROM cpu GROUP BY host HAVING ids > 3",
			"SELECT host, count(DISTINCT id) AS ids FROM read_parquet(['/p.parquet']) GROUP BY 1 HAVING count(DISTINCT id) > 3"},
		{"SELECT host, count(id) FROM cpu GROUP BY host HAVING count(DISTINCT id) >= 2",
			"SELECT host, count(id) FROM read_parquet(['/p.parquet']) GROUP BY 1 HAVING count(DISTINCT id) >= 2"},
		{"SELECT host, count(DISTINCT id) FROM cpu GROUP BY host ORDER BY count(DISTINCT id) DESC LIMIT 4",
			"SELECT host, count(DISTINCT id) FROM read_parquet(['/p.parquet']) GROUP BY 1 ORDER BY 2 DESC NULLS LAST LIMIT 4"},
		{"SELECT date_trunc('hour', time), count(DISTINCT id) FROM cpu GROUP BY 1 ORDER BY 1",
			"SELECT date_trunc('hour', time), count(DISTINCT id) FROM read_parquet(['/p.parquet']) GROUP BY 1 ORDER BY 1 ASC NULLS LAST"},
	}
	for _, c := range cases {
		d := decisionFor(t, c.sql)
		var got string
		var ok bool
		if d.Shape == ShapeScanAgg {
			got, ok = buildScanAggSQL(d, paths)
		} else {
			got, ok = buildGroupedSQL(d, paths)
		}
		if !ok || got != c.want {
			t.Errorf("%s\n got (%q, %t)\nwant %q", c.sql, got, ok, c.want)
		}
	}
}
