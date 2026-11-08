// agg-6b'' companion: the recognizer accepts predicate arguments of
// count_if/countif/bool_and/bool_or exactly where the engine serves them
// (ungrouped, bucket-grouped), re-serialized from validated tokens, and declines
// what the engine declines (IN, NOT, arithmetic, non-canonical literals, timestamp
// strings, tag-keyed grouping, HAVING references). Pure tokenizer tests.

package arcxrouter

import (
	"reflect"
	"testing"
)

func TestScanAggPredicateArgs(t *testing.T) {
	cases := []struct {
		sql   string
		items []string
	}{
		{"SELECT count_if(status >= 500) FROM http", []string{"count_if(status >= 500)"}},
		{"SELECT count(*), COUNT_IF(Status<>200), bool_and(cpu < 90.5), bool_or(host = 'web''1') FROM cpu",
			[]string{"count(*)", "count_if(Status != 200)", "bool_and(cpu < 90.5)", "bool_or(host = 'web''1')"}},
		{"SELECT countif(x BETWEEN -1 AND 2.5 OR s IS NOT NULL) FROM t WHERE x > 0",
			[]string{"countif(x BETWEEN -1 AND 2.5 OR s IS NOT NULL)"}},
		{"SELECT count_if((a > 0 AND (b < 1 OR c LIKE 'x%')) AND d NOT LIKE '%y') FROM t",
			[]string{"count_if((a > 0 AND (b < 1 OR c LIKE 'x%')) AND d NOT LIKE '%y')"}},
		{"SELECT count_if(s IS NULL), bool_or(level = 0), count_if(value > 0.000001) FROM t",
			[]string{"count_if(s IS NULL)", "bool_or(level = 0)", "count_if(value > 0.000001)"}},
		// The agg-6b' Boolean-column form is untouched.
		{"SELECT count_if(b), bool_and(b) FROM t", []string{"count_if(b)", "bool_and(b)"}},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok {
			t.Fatalf("should be eligible: %s", c.sql)
		}
		if m.shape != ShapeScanAgg {
			t.Fatalf("shape = %q, want scan_agg: %s", m.shape, c.sql)
		}
		if !reflect.DeepEqual(m.aggItems, c.items) {
			t.Fatalf("items = %v, want %v: %s", m.aggItems, c.items, c.sql)
		}
	}
}

func TestGroupedAggPredicateArgs(t *testing.T) {
	sql := "SELECT date_trunc('hour', time), count(*), count_if(cpu_user > 90) FROM cpu WHERE host = 'a' GROUP BY 1"
	m, ok := eligibleShape(sql)
	if !ok {
		t.Fatalf("should be eligible: %s", sql)
	}
	want := []string{"date_trunc('hour', time)", "count(*)", "count_if(cpu_user > 90)"}
	if !reflect.DeepEqual(m.aggItems, want) {
		t.Fatalf("items = %v, want %v", m.aggItems, want)
	}
}

func TestPredicateArgDeclines(t *testing.T) {
	for _, sql := range []string{
		// Grammar the engine declines.
		"SELECT count_if(s IN ('a', 'b')) FROM t",
		"SELECT count_if(s NOT IN ('a')) FROM t",
		"SELECT count_if(NOT x > 0) FROM t",
		"SELECT count_if(x * 2 > 1) FROM t",
		"SELECT count_if(x + 1 > 1) FROM t",
		"SELECT count_if(s ILIKE 'a%') FROM t",
		"SELECT count_if(s LIKE 'a\\%') FROM t",
		"SELECT count_if(m['k'] = 'v') FROM t",
		"SELECT count_if(t.x > 0) FROM t",
		"SELECT count_if(x > 0 AND) FROM t",
		"SELECT count_if((x > 0) FROM t",
		"SELECT count_if(true) FROM t",
		"SELECT count_if(1) FROM t",
		"SELECT count_if(end > 0) FROM t",
		"SELECT count_if(x IS 0) FROM t",
		// Non-canonical literals (1.5.6 renders them differently).
		"SELECT count_if(i > 007) FROM t",
		"SELECT count_if(x > 00.5) FROM t",
		"SELECT count_if(i > -0) FROM t",
		"SELECT count_if(x > -0.0) FROM t",
		"SELECT count_if(x > +2) FROM t",
		"SELECT count_if(x > - 1) FROM t",
		"SELECT count_if(i > 99999999999999999999) FROM t",
		// A string the engine lexes as a timestamp literal.
		"SELECT count_if(time > '2024-01-01T00:00:00Z') FROM t",
		// Lexer mirrors (also WHERE-wide): `!=-` and >15-digit decimals.
		"SELECT count_if(x!=-2) FROM t",
		"SELECT count(*) FROM t WHERE x!=-2",
		"SELECT count_if(x > 1.2345678901234567) FROM t",
		"SELECT count(*) FROM t WHERE x > 0.89555979711471049",
		// Predicate arguments only on the four Boolean aggregates.
		"SELECT sum(x > 0) FROM t",
		"SELECT count(x > 0) FROM t",
		"SELECT bit_or(i > 0) FROM t",
		// Tag-keyed grouping stays gated (momentAggFns), as for the column form.
		"SELECT host, count_if(x > 0) FROM t GROUP BY host",
		// HAVING on a predicate aggregate: the engine declines it (agg-6b'' B1).
		"SELECT date_trunc('hour', time), count_if(x > 0) FROM t GROUP BY 1 HAVING count_if(x > 0) > 3",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s (items %v)", sql, m.aggItems)
		}
	}
	// A 15-digit decimal still routes.
	if _, ok := eligibleShape("SELECT count(*) FROM t WHERE x > 0.12345678901234"); !ok {
		t.Fatalf("a 15-digit decimal should still be eligible")
	}
	// `!=` with a space before the sign still routes.
	if _, ok := eligibleShape("SELECT count_if(x != -2) FROM t"); !ok {
		t.Fatalf("`x != -2` should be eligible")
	}
}

func TestIsPredArgItem(t *testing.T) {
	for _, item := range []string{
		"count_if(x > 0)", "countif(a = 'b''c' OR b IS NULL)", "bool_and((x > 0 AND y < 1))",
		"bool_or(s NOT LIKE 'a%')", "count_if(x BETWEEN -1 AND 2.5)",
	} {
		if !isPredArgItem(item) {
			t.Fatalf("isPredArgItem(%q) = false", item)
		}
	}
	for _, item := range []string{
		"count_if(x > 0) FROM t", "count_if(x  > 0)", "count_if(x <> 0)", "sum(x > 0)",
		"count_if(x > 0); DROP", "count_if(x > 007)", "count_if()", "count_if(x IN (1))",
	} {
		if isPredArgItem(item) {
			t.Fatalf("isPredArgItem(%q) = true", item)
		}
	}
}
