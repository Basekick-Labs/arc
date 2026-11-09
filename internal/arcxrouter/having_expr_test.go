// res-3c companion: HAVING expressions (arcX feat/res-3c).

package arcxrouter

import (
	"strings"
	"testing"
)

func TestHavingExprRecognized(t *testing.T) {
	const b = "SELECT date_trunc('hour', time) AS t, "
	cases := []struct {
		sql      string
		having   string
		hidden   int
		unstable string
	}{
		// Arithmetic over aggregates, hidden aggregates and literals; emitted from
		// validated parts, tokens space-separated, literals as spelled.
		{"SELECT host FROM cpu GROUP BY host HAVING sum(bytes) / count(*) > 1024", "sum(bytes) / count(*) > 1024", 2, ""},
		{"SELECT host, max(x), min(x) FROM cpu GROUP BY host HAVING max(x) - min(x) > 10", "max(x) - min(x) > 10", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*)*1.5>=max(code)", "count(*) * 1.5 >= max(code)", 1, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING -count(*) < -5", "- count(*) < -5", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING (count(*) + 1) * 2 BETWEEN 4 AND 10", "(count(*) + 1) * 2 BETWEEN 4 AND 10", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING sum(code) / count(*) IS NULL", "sum(code) / count(*) IS NULL", 1, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 1e3", "count(*) > 1e3", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) >= 2.5E-1", "count(*) >= 2.5E-1", 0, ""},
		// A signed number after an operand is a binary minus (review M2).
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*)-1 > 5", "count(*) - 1 > 5", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING min(code) -2 * max(code) > 0", "min(code) - 2 * max(code) > 0", 2, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING (count(*)) -1 > 5", "(count(*)) - 1 > 5", 0, ""},
		// One float-accumulated item, through * or / (review H3).
		{"SELECT host, avg(x) FROM cpu GROUP BY host HAVING avg(x) * 2 > 5", "avg(x) * 2 > 5", 0, ""},
		{"SELECT host, avg(x) FROM cpu GROUP BY host HAVING avg(x) * count(*) > 5", "avg(x) * count(*) > 5", 1, ""},
		{"SELECT host, avg(x) FROM cpu GROUP BY host HAVING avg(x) + 1 > 5", "avg(x) + 1 > 5", 0, ""},
		{"SELECT host FROM cpu GROUP BY host HAVING arg_max(x, time) * 2 > 1", "arg_max(x, time) * 2 > 1", 1, "arg"},
		// Expression vs expression / reference, all order-exact.
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 > max(code) * 2", "count(*) + 1 > max(code) * 2", 1, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING 2 * max(code) < count(*)", "2 * max(code) < count(*)", 1, ""},
		// Grouping vs operand parentheses, NOT.
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING (count(*) > 1) AND (max(code) + 1 > 2)", "(count(*) > 1) AND (max(code) + 1 > 2)", 1, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING NOT (count(*) + 1 > 2)", "NOT (count(*) + 1 > 2)", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING ((count(*)) + 1) > 2", "((count(*)) + 1) > 2", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING (count(*) + 1) IN (2, 3e0)", "(count(*) + 1) IN (2, 3e0)", 0, ""},
		// A scientific bound over a bare reference (review H2: DOUBLE common type).
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) BETWEEN 1 AND 1e20", "count(*) BETWEEN 1 AND 1e20", 0, ""},
		// IS [NOT] DISTINCT FROM.
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IS DISTINCT FROM 5", "count(*) IS DISTINCT FROM 5", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING host IS NOT DISTINCT FROM 'a'", "host IS NOT DISTINCT FROM 'a'", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING max(code) IS NOT DISTINCT FROM NULL", "max(code) IS NOT DISTINCT FROM NULL", 1, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IS DISTINCT FROM max(code)", "count(*) IS DISTINCT FROM max(code)", 1, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 IS DISTINCT FROM max(code) * 2", "count(*) + 1 IS DISTINCT FROM max(code) * 2", 1, ""},
		{b + "max(time) FROM cpu GROUP BY 1 HAVING t IS DISTINCT FROM max(time)", "t IS DISTINCT FROM max(time)", 0, ""},
		// IS [NOT] TRUE / FALSE, [NOT] LIKE.
		{b + "bool_and(ok) FROM cpu GROUP BY 1 HAVING bool_and(ok) IS NOT TRUE", "bool_and(ok) IS NOT TRUE", 0, ""},
		{b + "count(*) FROM cpu GROUP BY 1 HAVING bool_or(ok) IS FALSE", "bool_or(ok) IS FALSE", 1, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING host LIKE 'web-%'", "host LIKE 'web-%'", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING host NOT LIKE 'it''s_%'", "host NOT LIKE 'it''s_%'", 0, ""},
		{"SELECT host, count(*) FROM cpu GROUP BY host HAVING NOT host LIKE 'a%' AND count(*) * 2 > 1", "NOT host LIKE 'a%' AND count(*) * 2 > 1", 0, ""},
	}
	for _, c := range cases {
		m, ok := eligibleShape(c.sql)
		if !ok || m.shape != ShapeScanAggGrouped {
			t.Fatalf("should route: %s", c.sql)
		}
		if m.havingText != c.having || m.havingHidden != c.hidden || m.havingUnstable != c.unstable {
			t.Fatalf("%s\n got having=%q hidden=%d unstable=%q\nwant having=%q hidden=%d unstable=%q",
				c.sql, m.havingText, m.havingHidden, m.havingUnstable, c.having, c.hidden, c.unstable)
		}
	}
}

func TestHavingExprDecline(t *testing.T) {
	const b = "SELECT date_trunc('hour', time) AS t, "
	for _, sql := range []string{
		// Operators and literal forms outside the slice.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) // 2 > 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING +count(*) > 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 1e400",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 1.e3",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) * -0e0 = 0",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) / - 0e0 < 0",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) / -(0e0) < 0",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING max(x) > -0e0",
		// Constant-only nodes, literal-only comparisons, NULL in arithmetic.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING 2 * 3 * count(*) > 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) > 1 + 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING 1 + 1 > 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + NULL > 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING 1 IS NULL",
		// Review H1 / M2.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING max(code) * -1 > -9223372036854775808",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*)-9223372036854775808 < 0",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING max(code) + 1 IN (-9223372036854775808, 1)",
		// Leaves the engine cannot type in arithmetic.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host + 1 > 1",
		b + "count(*) FROM cpu GROUP BY 1 HAVING t + 1 > 1",
		b + "count(*) FROM cpu GROUP BY 1 HAVING max(time) - min(time) > 1",
		b + "count(*) FROM cpu GROUP BY 1 HAVING bool_and(ok) + 1 > 1",
		b + "count(*) FROM cpu GROUP BY 1 HAVING regr_count(y, x) + 1 > 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 = 'a'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 = TRUE",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host BETWEEN 'a' AND 1e0",
		// Review H3.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING sum(x) - sum(y) > 0",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING avg(x) - max(x) < 0",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING avg(x) + count(*) > 3",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING avg(x) * avg(y) > 3",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING avg(x) * 2 > max(x)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING sum(code) / count(*) > max(code)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING avg(x) IS DISTINCT FROM max(x)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING -(avg(x) - count(*)) > 0",
		// Forms outside the grammar.
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 LIKE '1%'",
		b + "count(*) FROM cpu GROUP BY 1 HAVING bool_and(ok) LIKE 't%'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host LIKE 'a\\%'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host LIKE 'a%' ESCAPE '!'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING host LIKE host",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 IS TRUE",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) BETWEEN 1 AND max(code)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 IN (1, NULL)",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) + 1 BETWEEN 'a' AND 'b'",
		"SELECT host, count(*) FROM cpu GROUP BY host HAVING count(*) IS DISTINCT 5",
		// Review L4: a scientific literal still refuses in WHERE and elsewhere.
		"SELECT host, count(*) FROM cpu WHERE x > 1e0 GROUP BY host",
		"SELECT x FROM cpu WHERE x > 1e0",
		"SELECT host, count(*) FROM cpu GROUP BY host ORDER BY 2 LIMIT 1e1",
	} {
		if m, ok := eligibleShape(sql); ok {
			t.Fatalf("should decline: %s (having %q)", sql, m.havingText)
		}
	}
}

// The operand node budget mirrors the engine's MAX_ARITH_NODES (64 per comparison,
// counting every leaf, parenthesis, operator and unary minus) and operand
// parentheses share the predicate depth cap.
func TestHavingExprCaps(t *testing.T) {
	base := "SELECT host, count(*) FROM cpu GROUP BY host HAVING "
	// n terms joined by '+': 2n-1 nodes; the literal side adds one.
	sum := func(n int) string { return strings.Repeat("count(*) + ", n-1) + "count(*)" }
	if _, ok := eligibleShape(base + sum(31) + " > 1"); !ok {
		t.Fatal("62 nodes must route")
	}
	if _, ok := eligibleShape(base + sum(32) + " > 1"); !ok {
		t.Fatal("64 nodes must route")
	}
	if _, ok := eligibleShape(base + sum(33) + " > 1"); ok {
		t.Fatal("66 nodes must decline")
	}
	deep := func(n int) string { return strings.Repeat("(", n) + "count(*)" + strings.Repeat(")", n) + " + 1 > 1" }
	if _, ok := eligibleShape(base + deep(maxWhereDepth)); !ok {
		t.Fatalf("%d operand parentheses must route", maxWhereDepth)
	}
	if _, ok := eligibleShape(base + deep(maxWhereDepth+1)); ok {
		t.Fatal("operand parentheses past the depth cap must decline")
	}
}
