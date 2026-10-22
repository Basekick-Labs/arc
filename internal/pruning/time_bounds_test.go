package pruning

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func mustT(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := parseDateTime(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// Shapes that pruned before and must keep pruning: dashboards (Grafana's
// $__timeFilter, relative windows), with other filters ANDed alongside.
func TestTimeRangeKeepsPruningCommonShapes(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	for _, tc := range []struct {
		sql, start, end string
	}{
		{"SELECT * FROM db.cpu WHERE (time) >= '2026-10-20T10:00:00Z' AND (time) < '2026-10-20T11:00:00Z'", "2026-10-20T10:00:00Z", "2026-10-20T11:00:00Z"},
		{"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND time < '2026-10-20 11:00:00' AND host IN ('a','b')", "2026-10-20T10:00:00Z", "2026-10-20T11:00:00Z"},
		{"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND (host='a' OR host='b') AND time < '2026-10-20 11:00:00'", "2026-10-20T10:00:00Z", "2026-10-20T11:00:00Z"},
		{"SELECT * FROM db.cpu WHERE \"time\" >= '2026-10-20 10:00:00' AND \"time\" < '2026-10-20 11:00:00'", "2026-10-20T10:00:00Z", "2026-10-20T11:00:00Z"},
		{"SELECT * FROM db.cpu c WHERE (c.time >= '2026-10-20 10:00:00' AND c.time < '2026-10-20 11:00:00') AND host = 'x'", "2026-10-20T10:00:00Z", "2026-10-20T11:00:00Z"},
		{"SELECT * FROM db.cpu WHERE '2026-10-20 10:00:00' <= time AND '2026-10-20 11:00:00' > time", "2026-10-20T10:00:00Z", "2026-10-20T11:00:00Z"},
		{"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00'::timestamp AND time < TIMESTAMP '2026-10-20 11:00:00'", "2026-10-20T10:00:00Z", "2026-10-20T11:00:00Z"},
		{"SELECT * FROM db.cpu WHERE time BETWEEN '2026-10-20 10:00:00' AND '2026-10-20 10:59:59' GROUP BY host", "2026-10-20T10:00:00Z", "2026-10-20T10:59:59.000001Z"},
		{"SELECT time_bucket(INTERVAL '1 minute', time) b, count(*) FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND time < '2026-10-20 11:00:00' GROUP BY b ORDER BY b LIMIT 10", "2026-10-20T10:00:00Z", "2026-10-20T11:00:00Z"},
	} {
		tr := p.ExtractTimeRange(tc.sql)
		if tr == nil {
			t.Errorf("no range for %q", tc.sql)
			continue
		}
		if !tr.Start.Equal(mustT(t, tc.start)) || !tr.End.Equal(mustT(t, tc.end)) {
			t.Errorf("range %v..%v for %q, want %s..%s", tr.Start, tr.End, tc.sql, tc.start, tc.end)
		}
	}
	// Relative windows, quoted and unquoted.
	for _, q := range []string{
		"SELECT * FROM db.cpu WHERE time > now() - INTERVAL '5 minutes'",
		"SELECT * FROM db.cpu WHERE time >= NOW() - INTERVAL 5 MINUTE AND host = 'a'",
		"SELECT * FROM db.cpu WHERE time > CURRENT_TIMESTAMP - INTERVAL '5 minutes' ORDER BY time DESC LIMIT 100",
	} {
		tr := p.ExtractTimeRange(q)
		if tr == nil {
			t.Errorf("no range for %q", q)
			continue
		}
		if d := time.Since(tr.Start); d < 4*time.Minute || d > 6*time.Minute {
			t.Errorf("start %v (%v ago) for %q, want about 5 minutes ago", tr.Start, d, q)
		}
	}
}

// Shapes that read a range narrower than the rows the query returns. Each one
// lost rows on main; now each gives no range (or, for inclusive ends, a range
// that reaches the end's hour).
func TestTimeRangeNeverNarrowerThanTheQuery(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	for _, q := range []string{
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' OR host = 'a'",
		"SELECT * FROM db.cpu WHERE host = 'a' OR time >= '2026-10-20 10:00:00' AND host = 'b'",
		"SELECT * FROM db.cpu WHERE downtime >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE end_time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE timestamp >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE ts::timestamp >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE NOT (time >= '2026-10-20 10:00:00')",
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 11:00:00'::timestamp - INTERVAL '3 hours'",
		"SELECT * FROM db.cpu WHERE time > now() - INTERVAL '1 day' - INTERVAL '6 hours'",
		"SELECT * FROM db.cpu WHERE time > now() - INTERVAL '10 minutes' * 3",
		"SELECT * FROM db.cpu WHERE host = 'a'\n-- AND time >= '2026-10-20 10:00:00'\n",
		"SELECT * FROM db.cpu WHERE host = 'a' /* AND time >= '2026-10-20 10:00:00' */",
		"SELECT * FROM db.cpu WHERE CASE WHEN time >= '2026-10-20 10:00:00' THEN true ELSE host = 'a' END",
		"SELECT * FROM db.cpu WHERE host = 'a' QUALIFY time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE (time >= '2026-10-20 10:00:00' OR time < '2026-10-20 08:00:00')",
		"SELECT * FROM db.cpu c JOIN db.mem m ON true WHERE c.time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu c, db.mem m WHERE c.time >= '2026-10-20 10:00:00'",
		"SELECT time FROM db.cpu WHERE time >= '2026-10-20 10:00:00' UNION ALL SELECT time FROM db.mem",
		"WITH m AS (SELECT * FROM db.mem) SELECT * FROM db.cpu, m WHERE cpu.time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.mem WHERE time < (SELECT max(time) FROM db.cpu WHERE time >= '2026-10-20 11:00:00')",
		"SELECT * FROM (SELECT time, lag(time) OVER (ORDER BY time) p FROM db.cpu) WHERE time >= '2026-10-20 10:00:00'",
		"SELECT * FROM (SELECT host, max(time) AS time FROM db.cpu GROUP BY host) s WHERE time >= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE host IN (SELECT host FROM db.mem WHERE time >= '2026-10-20 10:00:00')",
		"SELECT * FROM db.cpu WHERE time NOT BETWEEN '2026-10-20 10:00:00' AND '2026-10-20 11:00:00'",
	} {
		if tr := p.ExtractTimeRange(q); tr != nil && !tr.StartAssumed {
			t.Errorf("range %v..%v for %q, want none", tr.Start, tr.End, q)
		}
	}
	// Inclusive end on an hour boundary reaches that hour.
	for _, q := range []string{
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 08:00:00' AND time <= '2026-10-20 10:00:00'",
		"SELECT * FROM db.cpu WHERE time BETWEEN '2026-10-20 08:00:00' AND '2026-10-20 10:00:00'",
	} {
		tr := p.ExtractTimeRange(q)
		if tr == nil || !tr.End.After(mustT(t, "2026-10-20 10:00:00")) {
			t.Errorf("inclusive end not past 10:00 for %q: %+v", q, tr)
		}
	}
	// End only: the start is the partition floor, never 2020.
	tr := p.ExtractTimeRange("SELECT * FROM db.cpu WHERE time < '2024-01-01'")
	if tr == nil || !tr.StartAssumed || tr.Start.After(mustT(t, "1971-01-01")) {
		t.Errorf("end-only range %+v, want an assumed start at the partition floor", tr)
	}
}

// Each guard of the scanner, on a query that reads a too-narrow range without
// it (the structural shapes above are covered by more than one guard).
func TestTimeRangeGuards(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	for name, q := range map[string]string{
		// AND binds tighter than OR: (time >= X AND host='a') OR host='b'.
		"top-level OR after AND": "SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND host = 'a' OR host = 'b'",
		// A comment's text is not masked, so only an unquoted interval in it
		// could read as a bound.
		"commented unquoted interval":       "SELECT * FROM db.cpu WHERE host = 'a'\n-- AND time > now() - INTERVAL 5 MINUTE\n",
		"block-commented unquoted interval": "SELECT * FROM db.cpu WHERE host = 'a' /* AND time > now() - INTERVAL 5 MINUTE */",
		// QUALIFY filters window results, not the rows read.
		"QUALIFY with AND": "SELECT * FROM db.cpu WHERE host = 'a' QUALIFY host = 'b' AND time >= '2026-10-20 10:00:00'",
		"HAVING with AND":  "SELECT host FROM db.cpu WHERE host <> 'z' GROUP BY host HAVING count(*) > 1 AND max(time) >= '2026-10-20 10:00:00'",
	} {
		if tr := p.ExtractTimeRange(q); tr != nil {
			t.Errorf("%s: range %v..%v, want none", name, tr.Start, tr.End)
		}
	}
}

// exact is false when a term mentions the time column without being a bare
// bound: the bounds found are then not the query's whole time predicate.
func TestTimeRangeExactness(t *testing.T) {
	for q, want := range map[string]bool{
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND time < '2026-10-20 11:00:00' AND downtime > 5":                       true,
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND time < '2026-10-20 11:00:00' AND host = 'a'":                         true,
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND date_trunc('hour', time) = '2026-10-20 10:00:00'":                    false,
		"SELECT * FROM db.cpu WHERE time >= '2026-10-20 10:00:00' AND time < '2026-10-20 11:00:00' AND NOT (time = '2026-10-20 10:30:00')": false,
	} {
		b, ok := scanTimeBounds(q)
		if !ok || b.exact != want {
			t.Errorf("exact=%v ok=%v for %q, want exact=%v", b.exact, ok, q, want)
		}
	}
}

// The exact-shape check behind the empty-range proof and the open-ended
// shortcut reads through the scanner: a commented-out or QUALIFY bound is not
// part of the query's range.
func TestExactShapeIgnoresCommentsAndQualify(t *testing.T) {
	for _, q := range []string{
		"SELECT * FROM cpu WHERE time >= '2026-02-01' AND host = 'x' /* AND time < '2026-02-02' */",
		"SELECT * FROM cpu WHERE time >= '2026-02-01' AND host = 'x' -- AND time < '2026-02-02'",
		"SELECT * FROM cpu WHERE time >= '2026-02-01' AND host = 'x' QUALIFY time < '2026-02-02'",
	} {
		if IsExactTimeRange(q) {
			t.Errorf("IsExactTimeRange true for %q: its end is not part of the WHERE", q)
		}
	}
}

// Several bounds on one end: the narrowest holds for every row.
func TestTimeRangeIntersectsBounds(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	tr := p.ExtractTimeRange("SELECT * FROM db.cpu WHERE time >= '2026-10-20 08:00:00' AND time >= '2026-10-20 10:00:00' AND time < '2026-10-20 12:00:00' AND time < '2026-10-20 11:00:00'")
	if tr == nil || !tr.Start.Equal(mustT(t, "2026-10-20 10:00:00")) || !tr.End.Equal(mustT(t, "2026-10-20 11:00:00")) {
		t.Fatalf("range %+v, want 10:00..11:00", tr)
	}
}
