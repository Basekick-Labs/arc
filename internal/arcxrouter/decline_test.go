//go:build cgo && arcx_engine

package arcxrouter

import (
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/metrics"
)

// The classifier's whole value is separating shapes the recognizer collapses.
// These probes mirror the adversarial review's 18-shape corpus that proved the
// recognizer alone buckets 16 of them identically.
func TestCensusClassifySeparatesShapes(t *testing.T) {
	cases := []struct {
		sql, want string
	}{
		// A WHERE-bearing single-key grouped agg became ELIGIBLE at the mimalloc
		// slice; multi-key grouping remains the census representative.
		{"SELECT host, core, avg(usage_idle) FROM cpu WHERE usage_idle > 1.0 GROUP BY host, core", "group_by"},
		{"SELECT a.host FROM cpu a JOIN meta b ON a.host = b.host", "join"},
		{"WITH t AS (SELECT host FROM cpu) SELECT host FROM t", "cte"},
		{"SELECT host FROM cpu UNION SELECT host FROM mem", "set_op"},
		{"SELECT host, row_number() OVER (ORDER BY value) FROM cpu", "window"},
		{"SELECT host FROM cpu GROUP BY host HAVING count(host) > 1", "having"},
		{"SELECT DISTINCT host FROM cpu", "distinct"},
		{"SELECT host FROM cpu LIMIT 10 OFFSET 5", "offset"},
		{"SELECT * FROM cpu", "star_projection"},
		{"SELECT median(value) FROM cpu", "agg_fn"},
		{"SELECT upper(host) FROM cpu", "fn_other"},
		// Guards keep their own buckets (correctness gates, not missing features).
		{"SELECT host FROM cpu WHERE t AT TIME ZONE 'UTC' > 1", "tz_setting"},
		{"SELECT host FROM cpu ORDER BY host COLLATE nocase", "collation"},
		// Outside the lexer's vocabulary (`|` — string concat): an honest
		// bucket, not a guess. NOTE `+`/`/` DO lex (2e arithmetic), so plain
		// arithmetic lands in none_ineligible, which is correct.
		{"SELECT host || 'x' FROM cpu", "unlexable"},
		{"SELECT value + 1 FROM cpu", "none_ineligible"},
	}
	for _, c := range cases {
		got, _ := CensusClassify(c.sql, "production")
		if got != c.want {
			t.Errorf("%q: got %q, want %q", c.sql, got, c.want)
		}
	}
}

// THE LOAD-BEARING TEST (repo-owner hard constraint): no user bytes may reach
// any census output. Sentinel-laden SQL through every path; the returned label
// must be a member of the closed set and free of the sentinel — which also
// covers the DEBUG log and the metric, since both receive only this string.
func TestCensusNeverEmitsUserBytes(t *testing.T) {
	allowed := make(map[string]bool, len(declineReasonNames))
	for _, n := range declineReasonNames {
		allowed[n] = true
	}
	probes := []string{
		"SELECT zzsentinelzz(zzcolzz) FROM zzmeaszz WHERE h = 'ZZSENTINELZZ'",
		"SELECT zzcolzz FROM zzmeaszz GROUP BY zzcolzz",
		"WITH zzcte AS (SELECT 1) SELECT * FROM zzcte",
		"SELECT * FROM zzmeaszz WHERE zzcolzz AT TIME ZONE 'zz' > 1",
		"SELECT zzcolzz FROM zz-not-an-ident.zzmeaszz",
		"SELECT \x00zz FROM zzmeaszz",
	}
	for _, sql := range probes {
		got, _ := CensusClassify(sql, "zzheaderzz")
		if !allowed[got] {
			t.Fatalf("out-of-set label %q for %q — input-derived?", got, sql)
		}
		if strings.Contains(strings.ToLower(got), "zz") {
			t.Fatalf("sentinel leaked into label %q for %q", got, sql)
		}
	}
}

// Pins arcxrouter's reason table to internal/metrics' storage slots. The index
// IS the slot, so divergence silently re-labels dashboard history.
func TestArcxCensusReasonSetMatchesRouter(t *testing.T) {
	stored := metrics.ArcxCensusReasons()
	if len(stored) != int(numDeclineReasons) {
		t.Fatalf("metrics has %d reasons, router has %d", len(stored), numDeclineReasons)
	}
	for i, name := range declineReasonNames {
		if stored[i] != name {
			t.Fatalf("slot %d: metrics=%q router=%q", i, stored[i], name)
		}
	}
}
