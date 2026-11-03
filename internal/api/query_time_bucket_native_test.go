package api

import (
	"fmt"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/database"
	"github.com/rs/zerolog"
)

// The epoch rewrites of time_bucket and date_trunc must bucket every row
// exactly as DuckDB's own functions do (#1352): the last half second of a
// bucket stays in it, buckets wider than a day line up on DuckDB's Monday
// origin, and instants before the origin and before 1970 floor, not round
// toward zero.
func TestTimeBucketRewritesMatchNativeDuckDB(t *testing.T) {
	db, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 1, MaxConnections: 1}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	instants := []string{
		"2026-10-23 03:29:59.7", "2026-10-23 03:29:59.5", "2026-10-23 03:29:59.499999", "2026-10-23 03:29:59.999999",
		"2026-10-23 03:30:00", "2026-10-23 03:30:00.000001", "2026-10-19 00:00:00", "2026-10-18 23:59:59.6",
		"2000-01-03 00:00:00", "2000-01-02 23:59:59.9", "1999-12-31 12:00:00.5", "1970-01-01 00:00:00.5",
		"1969-12-31 23:59:59.5", "1950-06-01 12:00:00.4", "2100-02-28 23:59:59.999999", "2024-02-29 11:59:59.75",
		// Centuries from the origin: a double quotient loses the microsecond here.
		"1500-03-01 11:59:59.999999", "1700-06-15 00:00:00.5", "2600-12-31 23:59:59.999999", "2300-01-01 00:00:00.000001",
	}
	vals := make([]string, len(instants))
	for i, s := range instants {
		vals[i] = fmt.Sprintf("(TIMESTAMPTZ '%s+00')", s)
	}
	from := "(VALUES " + strings.Join(vals, ", ") + ") v(time)"

	check := func(native, rewritten string) {
		t.Helper()
		if rewritten == native {
			t.Fatalf("%s was not rewritten", native)
		}
		q := fmt.Sprintf("SELECT count(*) FILTER (WHERE (%s) IS DISTINCT FROM (%s)), string_agg(CASE WHEN (%s) IS DISTINCT FROM (%s) THEN time::VARCHAR || ' native ' || (%s)::VARCHAR || ' rewritten ' || (%s)::VARCHAR END, '; ') FROM %s",
			native, rewritten, native, rewritten, native, rewritten, from)
		var n int
		var detail *string
		if err := db.DB().QueryRow(q).Scan(&n, &detail); err != nil {
			t.Fatalf("%s: %v", rewritten, err)
		}
		if n != 0 {
			t.Errorf("%s disagrees with %s on %d instants: %s", rewritten, native, n, *detail)
		}
	}

	for _, iv := range []string{"1 second", "30 seconds", "1 minute", "15 minutes", "1 hour", "7 hours", "1 day", "2 days", "5 days", "1 week", "3 weeks"} {
		native := fmt.Sprintf("time_bucket(INTERVAL '%s', time)", iv)
		check(native, rewriteTimeBucket(native))
		// Origins: a usual one, one with a fractional second, one centuries
		// after the data.
		for _, o := range []string{"2024-01-01 00:30:00", "2024-01-01 00:30:00.5", "2400-01-01 00:00:00"} {
			withOrigin := fmt.Sprintf("time_bucket(INTERVAL '%s', time, TIMESTAMPTZ '%s+00')", iv, o)
			check(withOrigin, rewriteTimeBucket(fmt.Sprintf("time_bucket(INTERVAL '%s', time, TIMESTAMP '%s')", iv, o)))
		}
	}
	for _, unit := range []string{"second", "minute", "hour", "day", "week"} {
		native := fmt.Sprintf("date_trunc('%s', time)", unit)
		check(native, rewriteDateTrunc(native))
	}
}
