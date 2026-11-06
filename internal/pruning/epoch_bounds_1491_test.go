package pruning

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1491: a bound written as epoch_ms(n), make_timestamp(n) or to_timestamp(x)
// of one numeric literal prunes like the TIMESTAMP literal it equals.
func TestTimeRangeReadsEpochCalls(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	const h10, h11 = "2026-10-01T10:00:00Z", "2026-10-01T11:00:00Z" // 1790848800 s, 1790852400 s
	for _, tc := range []struct {
		sql, start, end string
	}{
		{"SELECT * FROM db.q WHERE time >= epoch_ms(1790848800000) AND time < epoch_ms(1790852400000)", h10, h11},
		{"SELECT * FROM db.q WHERE time >= EPOCH_MS( 1790848800000 ) AND time < Epoch_Ms(1790852400000)", h10, h11},
		{"SELECT * FROM db.q WHERE time >= make_timestamp(1790848800000000) AND time < make_timestamp(1790852400000000)", h10, h11},
		{"SELECT * FROM db.q WHERE epoch_ms(1790848800000) <= time AND epoch_ms(1790852400000) > time AND symbol = 'EURUSD'", h10, h11},
		{"SELECT * FROM db.q WHERE time >= epoch_ms(1790848800000) AND time < '2026-10-01 11:00:00'", h10, h11},
		{"SELECT * FROM db.q q WHERE (q.time >= epoch_ms(1790848800000)) AND (time) < epoch_ms(1790852400000)", h10, h11},
		// Inclusive ends move one microsecond past the instant, as for literals.
		{"SELECT * FROM db.q WHERE time >= epoch_ms(1790848800000) AND time <= epoch_ms(1790852400000)", h10, "2026-10-01T11:00:00.000001Z"},
		{"SELECT * FROM db.q WHERE time BETWEEN epoch_ms(1790848800000) AND epoch_ms(1790852400000)", h10, "2026-10-01T11:00:00.000001Z"},
		// to_timestamp goes through a double in DuckDB: widened a second on the open side.
		{"SELECT * FROM db.q WHERE time >= to_timestamp(1790848800) AND time < to_timestamp(1790852400)", "2026-10-01T09:59:59Z", "2026-10-01T11:00:01Z"},
		{"SELECT * FROM db.q WHERE time > to_timestamp(1790848800.25) AND time <= to_timestamp(1790852400.5)", "2026-10-01T09:59:59.25Z", "2026-10-01T11:00:01.500001Z"},
		{"SELECT * FROM db.q WHERE time BETWEEN to_timestamp(1790848800) AND to_timestamp(1790852400)", "2026-10-01T09:59:59Z", "2026-10-01T11:00:01.000001Z"},
		// Negative values, and floor (not truncation) for a negative decimal: -1.9 s is 1969-12-31 23:59:58.1.
		{"SELECT * FROM db.q WHERE time >= epoch_ms(-1) AND time < epoch_ms(1000)", "1969-12-31T23:59:59.999Z", "1970-01-01T00:00:01Z"},
		{"SELECT * FROM db.q WHERE time >= to_timestamp(-1.9) AND time < to_timestamp(-1.9)", "1969-12-31T23:59:57.1Z", "1969-12-31T23:59:59.1Z"},
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
}

// Shapes that look like an epoch call but are not one plain literal argument,
// or that DuckDB evaluates to something else: each gives no bound from that
// term, so the query reads every partition, as before #1491.
func TestTimeRangeIgnoresOtherEpochShapes(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	for _, term := range []string{
		"time >= epoch_ms(1790848800000) + INTERVAL '1 hour'", // arithmetic after the call
		"time >= epoch_ms(1790848800000)::DATE",               // a cast after it
		"time >= epoch_ms(1790848800000 + 1)",                 // arithmetic inside
		"time >= epoch_ms(ts_ms)",                             // a column
		"time >= epoch_ms(1790848800000::BIGINT)",             // a cast inside
		"time >= epoch_ms(+1790848800000)",                    // unary plus
		"time >= epoch_ms(1_790_848_800_000)",                 // digit separators
		"time >= epoch_ms(0x10)",                              // hex
		"time >= epoch_ms(1.7908488e12)",                      // exponent
		"time >= epoch_ms(1790848800000.5)",                   // decimal: a binder error in DuckDB
		"time >= make_timestamp(1.5)",                         // decimal: a binder error in DuckDB
		"time >= epoch_ms(99999999999999999999)",              // past int64: a binder error in DuckDB
		"time >= main.epoch_ms(1790848800000)",                // qualified
		"time >= make_timestamp(2026, 10, 1, 10, 0, 0)",       // the six-argument form
		"time >= make_timestamptz(1790848800000000)",          // another function
		"time >= epoch_ms(time)",                              // the reverse function
		"time >= to_timestamp(1.)",                            // no digits after the point
		"time >= to_timestamp('1790848800')",                  // a string
		"time >= epoch(1790848800)",                           // not one of the three
		// Years past 9999: a literal cannot spell them, and a consumer that
		// compares the range as text (HasColdFilesInRange) reads them wrong.
		"time >= epoch_ms(253402300800000)",
		"time >= make_timestamp(9223372036854775806)",
		"time >= to_timestamp(253402300800)",
		"time >= make_timestamp(-9223372022400000000)",
	} {
		q := "SELECT * FROM db.q WHERE " + term
		if tr := p.ExtractTimeRange(q); tr != nil {
			t.Errorf("range %v..%v for %q, want none", tr.Start, tr.End, q)
		}
		// Beside a bound that is read, the term is ignored: the range is that
		// bound's alone, never narrowed by the other term.
		q2 := "SELECT * FROM db.q WHERE time < '2026-10-01 11:00:00' AND " + term
		tr := p.ExtractTimeRange(q2)
		if tr == nil || !tr.StartAssumed || !tr.End.Equal(mustT(t, "2026-10-01T11:00:00Z")) {
			t.Errorf("range %+v for %q, want an assumed start and the literal's end", tr, q2)
		}
	}
	// An end bound far in the future (an operator's "until forever") is not
	// read either, rather than read past year 9999.
	if tr := p.ExtractTimeRange("SELECT * FROM db.q WHERE time >= '2026-10-01' AND time < epoch_ms(253402300800000)"); tr == nil || !tr.EndAssumed {
		t.Errorf("range %+v, want the literal start and an assumed end", tr)
	}
}

// The new shapes are bounds only: they do not feed the empty-range proof or
// the start-only shortcut, which were built for literals and now().
func TestEpochCallBoundsAreNotProofSafe(t *testing.T) {
	for _, q := range []string{
		"SELECT * FROM db.q WHERE time >= epoch_ms(1790848800000) AND time < epoch_ms(1790852400000)",
		"SELECT * FROM db.q WHERE time >= make_timestamp(1790848800000000) AND time < '2026-10-01 11:00:00'",
		"SELECT * FROM db.q WHERE time >= to_timestamp(1790848800) AND time < '2026-10-01 11:00:00'",
	} {
		if IsExactTimeRange(q) {
			t.Errorf("IsExactTimeRange(%q) = true, want false", q)
		}
	}
	if IsStartOnlyTimeRange("SELECT * FROM db.q WHERE time >= epoch_ms(1790848800000)") {
		t.Error("IsStartOnlyTimeRange = true for an epoch_ms start, want false")
	}
	// The literal forms keep their proof.
	if !IsExactTimeRange("SELECT * FROM db.q WHERE time >= '2026-10-01 10:00:00' AND time < '2026-10-01 11:00:00'") {
		t.Error("IsExactTimeRange = false for literals, want true")
	}
}

// epochCallTime's year limits sit exactly at 0001-01-01 and 10000-01-01.
func TestEpochCallTimeYearLimits(t *testing.T) {
	last := time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC)
	for call, want := range map[string]bool{
		"make_timestamp(253402300799999999)":  true,  // 9999-12-31 23:59:59.999999
		"make_timestamp(253402300800000000)":  false, // 10000-01-01
		"make_timestamp(-62135596800000000)":  true,  // 0001-01-01
		"make_timestamp(-62135596800000001)":  false,
		"to_timestamp(253402300799)":          true,
		"to_timestamp(253402300801)":          false,
		"to_timestamp(-99999999999999999999)": false,
		"epoch_ms(-9223372036854775808)":      false, // int64 min
		"epoch_ms(9223372036854775807)":       false, // int64 max
		"epoch_ms(9223372036854775808)":       false, // past int64
	} {
		got, ok := epochCallTime(call)
		if ok != want {
			t.Errorf("epochCallTime(%s) ok=%v (%v), want %v", call, ok, got, want)
		}
	}
	if got, ok := epochCallTime("make_timestamp(253402300799999999)"); !ok || !got.Equal(last) {
		t.Errorf("epochCallTime = %v, %v; want %v", got, ok, last)
	}
}
