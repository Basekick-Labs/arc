package storage

import (
	"testing"
	"time"
)

func TestStripRewriteSuffix(t *testing.T) {
	tests := map[string]string{
		"db/cpu/2026/10/01/cpu_20261001_000000_1.parquet":                       "db/cpu/2026/10/01/cpu_20261001_000000_1.parquet",
		"db/cpu/2026/10/01/cpu_20261001_daily.parquet":                          "db/cpu/2026/10/01/cpu_20261001_daily.parquet",
		"db/cpu/2026/10/01/cpu_20261001_daily_rewrite_123.parquet":              "db/cpu/2026/10/01/cpu_20261001_daily.parquet",
		"db/cpu/2026/10/01/cpu_20261001_000000_1_compacted_rewrite_123.parquet": "db/cpu/2026/10/01/cpu_20261001_000000_1_compacted.parquet",
		"db/cpu/2026/10/01/cpu_rewrite_not_numeric.parquet":                     "db/cpu/2026/10/01/cpu_rewrite_not_numeric.parquet",
	}
	for input, want := range tests {
		if got := StripRewriteSuffix(input); got != want {
			t.Errorf("StripRewriteSuffix(%q) = %q, want %q", input, got, want)
		}
	}
}

// A repeated clock value must still yield increasing stamps, and a stamp that
// crosses a second carries the second, not just the nanos (#1282).
func TestNameTimeIsStrictlyIncreasing(t *testing.T) {
	base := time.Date(2099, 1, 2, 3, 4, 5, 999_999_999, time.UTC)
	first := NameTime(base)
	second := NameTime(base)
	if !second.After(first) {
		t.Fatalf("second stamp %v not after first %v", second, first)
	}
	if got, want := second.Format("20060102_150405"), "20990102_030406"; got != want {
		t.Fatalf("stamp past .999999999 formats as %s, want %s", got, want)
	}
	if second.Nanosecond() != 0 {
		t.Fatalf("stamp past .999999999 has nanos %d, want 0", second.Nanosecond())
	}
	// An earlier clock (a backward step) never reissues a stamp.
	if third := NameTime(base.Add(-time.Hour)); !third.After(second) {
		t.Fatalf("stamp after a backward clock step %v not after %v", third, second)
	}
}
