package storage

import "testing"

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
