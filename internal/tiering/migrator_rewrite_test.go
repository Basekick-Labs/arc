package tiering

import "testing"

func TestIsDailyCompactedPathAcceptsImmutableRewrite(t *testing.T) {
	for _, path := range []string{
		"db/cpu/2026/10/01/cpu_20261001_daily.parquet",
		"db/cpu/2026/10/01/cpu_20261001_daily_rewrite_123.parquet",
	} {
		if !isDailyCompactedPath(path) {
			t.Errorf("isDailyCompactedPath(%q) = false, want true", path)
		}
	}
	if isDailyCompactedPath("db/cpu/2026/10/01/cpu_20261001_compacted.parquet") {
		t.Fatal("hourly compacted file classified as daily")
	}
}
