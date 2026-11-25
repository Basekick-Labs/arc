package storage

import (
	"testing"
	"time"
)

func TestIngestFileTime(t *testing.T) {
	at := time.Date(2026, 8, 28, 20, 8, 2, 765188000, time.UTC)
	for _, c := range []struct {
		name, file string
		wantOK     bool
		want       time.Time
	}{
		{"ingest format", "cpu_20260828_200802_765188000.parquet", true, at},
		{"underscored measurement", "my_weird_2x_measure_20260828_200802_765188000.parquet", true, at},
		{"full path", "/data/apex/cpu/2026/08/28/20/cpu_20260828_200802_765188000.parquet", true, at},
		{"hourly compacted", "cpu_20260828_200000_1787948669258000000_b0_compacted.parquet", false, time.Time{}},
		{"daily compacted", "cpu_20260828_000000_1787948669258000000_b0_daily.parquet", false, time.Time{}},
		{"too few tokens", "cpu_20260828_200802.parquet", false, time.Time{}},
		{"non-digit nanos", "cpu_20260828_200802_76518800x.parquet", false, time.Time{}},
		{"bad minute", "cpu_20260828_207102_765188000.parquet", false, time.Time{}},
		{"bad month", "cpu_20261328_200802_765188000.parquet", false, time.Time{}},
		{"garbage", "whatever.parquet", false, time.Time{}},
		{"short nanos token keeps the second", "cpu_20260828_200802_7651.parquet", true, time.Date(2026, 8, 28, 20, 8, 2, 0, time.UTC)},
		// A DELETE rewrite reads as the file it rewrote: a caller that must
		// tell them apart compares StripRewriteSuffix(name) with name.
		{"a DELETE rewrite", "cpu_20260828_200802_765188000_rewrite_1787948669258000000.parquet", true, at},
	} {
		got, ok := IngestFileTime(c.file)
		if ok != c.wantOK || (ok && !got.Equal(c.want)) {
			t.Errorf("%s: IngestFileTime(%q) = %v, %v; want %v, %v", c.name, c.file, got, ok, c.want, c.wantOK)
		}
	}
	// What NameTime stamps, IngestFileTime reads back.
	stamp := NameTime(time.Now())
	name := "m_" + stamp.Format("20060102_150405") + "_" + pad9(stamp.Nanosecond()) + ".parquet"
	if got, ok := IngestFileTime(name); !ok || !got.Equal(stamp) {
		t.Errorf("round trip: %q read as %v, %v; want %v", name, got, ok, stamp)
	}
}

func pad9(n int) string {
	s := "000000000" + itoa(n)
	return s[len(s)-9:]
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
