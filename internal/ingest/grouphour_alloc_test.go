package ingest

import (
	"testing"
)

// These benchmarks isolate the single-hour flush decision: the old code called
// groupByHour (which allocates a per-row index bucket) unconditionally, then discarded
// it when the data fell in one hour. The new code does a cheap inline min/max scan and
// only calls groupByHour on the multi-hour path. This bench quantifies the per-flush
// allocation removed on the common single-hour path.

func singleHourTimes(n int) []int64 {
	base := int64(1_700_000_000_000_000) // some hour
	out := make([]int64, n)
	for i := range out {
		out[i] = base + int64(i) // all within the same hour, ascending
	}
	return out
}

// BenchmarkGroupByHour_SingleHour: the OLD cost — full bucketing on single-hour data.
func BenchmarkGroupByHour_SingleHour(b *testing.B) {
	times := singleHourTimes(5_000_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := groupByHour(times)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMinMaxScan_SingleHour: the NEW cost — just the inline min/max scan that
// replaces groupByHour on the single-hour decision.
func BenchmarkMinMaxScan_SingleHour(b *testing.B) {
	times := singleHourTimes(5_000_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mn, mx := times[0], times[0]
		for _, t := range times {
			if t < mn {
				mn = t
			}
			if t > mx {
				mx = t
			}
		}
		_, _ = mn, mx
	}
}
