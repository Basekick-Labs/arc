package ingest

import (
	"context"
	"testing"
)

// The covered write path enqueues the WAL entry under the shard lock (#1207).
// Same-key concurrent writers are the case that lock serialises.
func benchmarkCoveredWrite(b *testing.B, covered bool, rows int) {
	t := &testing.T{}
	var reg *coverageRegistrar
	if covered {
		reg = &coverageRegistrar{}
		registered := CoverageRegistered
		reg.autoSettle = &registered
	}
	buf, _ := newCoverageBuffer(t, 50000, reg)
	defer buf.Close()
	rec := coverageRecord("cpu", 1)
	if rows > 1 {
		ts, vs := make([]interface{}, rows), make([]interface{}, rows)
		for i := range ts {
			ts[i], vs[i] = int64(1_700_000_000_000_000+i), float64(i)
		}
		rec = coverageRecordFrom("cpu", ts, vs)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := buf.writeColumnarInternal(context.Background(), "db", rec, false, ""); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkWriteSameKeyCoverageOff(b *testing.B)        { benchmarkCoveredWrite(b, false, 1) }
func BenchmarkWriteSameKeyCoverageOn(b *testing.B)         { benchmarkCoveredWrite(b, true, 1) }
func BenchmarkWriteSameKey500RowsCoverageOff(b *testing.B) { benchmarkCoveredWrite(b, false, 500) }
func BenchmarkWriteSameKey500RowsCoverageOn(b *testing.B)  { benchmarkCoveredWrite(b, true, 500) }
