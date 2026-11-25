package iceberg

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// A MEASUREMENT harness for #1751, not a regression test (gated like
// deleted_manifest_cost_test.go). It answers the issue's acceptance line: does
// what an ingest export costs grow with the table's file count? Both arms go
// through the scheduler, as a due ingest request does: the pass the request
// ran before (listing, PlanFiles, duplicate check, commit, expiry, publish,
// prune, sweep) and the append (per-file check, footer, commit, publish).
//
//	ARC_ICEBERG_MANIFEST_BENCH=1 go test -tags=duckdb_arrow ./internal/iceberg/ -run TestMeasure_AppendAgainstAPass -v -timeout 60m
func TestMeasure_AppendAgainstAPass(t *testing.T) {
	requireMeasurementRun(t)
	ctx := context.Background()
	median := func(ds []time.Duration) time.Duration {
		sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
		return ds[len(ds)/2]
	}
	for _, size := range []int{1000, 8000, 20000} {
		for _, batch := range []int{1, 30} {
			t.Run(fmt.Sprintf("%d files, %d new", size, batch), func(t *testing.T) {
				r := newAppendRig(t)
				r.exp.retain = 10
				dir := filepath.Join(r.root, "mydb", "cpu", "2026", "07", "14", "15")
				for i := 0; i < size; i++ {
					writeArcStyleParquet(t, filepath.Join(dir, fmt.Sprintf("seed_%05d.parquet", i)), 1_700_000_000_000_000, 2)
				}
				// The first export: a pass that creates the table.
				r.report(r.newFile())
				r.serve()
				if !r.sched.appendOK["mydb\x00cpu"] {
					t.Fatal("the first export did not settle")
				}
				write := func() []string {
					names := make([]string, batch)
					for i := range names {
						names[i] = r.newFile()
					}
					return names
				}
				const rounds = 7
				var passes, appends []time.Duration
				for i := 0; i < rounds; i++ {
					// The pass, as an ingest request ran it before #1751.
					write()
					start := time.Now()
					if _, err := r.pass(); err != nil {
						t.Fatal(err)
					}
					passes = append(passes, time.Since(start))
					// The append.
					r.report(write()...)
					time.Sleep(5 * time.Millisecond)
					start = time.Now()
					r.sched.serviceIngest(ctx, ingestBatch)
					appends = append(appends, time.Since(start))
				}
				if got := r.listings(); got != 1+rounds {
					t.Fatalf("%d listings, want %d: an append listed the measurement", got, 1+rounds)
				}
				if got, want := len(r.entries()), size+1+2*rounds*batch; got != want {
					t.Fatalf("%d files in the table, want %d", got, want)
				}
				t.Logf("table of %5d files, %2d new per export: pass median %8.1f ms (min %.1f, max %.1f)   append median %6.1f ms (min %.1f, max %.1f)   manifests now %d",
					size, batch,
					float64(median(passes).Microseconds())/1000, float64(passes[0].Microseconds())/1000, float64(passes[len(passes)-1].Microseconds())/1000,
					float64(median(appends).Microseconds())/1000, float64(appends[0].Microseconds())/1000, float64(appends[len(appends)-1].Microseconds())/1000,
					len(r.manifests()))
			})
		}
	}
}
