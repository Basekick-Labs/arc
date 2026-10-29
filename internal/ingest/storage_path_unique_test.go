package ingest

import (
	"fmt"
	"regexp"
	"sync"
	"testing"
	"time"
)

// Concurrent flushes that read the same clock value must still get distinct
// file names; the second write would otherwise replace the first (#1282).
func TestGenerateStoragePathUniqueUnderFrozenClock(t *testing.T) {
	// 10 ns before a second boundary, so the run crosses it; far in the
	// future, so no stamp an earlier test issued is later.
	frozen := time.Date(2099, 10, 14, 21, 20, 1, 999_999_990, time.UTC)
	orig := flushNameNow
	flushNameNow = func() time.Time { return frozen }
	t.Cleanup(func() { flushNameNow = orig })

	const workers, perWorker = 28, 200
	b := &ArrowBuffer{}
	paths := make(chan string, workers*perWorker)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				paths <- b.generateStoragePath("rig", "bench", frozen)
			}
		}()
	}
	wg.Wait()
	close(paths)

	// The names are exactly the stamps frozen, frozen+1ns, ... with the
	// seconds and the nanos of each name read from the one stamp (past the
	// boundary both advance together), in the shape every parser reads:
	// <measurement>_<YYYYMMDD>_<HHMMSS>_<9-digit nanos>.parquet.
	shape := regexp.MustCompile(`^rig/bench/2099/10/14/21/bench_\d{8}_\d{6}_\d{9}\.parquet$`)
	want := make(map[string]bool, workers*perWorker)
	for i := 0; i < workers*perWorker; i++ {
		ts := frozen.Add(time.Duration(i))
		want[fmt.Sprintf("rig/bench/2099/10/14/21/bench_%s_%09d.parquet", ts.Format("20060102_150405"), ts.Nanosecond())] = true
	}
	seen := make(map[string]bool, workers*perWorker)
	for p := range paths {
		if seen[p] {
			t.Fatalf("duplicate storage path %s", p)
		}
		if !shape.MatchString(p) {
			t.Fatalf("storage path %s lost the flushed-file name shape", p)
		}
		if !want[p] {
			t.Fatalf("storage path %s is not one of the stamps from the frozen clock on", p)
		}
		seen[p] = true
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("got %d distinct paths, want %d", len(seen), workers*perWorker)
	}
}
