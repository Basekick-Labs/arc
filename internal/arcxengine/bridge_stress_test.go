//go:build cgo && arcx_engine

package arcxengine

// Regression guards for the FFI memory-ownership contract, distilled from the
// adversarial review that proved the bridge safe. These are the tests that would
// catch a future regression in the retain/release path (use-after-free), a
// double-free/leak, or a thread-safety break — the process-fatal classes.
//
// Fixture path via ARCX_STRESS_FIXTURE (a real Arc parquet file); skipped if
// unset so this doesn't need the production dataset in a normal run. Run under
// the race detector for the concurrency guard:
//   ARCX_STRESS_FIXTURE=<file> CGO_ENABLED=1 go test -race \
//     -tags=duckdb_arrow,arcx_engine -run TestBridgeStress ./internal/arcxengine/

import (
	"os"
	"runtime"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
)

func stressFixture(t *testing.T) string {
	f := os.Getenv("ARCX_STRESS_FIXTURE")
	if f == "" {
		t.Skip("set ARCX_STRESS_FIXTURE=<parquet file> to run the FFI stress guards")
	}
	if _, err := os.Stat(f); err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	return f
}

// TestBridgeStressUseAfterFree holds the returned Record across aggressive GC +
// allocation churn, THEN reads its values. If importRecord failed to retain the
// child columns before releasing the parent struct array, the underlying C
// buffers would be freed and this read would corrupt or crash.
func TestBridgeStressUseAfterFree(t *testing.T) {
	f := stressFixture(t)
	rec, err := Query("SELECT count(*) FROM read_parquet('"+f+"')", Context{})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rec.Release()

	// Churn the heap and force GC repeatedly between import and read.
	for i := 0; i < 50; i++ {
		_ = make([]byte, 1<<20)
		runtime.GC()
	}

	col, ok := rec.Column(0).(*array.Int64)
	if !ok {
		t.Fatalf("expected Int64, got %T", rec.Column(0))
	}
	if col.Len() != 1 || col.Value(0) <= 0 {
		t.Fatalf("value corrupted after GC pressure: len=%d val=%d", col.Len(), col.Value(0))
	}
}

// TestBridgeStressConcurrent hammers arcx_query from many goroutines. Run with
// -race to catch a thread-safety break (the engine must be reentrant / hold no
// shared mutable state). Also a leak/double-free churn.
func TestBridgeStressConcurrent(t *testing.T) {
	f := stressFixture(t)
	const goroutines, iters = 32, 40
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				rec, err := Query("SELECT count(*) FROM read_parquet('"+f+"')", Context{})
				if err != nil {
					errs <- err
					return
				}
				rec.Release()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent query failed: %v", err)
	}
}
