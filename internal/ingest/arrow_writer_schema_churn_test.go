package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// TestArrowBuffer_SchemaChurnExceededErrorContract pins the contract
// that flushOnSchemaChangeLocked returns ErrSchemaChurnExceeded (not
// nil, not an arbitrary error) when the iteration cap is reached, and
// that the totalSchemaChurnExceeded counter increments in lockstep
// with the error returns.
//
// The test launches many writers with rotating distinct schemas
// against the same (database, measurement) buffer, with a slow
// storage backend that widens the I/O window inside flushBufferLocked
// where a concurrent writer can install a third schema. Most writers
// succeed; the test asserts only the *shape* of any failures:
//
//  1. Every non-nil write error must satisfy errors.Is(err, ErrSchemaChurnExceeded).
//     Catches a regression where someone reverts to "return nil" and
//     wide-schema buffers land on disk silently.
//  2. The number of errored writes must equal the increment in
//     totalSchemaChurnExceeded counter. Catches a regression where
//     the counter increment is decoupled from the error return.
//
// The test does NOT assert that the cap is *always* hit — under fast
// machines steady state runs cleanly with 0 churn errors, which is
// the expected behavior. It only locks the contract for when the cap
// IS hit.
func TestArrowBuffer_SchemaChurnExceededErrorContract(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping schema-churn contract test in -short mode")
	}

	cfg := &config.IngestConfig{
		MaxBufferSize:   25,
		MaxBufferAgeMS:  1000,
		Compression:     "snappy",
		FlushWorkers:    1, // single worker so flushes serialize and we can saturate
		FlushQueueSize:  4,
		ShardCount:      1, // single shard so all writers contend on the same lock
		DataPageVersion: "2.0",
	}
	storage := newHangingStorage(0, 10*time.Millisecond) // wider I/O window
	buf := NewArrowBuffer(cfg, storage, zerolog.Nop())
	defer buf.Close()

	const numWriters = 16
	const writesPerWriter = 200
	const numSchemas = 12 // > schemaEvolutionMaxIters (8) so the cap is reachable

	// Each writer rotates through `numSchemas` distinct schemas, choosing
	// based on (writerID, iteration) to maximise cross-writer collision.
	// Each schema has a unique column name so the signature differs.
	makeRotatingSchema := func(schemaIdx, n int) map[string][]interface{} {
		ts := make([]interface{}, n)
		v := make([]interface{}, n)
		base := time.Now().UnixMicro()
		for i := 0; i < n; i++ {
			ts[i] = base + int64(i)
			v[i] = float64(i)
		}
		colName := fmt.Sprintf("col_%d", schemaIdx)
		return map[string][]interface{}{"time": ts, colName: v}
	}

	beforeCount := buf.totalSchemaChurnExceeded.Load()

	var (
		wg             sync.WaitGroup
		churnErrCount  atomic.Int64
		otherErrCount  atomic.Int64
		successesCount atomic.Int64
	)
	wg.Add(numWriters)
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("writer %d panicked: %v", writerID, r)
				}
			}()
			for j := 0; j < writesPerWriter; j++ {
				schemaIdx := (writerID + j) % numSchemas
				err := buf.WriteColumnarDirect(context.Background(), "testdb", "churn",
					makeRotatingSchema(schemaIdx, 5))
				switch {
				case err == nil:
					successesCount.Add(1)
				case errors.Is(err, ErrSchemaChurnExceeded):
					churnErrCount.Add(1)
				default:
					// Any other error is a contract violation: the only
					// failure mode under this workload is schema churn.
					t.Errorf("writer %d iter %d returned unexpected error type: %v", writerID, j, err)
					otherErrCount.Add(1)
				}
			}
		}(w)
	}
	wg.Wait()

	afterCount := buf.totalSchemaChurnExceeded.Load()
	counterDelta := afterCount - beforeCount

	t.Logf("churn workload result: successes=%d, churn_errors=%d, counter_delta=%d, other_errors=%d",
		successesCount.Load(), churnErrCount.Load(), counterDelta, otherErrCount.Load())

	// The counter increments inside flushOnSchemaChangeLocked — once per
	// helper invocation that hits the cap. A single write call can fan
	// out to at most one helper invocation (writeColumnarInternal /
	// writeTypedColumnarDirect each call the helper at most once per
	// write), so counter delta and churn-error count must match exactly.
	if counterDelta != churnErrCount.Load() {
		t.Errorf("counter/error decoupling: totalSchemaChurnExceeded delta=%d but churn errors=%d",
			counterDelta, churnErrCount.Load())
	}
}

// TestArrowBuffer_ErrSchemaChurnExceeded_IsExported asserts the
// sentinel is exported and stable so external callers (LP/TLE
// handlers) can errors.Is it without an internal-package reach.
func TestArrowBuffer_ErrSchemaChurnExceeded_IsExported(t *testing.T) {
	if ErrSchemaChurnExceeded == nil {
		t.Fatal("ErrSchemaChurnExceeded must be a non-nil exported sentinel")
	}
	wrapped := fmt.Errorf("write rejected: %w", ErrSchemaChurnExceeded)
	if !errors.Is(wrapped, ErrSchemaChurnExceeded) {
		t.Fatal("ErrSchemaChurnExceeded must be unwrappable via errors.Is")
	}
}
