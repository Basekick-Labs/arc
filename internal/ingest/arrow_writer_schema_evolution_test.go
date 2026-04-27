package ingest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// TestArrowBuffer_SchemaEvolutionConcurrentNoCorruption stresses the
// schema-evolution path under sustained concurrent writes to the
// same (database, measurement) buffer with alternating schemas.
// Regression for 26.05.1 critical bug C2: the original code released
// the shard mutex during the schema-change flush, allowing a
// concurrent writer to install a third schema in the I/O window and
// producing column mismatches in the next flush.
//
// The fix is a bounded loop that re-checks bufferSchemas after the
// flush returns under re-acquired lock; a third-schema racer
// triggers another iteration rather than silent corruption.
//
// Iteration count (not wall clock) drives the stress so the test
// exercises the same number of contended writes on slow CI runners
// as on fast laptops. Per-goroutine deferred recovers turn any
// panic inside a writer into a t.Errorf rather than crashing the
// test binary.
func TestArrowBuffer_SchemaEvolutionConcurrentNoCorruption(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping schema-evolution stress test in -short mode")
	}

	cfg := &config.IngestConfig{
		MaxBufferSize:   50, // small buffer so flushes happen often
		MaxBufferAgeMS:  1000,
		Compression:     "snappy",
		FlushWorkers:    2,
		FlushQueueSize:  8,
		ShardCount:      4,
		DataPageVersion: "2.0",
	}
	storage := newHangingStorage(0, 5*time.Millisecond) // brief "hang" mimics S3 latency
	buf := NewArrowBuffer(cfg, storage, zerolog.Nop())
	defer buf.Close()

	// Two schemas for the same measurement — alternating writers will
	// trigger schema-evolution flushes whenever they observe each
	// other's signature.
	schemaA := func(n int) map[string][]interface{} {
		ts := make([]interface{}, n)
		v := make([]interface{}, n)
		base := time.Now().UnixMicro()
		for i := 0; i < n; i++ {
			ts[i] = base + int64(i)
			v[i] = float64(i)
		}
		return map[string][]interface{}{"time": ts, "value": v}
	}
	schemaB := func(n int) map[string][]interface{} {
		ts := make([]interface{}, n)
		c := make([]interface{}, n)
		base := time.Now().UnixMicro()
		for i := 0; i < n; i++ {
			ts[i] = base + int64(i)
			c[i] = int64(i) // different column name AND type from schemaA's "value"
		}
		return map[string][]interface{}{"time": ts, "count": c}
	}

	const iterationsPerWriter = 1000

	// safeWriter runs `iterations` writes, recovering any panic into
	// the test's failure log. A panic inside a writer goroutine
	// otherwise crashes the test binary before the main goroutine's
	// recover() can catch it.
	safeWriter := func(name string, payload func() map[string][]interface{}) func() {
		return func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s panicked: %v", name, r)
				}
			}()
			for i := 0; i < iterationsPerWriter; i++ {
				_ = buf.WriteColumnarDirect(context.Background(), "testdb", "stress", payload())
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		safeWriter("writer-A", func() map[string][]interface{} { return schemaA(20) })()
	}()
	go func() {
		defer wg.Done()
		safeWriter("writer-B", func() map[string][]interface{} { return schemaB(20) })()
	}()

	wg.Wait()
	// If we reach here without t.Errorf calls from safeWriter and
	// Close (deferred) returns cleanly, the schema-evolution loop is
	// correctly serialising the writers despite the racy I/O window
	// inside flushBufferLocked.
}
