package ingest

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
)

// A DELETE whose baseline is already stale fails its barrier at once (#1938),
// and its message says the rows wait for WAL recovery only when there is a WAL:
// without one, a failed flush's rows have no copy to recover from.
func TestDeleteBarrierMessageFollowsTheWAL1938(t *testing.T) {
	for _, withWAL := range []bool{false, true} {
		cfg := &config.IngestConfig{MaxBufferSize: 100, MaxBufferAgeMS: 3_600_000, Compression: "snappy",
			ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 4, FlushTimeoutSeconds: 30, DataPageVersion: "2.0"}
		buf := NewArrowBuffer(cfg, &failingStorageBackend{err: errors.New("storage unavailable")}, zerolog.New(io.Discard))
		if withWAL {
			buf.SetWAL(&checkpointRecordingWAL{})
		}
		_, _, gen := buf.FlushFailureState()
		buf.markFlushFailure() // the generation moves past the DELETE's reading
		_, err := buf.FlushScopeAndWaitSince(context.Background(), "db", "m", gen)
		buf.Close()
		if err == nil {
			t.Fatalf("withWAL=%v: a stale baseline must fail the barrier", withWAL)
		}
		if got := strings.Contains(err.Error(), "WAL recovery"); got != withWAL {
			t.Fatalf("withWAL=%v: message %q mentions WAL recovery = %v", withWAL, err, got)
		}
	}
}
