package ingest

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// The latch-age gauge end to end (#1938): SetWAL is what turns it on, and the
// real 1 Hz sampler is what publishes it. A buffer whose SetWAL forgets hasWAL,
// or a sampler that skips the setter, leaves the metric at 0 forever.
func TestLatchAgeGaugeThroughTheSampler1938(t *testing.T) {
	cfg := &config.IngestConfig{MaxBufferSize: 100, MaxBufferAgeMS: 3_600_000, Compression: "snappy",
		ShardCount: 1, FlushWorkers: 1, FlushQueueSize: 4, FlushTimeoutSeconds: 30, DataPageVersion: "2.0"}
	buf := NewArrowBuffer(cfg, &failingStorageBackend{err: errors.New("storage unavailable")}, zerolog.New(io.Discard))
	defer buf.Close()

	require.False(t, buf.hasWAL.Load(), "no WAL yet")
	buf.SetWAL(&checkpointRecordingWAL{})
	require.True(t, buf.hasWAL.Load(), "SetWAL turns the gauge on")

	buf.markFlushFailure()
	age := func() float64 {
		v, _ := metrics.Get().Snapshot()["ingest_flush_failure_latch_age_seconds"].(float64)
		return v
	}
	require.Eventually(t, func() bool { return age() > 0 }, 5*time.Second, 50*time.Millisecond,
		"the sampler never published a latch age while the latch was set")
}
