package ingest

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// countingGate is a WAL disk gate with unlimited room that counts what is
// reserved.
type countingGate struct {
	mu       sync.Mutex
	reserved int64
}

func (g *countingGate) Pressure() bool { return false }
func (g *countingGate) Reserve(n int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reserved += n
	return true
}
func (g *countingGate) Release(n int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.reserved -= n
}

// A typed write without a raw payload (an import) charges its WAL bytes to
// the request's reservation as it appends them, as the columnar path does
// (#676): before, a long import held its whole reservation while its WAL
// bytes also landed on the volume.
func TestTypedWALAppendConsumesReservation(t *testing.T) {
	buf := NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000, MaxBufferAgeMS: 600000, Compression: "snappy", FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0"},
		&mockStorageBackend{}, zerolog.New(io.Discard))
	defer buf.Close()
	buf.SetWAL(&checkpointRecordingWAL{})
	gate := &countingGate{}
	buf.SetWALDiskGate(gate)

	ctx, release, err := buf.AdmitRequest(context.Background(), 1<<20)
	require.NoError(t, err)
	defer release()
	require.EqualValues(t, 1<<20, gate.reserved)

	batch := &TypedColumnBatch{Data: map[string]interface{}{"time": []int64{1, 2, 3}, "v": []int64{1, 2, 3}}}
	require.NoError(t, buf.WriteTypedColumnarDirect(ctx, "d", "m", batch, 3))
	require.Less(t, gate.reserved, int64(1<<20), "the WAL bytes were not charged to the reservation")
}
