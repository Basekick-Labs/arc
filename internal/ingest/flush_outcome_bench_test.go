package ingest

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// liveFlushBatches builds n batches of rows rows each, in the current hour,
// with no outcome: a live write's flush.
func liveFlushBatches(n, rows int) ([]interface{}, int) {
	start := time.Now().UTC().Truncate(time.Hour).UnixMicro()
	records := make([]interface{}, n)
	for i := range records {
		times := make([]int64, rows)
		vals := make([]float64, rows)
		for j := range times {
			times[j], vals[j] = start+int64(i*rows+j), float64(j)
		}
		data := map[string]interface{}{"time": times, "value": vals}
		records[i] = &TypedColumnBatch{Data: data, Signature: getColumnSignature(data)}
	}
	return records, n * rows
}

// BenchmarkFlushRecordsAsyncLive is a real flush of 1000 batches without an
// outcome to local storage (#1759): what a live write's flush pays for the
// outcome bookkeeping on its success path.
func BenchmarkFlushRecordsAsyncLive(b *testing.B) {
	backend, err := storage.NewLocalBackend(b.TempDir(), zerolog.Nop())
	if err != nil {
		b.Fatal(err)
	}
	buf := NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 1 << 30, MaxBufferAgeMS: 600000, Compression: "snappy",
		ShardCount: 4, FlushWorkers: 1, FlushQueueSize: 4,
	}, backend, zerolog.New(io.Discard))
	defer buf.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		records, rows := liveFlushBatches(1000, 10)
		b.StartTimer()
		if err := buf.flushRecordsAsync(ctx, "db/bench", "db", "bench", records, rows, nil, nil, time.Now()); err != nil {
			b.Fatal(err)
		}
	}
}
