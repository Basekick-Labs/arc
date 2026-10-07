package wal

import (
	"fmt"
	"testing"

	"github.com/rs/zerolog"
)

// BenchmarkMarkFlushedBatched measures one durable checkpoint sync for a
// representative batch of tracked ingestion entries.
func BenchmarkMarkFlushedBatched(b *testing.B) {
	const batchSize = 1_000
	writer, err := NewWriter(&WriterConfig{
		WALDir:   b.TempDir(),
		SyncMode: SyncModeFdatasync,
		Logger:   zerolog.Nop(),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := writer.Close(); err != nil {
			b.Errorf("close WAL writer: %v", err)
		}
	})

	hashes := make([]string, batchSize)
	for i := range hashes {
		hashes[i] = fmt.Sprintf("%032x", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := writer.MarkFlushed(hashes); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "checkpoint-ids/s")
}
