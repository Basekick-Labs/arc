package wal

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/rs/zerolog"
)

// TestColumnarReplayContinuesPastFailedEntry (#590): a single columnar entry
// whose replay callback fails must NOT abandon the remaining entries in the
// WAL file — every durable entry after the poisoned one must still be
// attempted, and the failure must be counted.
func TestColumnarReplayContinuesPastFailedEntry(t *testing.T) {
	tmpDir := t.TempDir()

	writer, err := NewWriter(&WriterConfig{
		WALDir:       tmpDir,
		SyncMode:     SyncModeFsync,
		MaxSizeBytes: 100 * 1024 * 1024,
		Logger:       zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("writer: %v", err)
	}

	for i := 0; i < 3; i++ {
		payload, merr := msgpack.Marshal(map[string]interface{}{
			"m": fmt.Sprintf("m%d", i),
			"columns": map[string]interface{}{
				"time": []interface{}{int64(1700000000000000 + i)},
				"v":    []interface{}{int64(i)},
			},
		})
		if merr != nil {
			t.Fatalf("marshal: %v", merr)
		}
		if aerr := writer.AppendRawWithMeta("db", payload); aerr != nil {
			t.Fatalf("append %d: %v", i, aerr)
		}
	}
	// Give the async writer goroutine time to drain, then close (flushes).
	time.Sleep(200 * time.Millisecond)
	if cerr := writer.Close(); cerr != nil {
		t.Fatalf("close: %v", cerr)
	}

	var attempted []string
	columnarCallback := func(ctx context.Context, database, measurement string, columns map[string][]interface{}) error {
		attempted = append(attempted, measurement)
		if measurement == "m0" {
			return fmt.Errorf("simulated poisoned entry")
		}
		return nil
	}
	rowCallback := func(ctx context.Context, records []map[string]interface{}) error { return nil }

	recovery := NewRecovery(tmpDir, zerolog.Nop())
	stats, err := recovery.RecoverWithOptions(context.Background(), rowCallback, &RecoveryOptions{
		ColumnarCallback: columnarCallback,
	})
	if err != nil {
		t.Fatalf("recover: %v", err)
	}

	if len(attempted) != 3 {
		t.Fatalf("expected all 3 entries attempted after one failure, got %d: %v", len(attempted), attempted)
	}
	if stats.CorruptedEntries != 1 {
		t.Fatalf("expected 1 failed entry counted, got %d", stats.CorruptedEntries)
	}
}
