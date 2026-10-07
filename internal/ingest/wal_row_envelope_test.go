package ingest

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// TestLineProtocolWALEntriesCarryTheirDatabase is the #889 repro. Line
// protocol reaches the buffer as columns with no raw client bytes, so its WAL
// entry takes the row-format fallback. That entry used to go out bare: the
// replication stream carried no database, and a receiver parsing it with
// ParseEnvelope(payload, "default") filed every row under `default`. The entry
// must now carry the envelope the msgpack path has always had, and still
// replay from disk as a row entry.
func TestLineProtocolWALEntriesCarryTheirDatabase(t *testing.T) {
	tmp := t.TempDir()
	writer, err := wal.NewWriter(&wal.WriterConfig{
		WALDir: filepath.Join(tmp, "wal"), SyncMode: wal.SyncModeAsync,
		MaxSizeBytes: 1024 * 1024 * 1024, BufferSize: 1000, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("wal.NewWriter: %v", err)
	}

	// Capture what the replication sender would receive.
	var (
		mu       sync.Mutex
		streamed [][]byte
	)
	writer.SetReplicationHook(func(e *wal.ReplicationEntry) {
		mu.Lock()
		defer mu.Unlock()
		streamed = append(streamed, append([]byte(nil), e.Payload...))
	})

	buf := newReplayTestBuffer(t, filepath.Join(tmp, "data"))
	t.Cleanup(func() { buf.Close() })
	buf.SetWAL(writer)

	// The line-protocol handler's entry point: columns, no RawPayload.
	columns := map[string][]interface{}{
		"time": {int64(1_700_000_000_000_000)},
		"host": {"h1"},
		"v":    {1.5},
	}
	if err := buf.WriteColumnarDirect(context.Background(), "rig", "live", columns); err != nil {
		t.Fatalf("write: %v", err)
	}
	walPath := writer.CurrentFile()
	if err := writer.Close(); err != nil {
		t.Fatalf("wal close: %v", err)
	}

	// 1. The replicated entry names its database. This is exactly what the
	//    receiver does with it (coordinator.buildReplicationIngestHandler).
	mu.Lock()
	defer mu.Unlock()
	if len(streamed) != 1 {
		t.Fatalf("replication hook saw %d entries, want 1", len(streamed))
	}
	database, inner := wal.ParseEnvelope(streamed[0], "default")
	if database != "rig" {
		t.Fatalf("replicated entry resolves to database %q, want rig: a receiver would file these rows under default (#889)", database)
	}
	var rows []map[string]interface{}
	if err := msgpack.Unmarshal(inner, &rows); err != nil {
		t.Fatalf("inner payload is not a row array: %v", err)
	}
	if len(rows) != 1 || rows[0]["_measurement"] != "live" {
		t.Fatalf("inner payload = %v, want one row for measurement live", rows)
	}

	// 2. The on-disk entry still reads back as a tracked row entry, with the
	//    database stamped on the record the recovery callback routes by.
	entries, err := wal.NewReader(walPath, zerolog.Nop()).ReadAll()
	if err != nil {
		t.Fatalf("read WAL: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("WAL holds %d entries, want 1", len(entries))
	}
	entry := entries[0]
	if len(entry.Records) != 1 {
		t.Fatalf("entry replays as %d row records (columnar=%v), want 1 row record", len(entry.Records), entry.ColumnarData != nil)
	}
	if entry.Records[0]["_database"] != "rig" || entry.Records[0]["_measurement"] != "live" {
		t.Fatalf("replayed record = %v, want _database=rig _measurement=live", entry.Records[0])
	}
	if len(entry.PayloadHash) != walTrackedIdentityHexLen {
		t.Fatalf("entry identity %q is not a tracked identity: the flush could not checkpoint it", entry.PayloadHash)
	}
}
