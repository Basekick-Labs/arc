package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
)

// TestWideRequestReachesWALAndReplays is the #677 repro end to end: a wide
// columnar msgpack request above the WAL single-entry cap goes through the
// real decode -> ArrowBuffer.Write path with a real WAL writer attached, the
// WAL holds the request's rows as chunked entries instead of a header-only
// file, and those entries replay through the recovery callback the same way
// boot recovery would apply them.
func TestWideRequestReachesWALAndReplays(t *testing.T) {
	tmp := t.TempDir()
	writer, err := wal.NewWriter(&wal.WriterConfig{
		WALDir: filepath.Join(tmp, "wal"), SyncMode: wal.SyncModeAsync,
		MaxSizeBytes: 1024 * 1024 * 1024, BufferSize: 100000, Logger: zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("wal.NewWriter: %v", err)
	}
	buf := newReplayTestBuffer(t, filepath.Join(tmp, "data")) // package-standard buffer+storage wiring
	t.Cleanup(func() { buf.Close() })
	buf.SetWAL(writer)

	// The reporter's payload: time + 10 fields of ~10 KB per row, above the
	// WAL single-entry cap.
	const rows = 1200
	wide := strings.Repeat("w", 10*1024)
	timeCol := make([]interface{}, rows)
	for i := range timeCol {
		timeCol[i] = int64(1_700_000_000_000_000 + i)
	}
	wideRow := make([]interface{}, rows)
	for i := range wideRow {
		wideRow[i] = wide
	}
	columns := map[string]interface{}{"time": timeCol}
	for f := 0; f < 10; f++ {
		columns[fmt.Sprintf("field_%d", f)] = wideRow
	}
	payload, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": columns})
	if err != nil || len(payload) <= wal.MaxWALPayloadSize {
		t.Fatalf("marshal (%v) or payload %d not above cap", err, len(payload))
	}

	// The live path: decode the request, write the decoded records.
	decoded, err := NewMessagePackDecoder(zerolog.Nop()).Decode(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := buf.Write(context.Background(), "default", decoded); err != nil {
		t.Fatalf("write: %v", err)
	}
	walPath := writer.CurrentFile()
	if err := writer.Close(); err != nil {
		t.Fatalf("wal close: %v", err)
	}
	// The WAL must not be header-only, and its entries must cover every row.
	if info, err := os.Stat(walPath); err != nil {
		t.Fatalf("stat: %v", err)
	} else if info.Size() <= 7 {
		t.Fatalf("WAL is header-only (%d bytes): the wide request never reached the WAL", info.Size())
	}
	entries, err := wal.NewReader(walPath, zerolog.Nop()).ReadAll()
	if err != nil || len(entries) < 2 {
		t.Fatalf("expected chunked entries, got %d (%v)", len(entries), err)
	}

	// Replay: a fresh buffer applies each entry the way boot recovery would.
	replayBuf := newReplayTestBuffer(t, filepath.Join(tmp, "replay-data"))
	t.Cleanup(func() { replayBuf.Close() })
	totalRows := 0
	for i, e := range entries {
		if e.ColumnarData == nil {
			t.Fatalf("entry %d not columnar", i)
		}
		if err := replayBuf.WriteColumnarDirectNoWAL(context.Background(), e.ColumnarData.Database, e.ColumnarData.Measurement, e.ColumnarData.Columns); err != nil {
			t.Fatalf("replay entry %d: %v", i, err)
		}
		for _, col := range e.ColumnarData.Columns {
			totalRows += len(col)
			break
		}
	}
	if totalRows != rows {
		t.Fatalf("replayed rows = %d, want %d", totalRows, rows)
	}
}
