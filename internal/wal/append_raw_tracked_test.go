package wal

import (
	"os"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
)

func TestAppendRawTrackedParticipatesInSequenceFloor(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = writer.Close()
		}
	})

	payload, err := msgpack.Marshal([]map[string]interface{}{{"measurement": "cpu", "value": int64(7)}})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	hashes, err := writer.AppendRawTracked(payload)
	if err != nil {
		t.Fatalf("AppendRawTracked: %v", err)
	}
	if len(hashes) != 1 {
		t.Fatalf("AppendRawTracked returned %d identities, want 1", len(hashes))
	}
	dataFile := writer.CurrentFile()
	if err := writer.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	if got := writer.MinUnflushedSequence(); got != 1 {
		t.Fatalf("MinUnflushedSequence before checkpoint = %d, want 1", got)
	}
	if deleted, err := writer.PurgeFlushed(writer.MinUnflushedSequence()); err != nil || deleted != 0 {
		t.Fatalf("PurgeFlushed before checkpoint = (%d, %v), want (0, nil)", deleted, err)
	}
	if err := writer.MarkFlushed(hashes); err != nil {
		t.Fatalf("MarkFlushed: %v", err)
	}
	if got := writer.MinUnflushedSequence(); got <= 1 {
		t.Fatalf("MinUnflushedSequence after checkpoint = %d, want > 1", got)
	}
	if deleted, err := writer.PurgeFlushed(writer.MinUnflushedSequence()); err != nil || deleted != 1 {
		t.Fatalf("PurgeFlushed after checkpoint = (%d, %v), want (1, nil)", deleted, err)
	}
	if _, err := os.Stat(dataFile); !os.IsNotExist(err) {
		t.Fatalf("tracked follower WAL file still exists after checkpoint: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	closed = true
}
