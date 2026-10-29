package wal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/Basekick-Labs/msgpack/v6"
)

func coveredTestPayload(t *testing.T, rows int) []byte {
	t.Helper()
	cols := map[string]interface{}{"time": make([]interface{}, rows), "value": make([]interface{}, rows)}
	for i := 0; i < rows; i++ {
		cols["time"].([]interface{})[i] = int64(i)
		cols["value"].([]interface{})[i] = float64(i)
	}
	payload, err := msgpack.Marshal(map[string]interface{}{"m": "cpu", "columns": cols})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return payload
}

func readAllEntries(t *testing.T, w *Writer, path string) []Entry {
	t.Helper()
	entries, err := NewReader(path, w.logger).ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return entries
}

// A covered entry reads back with its coverage, the same identity token form
// as a 0x03 entry, and the same logical payload (#1207).
func TestCoveredEntryRoundTrip(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer writer.Close()

	p, err := writer.PrepareCovered("db1", "db1/cpu", coveredTestPayload(t, 3))
	if err != nil {
		t.Fatalf("PrepareCovered: %v", err)
	}
	tokens, err := p.Commit(42)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(tokens) != 1 || len(tokens[0]) != 32 {
		t.Fatalf("tokens = %v, want one 32-hex token", tokens)
	}
	path := writer.CurrentFile()
	if err := writer.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	entries := readAllEntries(t, writer, path)
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	e := entries[0]
	if e.PayloadHash != tokens[0] {
		t.Fatalf("PayloadHash = %q, want %q", e.PayloadHash, tokens[0])
	}
	want := Coverage{Instance: writer.trackedInstance, Key: "db1/cpu", CSeq: 42, Chunk: 0, Chunks: 1}
	if e.Coverage == nil || *e.Coverage != want {
		t.Fatalf("Coverage = %+v, want %+v", e.Coverage, want)
	}
	if e.ColumnarData == nil || e.ColumnarData.Database != "db1" || e.ColumnarData.Measurement != "cpu" || len(e.ColumnarData.Columns["value"]) != 3 {
		t.Fatalf("ColumnarData = %+v", e.ColumnarData)
	}
}

// The CRC is finished at Commit; an entry whose cseq is altered on disk must
// fail the checksum, which proves the cseq is covered by it.
func TestCoveredEntryCRCCoversCSeq(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer writer.Close()

	p, err := writer.PrepareCovered("db1", "db1/cpu", coveredTestPayload(t, 1))
	if err != nil {
		t.Fatalf("PrepareCovered: %v", err)
	}
	if _, err := p.Commit(7); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	path := writer.CurrentFile()
	if err := writer.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var seven [8]byte
	seven[7] = 7
	idx := bytes.LastIndex(raw, seven[:])
	if idx < 0 {
		t.Fatal("cseq bytes not found in the file")
	}
	raw[idx+7] = 8
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewReader(path, writer.logger)
	entries, err := r.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(entries) != 0 || r.CorruptedEntries != 1 {
		t.Fatalf("entries=%d corrupted=%d: an entry whose cseq changed after Commit passed the checksum", len(entries), r.CorruptedEntries)
	}
}

// Abandon writes nothing and releases the floor: a write refused to the client
// never reaches the WAL (#1207).
func TestCoveredAbandonWritesNothing(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer writer.Close()

	var hooked int
	writer.SetReplicationHook(func(*ReplicationEntry) { hooked++ })
	p, err := writer.PrepareCovered("db1", "db1/cpu", coveredTestPayload(t, 2))
	if err != nil {
		t.Fatalf("PrepareCovered: %v", err)
	}
	if got := writer.PendingUnflushedCount(); got != 1 {
		t.Fatalf("pending after prepare = %d, want 1", got)
	}
	p.Abandon()
	p.Abandon() // idempotent
	if got := writer.PendingUnflushedCount(); got != 0 {
		t.Fatalf("pending after abandon = %d, want 0", got)
	}
	if hooked != 0 {
		t.Fatalf("replication hook fired %d times for an abandoned append", hooked)
	}
	path := writer.CurrentFile()
	if err := writer.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if entries := readAllEntries(t, writer, path); len(entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(entries))
	}
	if _, err := p.Commit(1); err == nil {
		t.Fatal("Commit after Abandon succeeded")
	}
}

// The hook fires at Commit with the coverage, never at Prepare.
func TestCoveredHookFiresAtCommitWithCoverage(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer writer.Close()

	var got []*ReplicationEntry
	writer.SetReplicationHook(func(e *ReplicationEntry) { got = append(got, e) })
	p, err := writer.PrepareCovered("db1", "db1/cpu", coveredTestPayload(t, 2))
	if err != nil {
		t.Fatalf("PrepareCovered: %v", err)
	}
	if len(got) != 0 {
		t.Fatal("hook fired at Prepare")
	}
	if _, err := p.Commit(9); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(got) != 1 || got[0].Coverage == nil || got[0].Coverage.CSeq != 9 || got[0].Coverage.Key != "db1/cpu" {
		t.Fatalf("hook entries = %+v", got)
	}
	if db, _ := ParseEnvelope(got[0].Payload, ""); db != "db1" {
		t.Fatalf("hook payload is not the enveloped logical payload (db=%q)", db)
	}
}

// A full queue drops at Commit: the batch still commits, entries already
// enqueued keep their tokens (they are in the WAL), the dropped one and the
// rest are released.
func TestCoveredCommitDropKeepsEnqueuedTokens(t *testing.T) {
	w := &Writer{entryChan: make(chan walEntry, 1), pendingSeqs: map[uint64]struct{}{}, trackedInstance: 5}
	p := &PreparedAppend{w: w, key: "db/m"}
	for i := 0; i < 3; i++ {
		seq := uint64(i + 1)
		w.pendingSeqs[seq] = struct{}{}
		p.entries = append(p.entries, preparedEntry{data: make([]byte, WALEntryHeaderSize+8), seq: seq, token: fmt.Sprintf("%016x%016x", 5, seq), chunk: uint16(i)})
	}
	tokens, err := p.Commit(1)
	if !errors.Is(err, ErrWALDropped) {
		t.Fatalf("Commit err = %v, want ErrWALDropped", err)
	}
	if len(tokens) != 1 || tokens[0] != p.entries[0].token {
		t.Fatalf("tokens = %v, want the first chunk's only", tokens)
	}
	if _, ok := w.pendingSeqs[1]; !ok {
		t.Fatal("enqueued chunk released from the floor")
	}
	if len(w.pendingSeqs) != 1 {
		t.Fatalf("pending = %v, want only seq 1", w.pendingSeqs)
	}
}

// A retained checkpoint stops replay but keeps the floor until
// ReleaseRegistered (#1207).
func TestMarkFlushedRetainedHoldsFloor(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	defer writer.Close()

	p, err := writer.PrepareCovered("db1", "db1/cpu", coveredTestPayload(t, 1))
	if err != nil {
		t.Fatalf("PrepareCovered: %v", err)
	}
	tokens, err := p.Commit(1)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := writer.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if err := writer.MarkFlushedRetained(tokens); err != nil {
		t.Fatalf("MarkFlushedRetained: %v", err)
	}
	if err := writer.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if n, err := writer.PurgeFlushed(writer.MinUnflushedSequence()); err != nil || n != 0 {
		t.Fatalf("PurgeFlushed before release = (%d, %v), want (0, nil)", n, err)
	}
	writer.ReleaseRegistered(tokens)
	if n, err := writer.PurgeFlushed(writer.MinUnflushedSequence()); err != nil || n == 0 {
		t.Fatalf("PurgeFlushed after release = (%d, %v), want > 0", n, err)
	}
}

// A malformed trailer is an error, not a silent mis-parse.
func TestParseCoveredPayloadRejectsBadTrailer(t *testing.T) {
	bad := make([]byte, walTrackedHeaderSize+walCoverageFixedTrailer)
	bad[0] = WALCoveredMarker
	// chunks = 0
	if _, _, _, err := parseCoveredPayload(bad); err == nil {
		t.Fatal("accepted chunks=0")
	}
	bad[len(bad)-9] = 1    // chunks = 1
	bad[len(bad)-13] = 200 // keyLen = 200 > available
	if _, _, _, err := parseCoveredPayload(bad); err == nil {
		t.Fatal("accepted a key longer than the payload")
	}
}

// Recovery hands a replayed covered entry's coverage to the replay callback.
func TestRecoveryPassesReplayCoverage(t *testing.T) {
	writer, dir := newTestWriter(t, SyncModeFsync)
	defer os.RemoveAll(dir)
	p, err := writer.PrepareCovered("db1", "db1/cpu", coveredTestPayload(t, 2))
	if err != nil {
		t.Fatalf("PrepareCovered: %v", err)
	}
	if _, err := p.Commit(11); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	var got []ReplayCoverage
	_, err = NewRecovery(dir, writer.logger).RecoverWithOptions(context.Background(), nil, &RecoveryOptions{
		ColumnarCallback: func(ctx context.Context, _, _ string, _ map[string][]interface{}, _ string) error {
			rc, ok := ReplayCoverageFrom(ctx)
			if !ok {
				t.Error("replay callback got no coverage")
			}
			got = append(got, rc)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(got) != 1 || got[0].Coverage.CSeq != 11 || got[0].Coverage.Key != "db1/cpu" || got[0].Coverage.Instance != writer.trackedInstance {
		t.Fatalf("replay coverage = %+v", got)
	}
}
