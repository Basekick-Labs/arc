package cluster

// #1207 S4b1c end to end, without the claimant loop: records of a claimed
// instance decoded from their stored payloads, replayed by the ingest
// buffer, sent by the real outbox and arbitrated by the real manifest.

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

func logicalPayload(t *testing.T, db string, v any) []byte {
	t.Helper()
	raw, err := msgpack.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	out := []byte{wal.WALEnvelopeMarker, 0, 0}
	binary.BigEndian.PutUint16(out[1:], uint16(len(db)))
	return append(append(out, db...), raw...)
}

func TestComplementRows(t *testing.T) {
	for _, tc := range []struct {
		in    [][2]int
		total int
		want  string
	}{
		{nil, 4, "[[0 4]]"},
		{[][2]int{{0, 4}}, 4, "[]"},
		{[][2]int{{1, 2}}, 4, "[[0 1] [2 4]]"},
		{[][2]int{{0, 2}, {1, 3}}, 4, "[[3 4]]"},
	} {
		if got := complementRows(tc.in, tc.total); fmt.Sprint(got) != tc.want {
			t.Errorf("%v of %d: %v, want %s", tc.in, tc.total, got, tc.want)
		}
	}
}

// rigManifest is the coordinator as the outbox's manifest; the struct-literal
// rig has no leader address book, so leader waits answer from Raft directly.
type rigManifest struct{ *Coordinator }

func (m rigManifest) WaitForLeader(time.Duration) error { return nil }

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestClaimedReplayIsPublishedOnceThroughTheManifest(t *testing.T) {
	c, _ := takeoverOriginRig(t, true) // writer-1, the designated primary
	ctx := context.Background()
	const inst = 0x500
	key := "db/cpu"
	h1 := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	h2 := h1.Add(time.Hour)
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: inst, FreshStart: true})
	// The origin registered rows [0, 1) of batch 2 in hour 1 before it died.
	propose(t, c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/origin.parquet", SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: h1, OriginNodeID: "writer-2", CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: inst, Key: key, Partial: []raft.CoveragePiece{{CSeq: 2, RowLo: 0, RowHi: 1, RowTotal: 2}}}}})
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: inst, Keys: map[string]raft.Intervals{key: {{1, 2}}}}}})
	fsm := c.raftNode.FSM()
	claims := fsm.ClaimsBy("writer-1")
	if len(claims) != 1 {
		t.Fatalf("claims %+v", claims)
	}
	epoch := claims[0].Epoch

	logger := zerolog.Nop()
	backend, err := storage.NewLocalBackend(t.TempDir(), logger)
	if err != nil {
		t.Fatal(err)
	}
	w, err := wal.NewWriter(&wal.WriterConfig{WALDir: t.TempDir(), SyncMode: wal.SyncModeAsync, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.Close() })
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{MaxBufferSize: 1000000, MaxBufferAgeMS: 600000,
		Compression: "snappy", ShardCount: 4, FlushWorkers: 2, FlushQueueSize: 8}, backend, logger)
	buffer.SetWAL(w)
	outbox := newTestOutbox(t, t.TempDir())
	buffer.SetFileRegistrar(outbox)
	if err := buffer.EnableCoverage(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = buffer.Close() })
	c.raftFSM = fsm
	outbox.HandOff(rigManifest{c}, backend, "writer-1")

	us := func(h time.Time, s int) int64 { return h.Add(time.Duration(s) * time.Second).UnixMicro() }
	// Batch 1: columnar, rows in hours 1 and 2. Batch 2: row form, both rows
	// in hour 1, row 0 already registered by the origin.
	b1 := wal.DecodeLogical(logicalPayload(t, "db", map[string]any{"m": "cpu", "columns": map[string]any{
		"time": []any{us(h1, 1), us(h2, 1)}, "value": []any{1.0, 2.0}}}))
	b2 := wal.DecodeLogical(logicalPayload(t, "db", []map[string]any{
		{"_database": "db", "_measurement": "cpu", "time": us(h1, 2), "value": 3.0},
		{"_database": "db", "_measurement": "cpu", "time": us(h1, 3), "value": 4.0}}))
	rec := func(cseq uint64, e *wal.Entry) ingest.ClaimedRecord {
		return ingest.ClaimedRecord{CSeq: cseq, Chunks: 1, Entry: e, Keep: claimKeep(fsm, inst, key, cseq, 0, 1)}
	}
	replay := ingest.ClaimedReplay{Database: "db", Measurement: "cpu", Instance: inst, Epoch: epoch,
		Records: []ingest.ClaimedRecord{rec(1, b1), rec(2, b2)}}
	out, err := buffer.ReplayClaimed(ctx, replay)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range out {
		if o.Outcome != ingest.ReplayWritten {
			t.Fatalf("outcome %+v", o)
		}
	}
	covered := func(cseq uint64, h time.Time) bool {
		st, ranges, _, _ := fsm.CoveredRows(inst, key, cseq, 0, 1, h)
		return st == raft.CoveredAll || (cseq == 2 && len(ranges) == 2)
	}
	waitFSM(t, func() bool { return covered(1, h1) && covered(1, h2) && covered(2, h1) })
	waitFSM(t, func() bool { return outbox.Pending() == 0 })
	// Replaying again publishes nothing: every hour is registered now.
	out, err = buffer.ReplayClaimed(ctx, replay)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range out {
		if o.Outcome != ingest.ReplayCovered && o.Outcome != ingest.ReplayNothingLeft {
			t.Fatalf("second replay %+v", o)
		}
	}
	// A copy without the claim epoch is the origin's, and it is refused.
	err = c.applyOrForward(ctx, &raft.Command{Type: raft.CommandRegisterFile, Payload: mustMarshal(t, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/late.parquet", SHA256: "y", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: h2, OriginNodeID: "writer-2", CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: inst, Key: key, Ranges: [][2]uint64{{1, 1}}}}})})
	if ops := raft.StreamClaimConflictOps(err); len(ops) != 1 || ops[0].Kind != raft.RefusedClaimed {
		t.Fatalf("origin copy: %v", err)
	}
}

// A record whose registered rows were counted against another row total
// cannot be subtracted row by row: its hour is not replayed.
func TestClaimKeepRefusesADifferentRowTotal(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	h := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x500, FreshStart: true})
	propose(t, c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/o.parquet", SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: h, OriginNodeID: "writer-2", CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: 0x500, Key: "db/cpu", Partial: []raft.CoveragePiece{{CSeq: 2, RowLo: 0, RowHi: 1, RowTotal: 3}}}}})
	keep := claimKeep(c.raftNode.FSM(), 0x500, "db/cpu", 2, 0, 1)
	if _, err := keep(h, 2); err == nil {
		t.Fatal("a different row total was subtracted")
	}
	if got, err := keep(h, 3); err != nil || fmt.Sprint(got) != "[[1 3]]" {
		t.Fatalf("same total: %v %v", got, err)
	}
	if got, err := claimKeep(c.raftNode.FSM(), 0x999, "db/cpu", 2, 0, 1)(h, 3); err == nil {
		t.Fatalf("untracked instance: %v", got)
	}
	if got, err := claimKeep(c.raftNode.FSM(), 0x500, "db/cpu", 9, 0, 1)(h, 3); err != nil || fmt.Sprint(got) != "[[0 3]]" {
		t.Fatalf("nothing registered: %v %v", got, err)
	}
}
