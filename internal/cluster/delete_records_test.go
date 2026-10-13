package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #1207 d1: DELETE records, the coordinator's side.

type fakeMarks map[string]uint64

func (m fakeMarks) FlushScopeAndWait(_ context.Context, db, measurement string) (map[string]uint64, error) {
	out := map[string]uint64{}
	for k, v := range m {
		if k == db+"/"+measurement || (measurement == "" && len(k) > len(db) && k[:len(db)+1] == db+"/") {
			out[k] = v
		}
	}
	return out, nil
}

// A writer a DELETE record names refuses writes until it acknowledged the
// record with the batches it had acknowledged; the loop does so.
func TestDeleteRecordAckLoopAndGate(t *testing.T) {
	r := newClaimantRig(t)
	c := r.c
	fsm := c.raftNode.FSM()
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-1", Instance: 0x11, FreshStart: true})
	c.deleteRecords.Store(&deleteRecordState{buffer: fakeMarks{"db/cpu": 4, "db/mem": 9}, instance: func() uint64 { return 0x11 }})
	if err := c.IngestGate()(); err != nil {
		t.Fatalf("gated with no record: %v", err)
	}
	// writer-2 is the primary for a while and runs a DELETE; then writer-1
	// is the primary again, with the same process.
	propose(t, c, raft.CommandPromoteWriter, raft.PromoteWriterPayload{NodeID: clOrigin})
	propose(t, c, raft.CommandRecordDelete, raft.RecordDeletePayload{Origin: clOrigin, Instance: clInst, Database: "db", Measurement: "cpu", Now: time.Now()})
	propose(t, c, raft.CommandPromoteWriter, raft.PromoteWriterPayload{NodeID: "writer-1"})
	if err := c.IngestGate()(); !errors.Is(err, errDeleteAckPending) {
		t.Fatalf("writes admitted before the acknowledgement: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.deleteAckLoop(ctx, fsm)
	waitFSM(t, func() bool { return !fsm.OwesDeleteAck(0x11) })
	if err := c.IngestGate()(); err != nil {
		t.Fatalf("gated after the acknowledgement: %v", err)
	}
	if v := c.StreamTakeover(); len(v.DeleteRecords) != 1 || v.DeleteRecords[0].Measurement != "cpu" || len(v.DeleteRecords[0].Unacknowledged) != 0 {
		t.Fatalf("listing %+v", v.DeleteRecords)
	}
	recs := fsm.DeleteRecords()
	if len(recs) != 1 || !recs[0].InScope(0x11, "db/cpu", 4) || recs[0].InScope(0x11, "db/cpu", 5) || recs[0].InScope(0x11, "db/mem", 1) {
		t.Fatalf("acknowledged scope %+v", recs)
	}
}

func TestRecordDeleteAndLocalFiles(t *testing.T) {
	r := newClaimantRig(t)
	c := r.c
	ctx := context.Background()
	if !c.DeleteRecordsActive() {
		t.Fatal("not active with stream takeover on")
	}
	if _, err := c.RecordDelete(ctx, "db", "cpu", "v > 1", nil); err == nil {
		t.Fatal("recorded on a writer not wired for it")
	}
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-1", Instance: 0x11, FreshStart: true})
	c.deleteRecords.Store(&deleteRecordState{buffer: fakeMarks{"db/cpu": 4}, instance: func() uint64 { return 0x11 }})
	id, err := c.RecordDelete(ctx, "db", "cpu", "v > 1", map[string]uint64{"db/cpu": 4, "db/mem": 9})
	if err != nil || id == 0 {
		t.Fatalf("record: %d %v", id, err)
	}
	recs := c.raftNode.FSM().DeleteRecords()
	if len(recs) != 1 || recs[0].ID != id || recs[0].Where != "v > 1" || !recs[0].InScope(0x11, "db/cpu", 4) || recs[0].InScope(0x11, "db/cpu", 5) {
		t.Fatalf("records %+v", recs)
	}

	hour := time.Date(2026, 10, 12, 10, 0, 0, 0, time.UTC)
	reg := func(path, tier string) {
		propose(t, c, raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{Path: path, SHA256: "x", SizeBytes: 3,
			Database: "db", Measurement: "cpu", PartitionTime: hour, OriginNodeID: clOrigin, Tier: tier, CreatedAt: time.Now()}})
	}
	reg("db/cpu/2026/10/12/10/here.parquet", "hot")
	reg("db/cpu/2026/10/12/10/away.parquet", "hot")
	reg("db/cpu/2026/10/12/10/cold.parquet", "cold")
	reg("db/cpu/2026/10/12/10/short.parquet", "hot")
	if err := r.backend.Write(ctx, "db/cpu/2026/10/12/10/short.parquet", []byte("ab")); err != nil {
		t.Fatal(err)
	}
	if err := r.backend.Write(ctx, "db/cpu/2026/10/12/10/here.parquet", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	missing, err := c.MissingLocalFiles(ctx, "db", "cpu")
	if err != nil || len(missing) != 2 || missing[0] != "db/cpu/2026/10/12/10/away.parquet" || missing[1] != "db/cpu/2026/10/12/10/short.parquet" {
		t.Fatalf("missing %v %v", missing, err)
	}
	if m, _ := c.MissingLocalFiles(ctx, "db", "mem"); len(m) != 0 {
		t.Fatalf("another measurement: %v", m)
	}
	if m, _ := c.MissingLocalFiles(ctx, "db", ""); len(m) != 2 {
		t.Fatalf("whole database: %v", m)
	}
	start := time.Now()
	if m, _ := c.WaitLocalFiles(ctx, "db", "cpu", 300*time.Millisecond); len(m) != 2 || time.Since(start) < 300*time.Millisecond {
		t.Fatalf("wait %v after %v", m, time.Since(start))
	}
	if err := r.backend.Write(ctx, "db/cpu/2026/10/12/10/away.parquet", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := r.backend.Write(ctx, "db/cpu/2026/10/12/10/short.parquet", []byte("abc")); err != nil {
		t.Fatal(err)
	}
	if m, err := c.WaitLocalFiles(ctx, "db", "cpu", time.Second); len(m) != 0 || err != nil {
		t.Fatalf("wait %v %v", m, err)
	}
}
