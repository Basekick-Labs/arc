package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/wal"
)

// republishRig: the rig's buffer writes batches 1-3 of key db/cpu under its
// WAL's instance, which the cluster knows as an earlier instance of
// writer-1 (this node) — the process republishing it runs as another one.
// Batch 1 was registered by another file first, so the flushed file, holding
// all three, is refused as a duplicate and parked.
func republishRig(t *testing.T) (*claimantRigT, uint64, *republisher) {
	t.Helper()
	r := newClaimantRig(t)
	ctx := context.Background()
	inst := r.wal.TrackedInstance()
	r.outbox.SetLocalParking(LocalParking{Base: r.backend.(*storage.LocalBackend).GetBasePath()})
	propose(t, r.c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-1", Instance: inst, FreshStart: true})
	hour := time.Now().UTC().Truncate(time.Hour)
	for i := 0; i < 3; i++ {
		ts := hour.Add(time.Duration(i) * time.Second).UnixMicro()
		if err := r.buffer.WriteColumnarDirect(ctx, "db", "cpu", map[string][]interface{}{"time": {ts}, "value": {float64(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	// Batch 1 registered elsewhere (a file this node holds).
	propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/first.parquet", SHA256: "f", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: hour, OriginNodeID: "writer-1", CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: inst, Key: "db/cpu", Ranges: [][2]uint64{{1, 1}}}}})
	if err := r.backend.Write(ctx, "db/cpu/first.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := r.buffer.FlushAllAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	parked := 0
	for _, f := range r.outbox.InstanceFiles(inst) {
		if f.Parked {
			parked++
		}
	}
	if parked != 1 {
		t.Fatalf("parked files %d", parked)
	}
	rp := newRepublisher(r.c, r.outbox, r.buffer, filepath.Dir(r.wal.CurrentFile()), func() uint64 { return inst + 1 })
	return r, inst, rp
}

// The rows of a parked file nobody registered are republished from the WAL,
// once; the parked file then resolves.
func TestRepublishParkedRowsFromTheWAL(t *testing.T) {
	r, inst, rp := republishRig(t)
	ctx := context.Background()
	fsm := r.c.raftNode.FSM()
	hour := time.Now().UTC().Truncate(time.Hour)
	rp.pass(ctx)
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	for c := uint64(2); c <= 3; c++ {
		if !fsm.CoverageCovered(inst, "db/cpu", raft.CoveragePiece{CSeq: c}, hour) {
			t.Fatalf("batch %d not republished", c)
		}
	}
	files := len(fsm.GetFilesByDatabase("db"))
	rp.pass(ctx)
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	if n := len(fsm.GetFilesByDatabase("db")); n != files {
		t.Fatalf("republished twice: %d -> %d files", files, n)
	}
	r.outbox.resolveParked(ctx, rigManifest{r.c}, rigManifest{r.c}, r.backend)
	for _, f := range r.outbox.InstanceFiles(inst) {
		if f.Parked {
			t.Fatalf("parked file not resolved: %+v", f)
		}
	}
}

// The current process's own batches are left to its flush groups; batches
// an unfinished claim holds are its claimant's.
func TestRepublishSkipsTheCurrentInstanceAndClaimedBatches(t *testing.T) {
	r, inst, rp := republishRig(t)
	ctx := context.Background()
	rp.instance = func() uint64 { return inst }
	if c := rp.candidates(ctx, r.c.raftNode.FSM()); len(c) != 0 {
		t.Fatalf("current instance republished: %+v", c)
	}
	rp.instance = func() uint64 { return inst + 1 }
	// writer-2 (designated) claims batch 2 of it.
	propose(t, r.c, raft.CommandPromoteWriter, raft.PromoteWriterPayload{NodeID: clOrigin})
	propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-1", By: clOrigin, Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: inst, Keys: map[string]raft.Intervals{"db/cpu": {{2, 2}}}}}})
	c := rp.candidates(ctx, r.c.raftNode.FSM())
	if len(c[inst]["db/cpu"]) != 1 || c[inst]["db/cpu"][3] == nil {
		t.Fatalf("candidates %+v", c)
	}
}

type fakeRepublishOutbox struct {
	insts []uint64
	files map[uint64][]InstanceFile
}

func (f fakeRepublishOutbox) Instances() []uint64                      { return f.insts }
func (f fakeRepublishOutbox) InstanceFiles(inst uint64) []InstanceFile { return f.files[inst] }

// Only exact, parked origin files are candidates: a claimant's parked file
// (a claim epoch) has no WAL copy here.
func TestRepublishCandidatesSkipClaimantFiles(t *testing.T) {
	r := newClaimantRig(t)
	hour := r.hour
	fo := fakeRepublishOutbox{insts: []uint64{clInst}, files: map[uint64][]InstanceFile{clInst: {
		{Path: "a", Key: clKey, PartitionTime: hour, Ranges: [][2]uint64{{1, 1}}, Exact: true, Parked: true},
		{Path: "b", Key: clKey, PartitionTime: hour, Ranges: [][2]uint64{{2, 2}}, Exact: true, Parked: true, Epoch: 9},
		{Path: "c", Key: clKey, PartitionTime: hour, Ranges: [][2]uint64{{3, 3}}, Exact: false, Parked: true},
	}}}
	rp := newRepublisher(r.c, fo, r.buffer, t.TempDir(), func() uint64 { return 1 })
	c := rp.candidates(context.Background(), r.c.raftNode.FSM())
	if len(c[clInst][clKey]) != 1 || c[clInst][clKey][1] == nil {
		t.Fatalf("candidates %+v", c)
	}
}

// Keep returns a record's candidate rows only in the parked file's hour,
// minus the rows registered there.
func TestRepublishKeep(t *testing.T) {
	r := newClaimantRig(t)
	fsm := r.c.raftNode.FSM()
	propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/x.parquet", SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: r.hour, OriginNodeID: clOrigin, CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: clInst, Key: clKey, Partial: []raft.CoveragePiece{{CSeq: 4, RowLo: 0, RowHi: 2, RowTotal: 6}}}}})
	keep := republishKeep(fsm, []republishCand{{hour: r.hour, chunk: -1}}, &wal.Coverage{Instance: clInst, Key: clKey, CSeq: 4, Chunks: 1})
	if got, err := keep(r.hour, 6); err != nil || len(got) != 1 || got[0] != [2]int{2, 6} {
		t.Fatalf("keep %v %v", got, err)
	}
	if got, _ := keep(r.hour.Add(time.Hour), 6); got != nil {
		t.Fatalf("another hour %v", got)
	}
	rows := republishKeep(fsm, []republishCand{{hour: r.hour, chunk: -1, rowLo: 1, rowHi: 4, rowTotal: 6}}, &wal.Coverage{Instance: clInst, Key: clKey, CSeq: 4, Chunks: 1})
	if got, _ := rows(r.hour, 6); len(got) != 1 || got[0] != [2]int{2, 4} {
		t.Fatalf("row range %v", got)
	}
	if _, err := rows(r.hour, 7); err == nil {
		t.Fatal("other coordinates accepted")
	}
	chunk := republishKeep(fsm, []republishCand{{hour: r.hour, chunk: 1}}, &wal.Coverage{Instance: clInst, Key: clKey, CSeq: 5, Chunk: 0, Chunks: 2})
	if got, _ := chunk(r.hour, 3); got != nil {
		t.Fatalf("another chunk %v", got)
	}
}

// A WAL file holding none of the candidates is not read; the summary
// follows the file.
func TestRepublishFileMayHold(t *testing.T) {
	r, inst, rp := republishRig(t)
	path := r.wal.CurrentFile()
	_ = r.wal.Rotate()
	cands := map[uint64]map[string]map[uint64][]republishCand{inst: {"db/cpu": {2: nil}}}
	if !rp.fileMayHold(path, cands) {
		t.Fatal("file with the batch")
	}
	if rp.fileMayHold(path, map[uint64]map[string]map[uint64][]republishCand{inst: {"db/cpu": {99: nil}}}) {
		t.Fatal("file without the batch")
	}
}

// A WAL file and its copy (the same entries twice) republish once.
func TestRepublishReadsAnEntryOnce(t *testing.T) {
	r, inst, rp := republishRig(t)
	src := r.wal.CurrentFile()
	_ = r.wal.Rotate()
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(src+".copy.wal", raw, 0o600); err != nil {
		t.Fatal(err)
	}
	rp.pass(context.Background())
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	if !r.c.raftNode.FSM().CoverageCovered(inst, "db/cpu", raft.CoveragePiece{CSeq: 3}, time.Now().UTC().Truncate(time.Hour)) {
		t.Fatal("not republished")
	}
}

// A whole parked batch whose rows end up registered as row ranges (part by
// another file, the rest republished) is never whole in the manifest's
// eyes: the republisher verifies it and the parked file resolves.
func TestRepublishResolvesABatchRegisteredInParts(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	inst := r.wal.TrackedInstance()
	r.outbox.SetLocalParking(LocalParking{Base: r.backend.(*storage.LocalBackend).GetBasePath()})
	propose(t, r.c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-1", Instance: inst, FreshStart: true})
	hour := time.Now().UTC().Truncate(time.Hour)
	ts := func(i int) int64 { return hour.Add(time.Duration(i) * time.Second).UnixMicro() }
	if err := r.buffer.WriteColumnarDirect(ctx, "db", "cpu", map[string][]interface{}{"time": {ts(0), ts(1)}, "value": {0.0, 1.0}}); err != nil {
		t.Fatal(err)
	}
	// Row 0 of batch 1 registered elsewhere: the flushed file (all of batch
	// 1) is refused as a duplicate and parked.
	propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: "db/cpu/part.parquet", SHA256: "p", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: hour, OriginNodeID: "writer-1", CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: inst, Key: "db/cpu", Partial: []raft.CoveragePiece{{CSeq: 1, RowLo: 0, RowHi: 1, RowTotal: 2}}}}})
	if err := r.backend.Write(ctx, "db/cpu/part.parquet", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := r.buffer.FlushAllAndWait(ctx); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	rp := newRepublisher(r.c, r.outbox, r.buffer, filepath.Dir(r.wal.CurrentFile()), func() uint64 { return inst + 1 })
	r.outbox.SetParkedCovered(rp.ParkedCovered)
	parked := func() int {
		n := 0
		for _, f := range r.outbox.InstanceFiles(inst) {
			if f.Parked {
				n++
			}
		}
		return n
	}
	if parked() != 1 {
		t.Fatalf("parked %d", parked())
	}
	rp.pass(ctx)
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	if rp.ParkedCovered(inst, "db/cpu", 1, hour) {
		t.Fatal("verified on a write, before its file registered")
	}
	rp.pass(ctx)
	waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	st, ranges, _, _ := r.c.raftNode.FSM().CoveredRows(inst, "db/cpu", 1, 0, 1, hour)
	if st != raft.CoveredRanges || len(complementRows(ranges, 2)) != 0 {
		t.Fatalf("rows %v %v", st, ranges)
	}
	if !rp.ParkedCovered(inst, "db/cpu", 1, hour) {
		t.Fatal("not verified")
	}
	if c := rp.candidates(ctx, r.c.raftNode.FSM()); len(c) != 0 {
		t.Fatalf("a verified batch is still a candidate: %+v", c)
	}
	r.outbox.resolveParked(ctx, rigManifest{r.c}, rigManifest{r.c}, r.backend)
	if parked() != 0 {
		t.Fatal("parked file not resolved")
	}
	rp.candidates(ctx, r.c.raftNode.FSM())
	if len(rp.covered) != 0 {
		t.Fatalf("verdicts kept after the parked file went: %v", rp.covered)
	}
}

type failingReplay struct{}

func (failingReplay) ReplayClaimed(context.Context, ingest.ClaimedReplay) ([]ingest.HourOutcome, error) {
	return nil, errors.New("disk full")
}

// A replay call that fails verifies nothing.
func TestRepublishFailedCallVerifiesNothing(t *testing.T) {
	r, inst, rp := republishRig(t)
	rp.replay = failingReplay{}
	rp.pass(context.Background())
	hour := time.Now().UTC().Truncate(time.Hour)
	for c := uint64(1); c <= 3; c++ {
		if rp.ParkedCovered(inst, "db/cpu", c, hour) {
			t.Fatalf("batch %d verified", c)
		}
	}
	_ = r
}

// An untracked instance has nothing to arbitrate its copies: none.
func TestRepublishSkipsUntrackedInstances(t *testing.T) {
	r := newClaimantRig(t)
	// raft-1 registered files before takeover was on: its instance is not
	// tracked.
	propose(t, r.c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "raft-1", Instance: 0x777, FreshStart: false})
	file := func(inst uint64) map[uint64][]InstanceFile {
		return map[uint64][]InstanceFile{inst: {{Path: "a", Key: clKey, PartitionTime: r.hour, Ranges: [][2]uint64{{1, 1}}, Exact: true, Parked: true}}}
	}
	rp := newRepublisher(r.c, fakeRepublishOutbox{insts: []uint64{clInst}, files: file(clInst)}, r.buffer, t.TempDir(), func() uint64 { return 1 })
	if c := rp.candidates(context.Background(), r.c.raftNode.FSM()); len(c) != 1 {
		t.Fatalf("tracked: %+v", c)
	}
	rp.outbox = fakeRepublishOutbox{insts: []uint64{0x777}, files: file(0x777)}
	if c := rp.candidates(context.Background(), r.c.raftNode.FSM()); len(c) != 0 {
		t.Fatalf("untracked: %+v", c)
	}
}
