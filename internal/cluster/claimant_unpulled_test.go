package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/filereplication"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

const unpulledPath = "db/cpu/origin.parquet"

// registerOriginFile registers a file of writer-2 holding row 0 of batch 1
// (of 2 rows), which no node has.
func (r *claimantRigT) registerOriginFile(t *testing.T) {
	t.Helper()
	propose(t, r.c, raft.CommandRegisterFile, raft.RegisterFilePayload{
		File: raft.FileEntry{Path: unpulledPath, SHA256: "x", SizeBytes: 1, Database: "db", Measurement: "cpu",
			PartitionTime: r.hour, OriginNodeID: clOrigin, CreatedAt: time.Now()},
		Cov2: []raft.InstanceCoverage{{Instance: clInst, Key: clKey, Partial: []raft.CoveragePiece{{CSeq: 1, RowLo: 0, RowHi: 1, RowTotal: 2}}}}})
}

// healthyPeer puts a healthy reader in the registry: the claimant asks it.
func (r *claimantRigT) healthyPeer(t *testing.T) {
	t.Helper()
	n := NewNode("reader-1", "reader-1", RoleReader, "")
	n.Address, n.State = "127.0.0.1:1", StateHealthy
	if err := r.c.registry.Register(n); err != nil {
		t.Fatal(err)
	}
}

// A file whose coverage a DELETE rewrite inherited is never removed: a
// standby copy still holds the deleted rows.
func TestClaimantIgnoresInheritedCoverage(t *testing.T) {
	r := newClaimantRig(t)
	r.registerOriginFile(t)
	rw := raft.RegisterFilePayload{File: raft.FileEntry{Path: "db/cpu/rw.parquet", SHA256: "y", SizeBytes: 1, Database: "db", Measurement: "cpu",
		PartitionTime: r.hour, OriginNodeID: "writer-1", CreatedAt: time.Now()}, InheritFrom: unpulledPath}
	propose(t, r.c, raft.CommandBatchFileOps, raft.BatchFileOpsPayload{Ops: []raft.BatchFileOp{
		{Type: raft.CommandRegisterFile, Payload: mustJSONc(t, rw)},
		{Type: raft.CommandDeleteFile, Payload: mustJSONc(t, raft.DeleteFilePayload{Path: unpulledPath, Reason: "delete-rewrite"})}}})
	if files := r.c.raftNode.FSM().HotFilesOfInstance(clInst); len(files) != 0 {
		t.Fatalf("files %+v", files)
	}
}

// A retired instance is claimed by nobody, and its standby copy goes.
func TestClaimantDropsARetiredCopy(t *testing.T) {
	r := newClaimantRig(t)
	ctx := context.Background()
	r.stored(t, 1, 1)
	// writer-2 restarts on new disks: orphaned; writer-1 claims, publishes.
	propose(t, r.c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: clOrigin, Instance: 0x600, FreshStart: true})
	r.runClaim(t, 3)
	r.stored(t, 1, 1)
	for _, n := range r.c.raftNode.FSM().GetNodesByRole(string(RoleWriter)) {
		if n.ID != "writer-1" && n.ID != clOrigin {
			propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: n.ID, Now: time.Now(),
				Claims: []raft.ClaimSet{{Instance: clInst}}})
		}
	}
	in := streamInstance(r.c, clInst)
	if in == nil || in.Status != raft.InstanceRetired {
		t.Fatalf("instance %+v", in)
	}
	if kind := claimKindOf(*in, map[uint64]raft.StreamClaim{}, "writer-3", "writer-3", false); kind != claimNone {
		t.Fatalf("a retired instance is claimable: %v", kind)
	}
	r.cl.pass(ctx)
	if r.store.Held(clOrigin, clInst) != nil {
		t.Fatal("copy of a retired instance kept")
	}
}

func mustJSONc(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

type probeFetcher struct {
	err     error
	offset  int64
	hashSet bool
}

func (f *probeFetcher) Fetch(_ context.Context, _ string, _ *raft.FileEntry, _ io.Writer, off int64, h hash.Hash) (int64, error) {
	f.offset, f.hashSet = off, h != nil
	return 0, f.err
}

// A peer refusing a fetch at the end of the file as a bad offset has it;
// one whose storage lacks it does not; one whose manifest lacks it, or
// that fails otherwise, cannot tell.
func TestProbeFileAnswers(t *testing.T) {
	c := &Coordinator{}
	e := raft.FileEntry{Path: "p", SizeBytes: 10, SHA256: "s"}
	for _, tc := range []struct {
		err  error
		want probeAnswer
	}{
		{fmt.Errorf("%w: x", filereplication.ErrBadOffset), probeHas},
		{fmt.Errorf("%w: x", filereplication.ErrFileNotOnPeer), probeAbsent},
		{fmt.Errorf("%w: %w: x", filereplication.ErrFileNotOnPeer, filereplication.ErrNotInPeerManifest), probeUnknown},
		{errors.New("dial"), probeUnknown},
		{nil, probeUnknown},
	} {
		f := &probeFetcher{err: tc.err}
		c.fetcher = f
		if got := c.probeFile(context.Background(), "a", e); got != tc.want || f.offset != 10 || !f.hashSet {
			t.Fatalf("%v: %v (offset %d)", tc.err, got, f.offset)
		}
	}
	// An empty file: a whole fetch succeeding means it is there.
	f := &probeFetcher{}
	c.fetcher = f
	if got := c.probeFile(context.Background(), "a", raft.FileEntry{Path: "p", SHA256: "s"}); got != probeHas || f.offset != 0 || f.hashSet {
		t.Fatalf("empty file: %v", got)
	}
	c.fetcher = nil
	if c.probeFile(context.Background(), "a", e) != probeUnknown {
		t.Fatal("no fetcher")
	}
}

// A file that changed since it was found missing is not removed.
func TestVanishSkipsAChangedEntry(t *testing.T) {
	r := newClaimantRig(t)
	r.registerOriginFile(t)
	f, _ := r.c.raftNode.FSM().GetFile(unpulledPath)
	stale := *f
	stale.SHA256 = "other"
	r.cl.vanish(context.Background(), r.c.raftNode.FSM(), raft.StreamClaim{Instance: clInst, Origin: clOrigin}, stale, probeAbsent)
	if _, ok := r.c.raftNode.FSM().GetFile(unpulledPath); !ok {
		t.Fatal("removed a file that changed")
	}
}

// A writer that never claimed a retired instance drops its copy of it.
func TestRetiredCopyDroppedByAWriterThatNeverClaimedIt(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 1, 1)
	propose(t, r.c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: clOrigin, Instance: 0x600, FreshStart: true})
	propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: "raft-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: clInst, Keys: map[string]raft.Intervals{clKey: {{1, 1}}}}}})
	k, _ := r.claim(t)
	propose(t, r.c, raft.CommandClaimDone, raft.ClaimDonePayload{Instance: clInst, By: "raft-1", Epoch: k.Epoch, Now: time.Now()})
	propose(t, r.c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: clOrigin, By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: clInst}}})
	if in := streamInstance(r.c, clInst); in == nil || in.Status != raft.InstanceRetired {
		t.Fatalf("instance %+v", in)
	}
	r.cl.pass(context.Background())
	if r.store.Held(clOrigin, clInst) != nil {
		t.Fatal("copy of a retired instance kept")
	}
}

// runClaim runs passes, each a little later (unpulledEvery), letting the
// outbox drain in between.
func (r *claimantRigT) runClaim(t *testing.T, passes int) raft.StreamClaim {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < passes; i++ {
		at := r.cl.now().Add(unpulledEvery)
		r.cl.now = func() time.Time { return at }
		r.cl.pass(ctx)
		waitFSM(t, func() bool { return r.outbox.Pending() == 0 })
	}
	k, _ := r.claim(t)
	return k
}

// The dead writer is a member nobody can ask: a file only it had is removed
// after twice the grace, and its rows are published from the standby copy,
// which holds every batch of it; the claim finishes only then.
func TestClaimantRepublishesAFileNoNodeHolds(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 1, 2)
	r.registerOriginFile(t)
	r.healthyPeer(t)
	asked := 0
	r.cl.probe = func(_ context.Context, addr string, e raft.FileEntry) probeAnswer {
		asked++
		if addr != "127.0.0.1:1" || e.Path != unpulledPath {
			t.Fatalf("probe %s %s", addr, e.Path)
		}
		return probeAbsent
	}
	r.afterGrace()
	if k := r.runClaim(t, 3); k.Done {
		t.Fatal("finished before twice the grace")
	}
	if _, ok := r.c.raftNode.FSM().GetFile(unpulledPath); !ok {
		t.Fatal("removed before twice the grace")
	}
	k := r.runClaim(t, 5)
	fsm := r.c.raftNode.FSM()
	if !k.Done || len(k.Unreplayable) != 0 || asked == 0 {
		t.Fatalf("claim %+v, asked %d", k, asked)
	}
	if _, ok := fsm.GetFile(unpulledPath); ok {
		t.Fatal("file no node holds still registered")
	}
	if v := fsm.VanishedFiles(); len(v) != 1 || v[0].Path != unpulledPath {
		t.Fatalf("vanished %+v", v)
	}
	// Both rows registered again: row 0 now from the standby copy.
	st, ranges, _, _ := fsm.CoveredRows(clInst, clKey, 1, 0, 1, r.hour)
	if st != raft.CoveredAll && !(st == raft.CoveredRanges && len(complementRows(ranges, 2)) == 0) {
		t.Fatalf("batch 1 not published whole again: %v %v", st, ranges)
	}
}

// Every member answering it does not have the file: removed at once.
func TestClaimantRemovesAFileEveryMemberLacks(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 1, 2)
	r.registerOriginFile(t)
	for _, id := range []string{"raft-1", "reader-1", clOrigin} {
		n := NewNode(id, id, RoleReader, "")
		n.Address, n.State = id+":1", StateHealthy
		if err := r.c.registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	r.cl.probe = func(context.Context, string, raft.FileEntry) probeAnswer { return probeAbsent }
	k := raft.StreamClaim{Instance: clInst, Origin: clOrigin}
	r.cl.unpulledSettled(context.Background(), r.c.raftNode.FSM(), k, r.cl.workFor(k))
	if _, ok := r.c.raftNode.FSM().GetFile(unpulledPath); ok {
		t.Fatal("kept though every member lacks it")
	}
}

// A peer that holds it: the claim waits for the pull, nothing is removed.
func TestClaimantWaitsForAFileAPeerHolds(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 1, 2)
	r.registerOriginFile(t)
	r.healthyPeer(t)
	r.cl.probe = func(context.Context, string, raft.FileEntry) probeAnswer { return probeHas }
	r.afterGrace()
	k := r.runClaim(t, 8)
	if k.Done {
		t.Fatal("claim finished without the file")
	}
	if _, ok := r.c.raftNode.FSM().GetFile(unpulledPath); !ok {
		t.Fatal("a file a peer holds was removed")
	}
	if r.cl.work[clInst].unpulled != 1 {
		t.Fatalf("unpulled %d", r.cl.work[clInst].unpulled)
	}
	// A copy of another size is not it.
	if err := r.backend.Write(context.Background(), unpulledPath, []byte("xy")); err != nil {
		t.Fatal(err)
	}
	if k = r.runClaim(t, 2); k.Done {
		t.Fatal("finished with a copy of another size")
	}
	// Pulled: the claim finishes, row 0 stays the origin's.
	if err := r.backend.Write(context.Background(), unpulledPath, []byte("x")); err != nil {
		t.Fatal(err)
	}
	k = r.runClaim(t, 3)
	if !k.Done || len(r.c.raftNode.FSM().VanishedFiles()) != 0 {
		t.Fatalf("claim %+v", k)
	}
}

// A file the standby copy could not publish again is never removed and does
// not hold the claim.
func TestClaimantLeavesAFileItCannotRepublish(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 2, 1) // batch 1, the file's, is not in the copy
	r.registerOriginFile(t)
	r.healthyPeer(t)
	r.cl.probe = func(context.Context, string, raft.FileEntry) probeAnswer { return probeAbsent }
	r.afterGrace()
	k := r.runClaim(t, 10)
	if !k.Done {
		t.Fatalf("claim %+v", k)
	}
	if _, ok := r.c.raftNode.FSM().GetFile(unpulledPath); !ok {
		t.Fatal("removed a file the copy cannot publish again")
	}
}

// Without peer replication nothing is pulled and nobody asked.
func TestClaimantWithoutPeerReplication(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 1, 2)
	r.registerOriginFile(t)
	r.cl.probe, r.c.fetcher = nil, nil
	r.afterGrace()
	if k := r.runClaim(t, 3); !k.Done {
		t.Fatalf("claim %+v", k)
	}
	if _, ok := r.c.raftNode.FSM().GetFile(unpulledPath); !ok {
		t.Fatal("removed")
	}
}

// The origin is asked like any member (a writer back on new disks may still
// have the file); a member not heard from cannot answer.
func TestLocateAsksTheOriginAndWaitsForTheSilent(t *testing.T) {
	r := newClaimantRig(t)
	r.registerOriginFile(t)
	f, _ := r.c.raftNode.FSM().GetFile(unpulledPath)
	origin := NewNode(clOrigin, clOrigin, RoleWriter, "")
	origin.Address, origin.State = "origin:1", StateHealthy
	if err := r.c.registry.Register(origin); err != nil {
		t.Fatal(err)
	}
	probe := func(_ context.Context, addr string, _ raft.FileEntry) probeAnswer {
		if addr == "origin:1" {
			return probeHas
		}
		return probeAbsent
	}
	if got := r.cl.locate(context.Background(), r.c.raftNode.FSM(), *f, probe); got != probeHas {
		t.Fatalf("origin not asked: %v", got)
	}
	absent := func(context.Context, string, raft.FileEntry) probeAnswer { return probeAbsent }
	if got := r.cl.locate(context.Background(), r.c.raftNode.FSM(), *f, absent); got != probeUnknown {
		t.Fatalf("members not heard from counted absent: %v", got)
	}
}

// Nobody able to answer: removed only after twice the grace from the first
// unanswered check; checks are throttled while files are missing.
func TestUnpulledUnknownWindowAndThrottle(t *testing.T) {
	r := newClaimantRig(t)
	r.stored(t, 1, 2)
	r.registerOriginFile(t)
	asked := 0
	r.cl.probe = func(context.Context, string, raft.FileEntry) probeAnswer { asked++; return probeUnknown }
	r.healthyPeer(t)
	k := raft.StreamClaim{Instance: clInst, Origin: clOrigin}
	w := r.cl.workFor(k)
	t0 := time.Now()
	at := func(d time.Duration) bool {
		now := t0.Add(d)
		r.cl.now = func() time.Time { return now }
		return r.cl.unpulledSettled(context.Background(), r.c.raftNode.FSM(), k, w)
	}
	at(0)
	if asked != 1 {
		t.Fatalf("asked %d", asked)
	}
	at(unpulledEvery / 2)
	if asked != 1 {
		t.Fatal("asked again within the throttle")
	}
	at(r.cl.grace + time.Second)
	if _, ok := r.c.raftNode.FSM().GetFile(unpulledPath); !ok {
		t.Fatal("removed after one grace")
	}
	at(2*r.cl.grace + time.Second)
	if _, ok := r.c.raftNode.FSM().GetFile(unpulledPath); ok {
		t.Fatal("kept after twice the grace")
	}
}

// Work for a claim that went (its instance removed) is forgotten.
func TestClaimantForgetsEndedClaims(t *testing.T) {
	r := newClaimantRig(t)
	r.cl.work[0xdead] = &claimWork{unpulled: 3}
	r.cl.pass(context.Background())
	if _, ok := r.cl.work[0xdead]; ok {
		t.Fatal("stale work kept")
	}
}
