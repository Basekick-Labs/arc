package raft

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1207 S4b: the stream takeover rules, against a real FSM.

const (
	hourA = "2026-10-12T10:00:00Z"
	hourB = "2026-10-12T11:00:00Z"
)

type tkFSM struct {
	t   *testing.T
	f   *ClusterFSM
	idx uint64
}

// takeoverFSM: writers w1 (primary), w2, w3 and reader r1, all healthy, the
// flag on.
func takeoverFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := &tkFSM{t: t, f: NewClusterFSM(zerolog.Nop())}
	for _, n := range []string{"w1", "w2", "w3", "r1"} {
		role := "writer"
		if n == "r1" {
			role = "reader"
		}
		x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: n, Role: role, State: "healthy"}})
	}
	x.ok(CommandPromoteWriter, PromoteWriterPayload{NodeID: "w1"})
	x.ok(CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: []string{"w1", "w2", "w3", "r1"}, Now: t0})
	return x
}

func (x *tkFSM) apply(typ CommandType, p any) interface{} {
	x.t.Helper()
	x.idx++
	return applyCmd(x.t, x.f, x.idx, typ, p)
}

func (x *tkFSM) ok(typ CommandType, p any) {
	x.t.Helper()
	if res := x.apply(typ, p); res != nil {
		x.t.Fatalf("command %d: %v", typ, res)
	}
}

func (x *tkFSM) refused(typ CommandType, p any) {
	x.t.Helper()
	if res := x.apply(typ, p); res == nil {
		x.t.Fatalf("command %d accepted, want refused", typ)
	}
}

func (x *tkFSM) announce(origin string, inst uint64, cont Continuity, holds ...uint64) {
	x.t.Helper()
	x.ok(CommandAnnounceInstance, AnnounceInstancePayload{Origin: origin, Instance: inst, Continuity: cont, Holds: holds, FreshStart: true})
}

func cov(inst uint64, key string, lo, hi uint64) InstanceCoverage {
	return InstanceCoverage{Instance: inst, Key: key, Ranges: [][2]uint64{{lo, hi}}}
}

func regPayload(path, origin, hour string, c ...InstanceCoverage) RegisterFilePayload {
	pt, _ := time.Parse(time.RFC3339, hour)
	return RegisterFilePayload{File: FileEntry{Path: path, SHA256: path, SizeBytes: 1, Database: "db", Measurement: "cpu",
		PartitionTime: pt, OriginNodeID: origin, CreatedAt: t0}, Cov2: c}
}

// register applies a single registration and returns the refusal kind ("" if
// accepted).
func (x *tkFSM) register(p RegisterFilePayload) string {
	x.t.Helper()
	res := x.apply(CommandRegisterFile, p)
	if res == nil {
		return ""
	}
	ops := StreamClaimConflictOps(res.(error))
	if len(ops) != 1 {
		x.t.Fatalf("register %s: %v", p.File.Path, res)
	}
	return ops[0].Kind
}

func (x *tkFSM) batch(ops ...BatchFileOp) []RefusedOp {
	x.t.Helper()
	res := x.apply(CommandBatchFileOps, BatchFileOpsPayload{Ops: ops})
	if res == nil {
		return nil
	}
	ops2 := StreamClaimConflictOps(res.(error))
	if len(ops2) == 0 {
		x.t.Fatalf("batch: %v", res)
	}
	return ops2
}

func regOp(t *testing.T, p RegisterFilePayload) BatchFileOp {
	return BatchFileOp{Type: CommandRegisterFile, Payload: mustJSON(t, p)}
}

func delOp(t *testing.T, path, reason string) BatchFileOp {
	return BatchFileOp{Type: CommandDeleteFile, Payload: mustJSON(t, DeleteFilePayload{Path: path, Reason: reason})}
}

func (x *tkFSM) claim(by, origin string, inst uint64, key string, lo, hi uint64) uint64 {
	x.t.Helper()
	x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: origin, By: by, Now: t0,
		Claims: []ClaimSet{{Instance: inst, Keys: map[string]Intervals{key: {{lo, hi}}}}}})
	return x.idx
}

func (x *tkFSM) inst(id uint64) *StreamInstance {
	for _, in := range x.f.StreamInstances() {
		if in.Instance == id {
			return &in
		}
	}
	return nil
}

// ---- registration rules ----

func TestDuplicateIsRefusedPerBatchAndHour(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	if k := x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 1, 5))); k != "" {
		t.Fatalf("first copy refused: %s", k)
	}
	// Same batches, same hour: a duplicate (a crash replay of a file whose
	// checkpoint was lost).
	if k := x.register(regPayload("b", "w1", hourA, cov(7, "db/cpu", 5, 6))); k != RefusedDuplicate {
		t.Fatalf("overlap in the same hour: %q", k)
	}
	// Same batch, another hour: its rows of that hour are not registered.
	if k := x.register(regPayload("c", "w1", hourB, cov(7, "db/cpu", 5, 6))); k != "" {
		t.Fatalf("another hour refused: %s", k)
	}
	// Another key, another instance: unrelated.
	if k := x.register(regPayload("d", "w1", hourA, cov(7, "db/mem", 1, 5))); k != "" {
		t.Fatalf("another key refused: %s", k)
	}
	// A retry of a committed registration is the same file.
	if k := x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 1, 5))); k != "" {
		t.Fatalf("idempotent retry refused: %s", k)
	}
}

func TestDuplicateOfPartialPieces(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	rows := func(path string, lo, hi int) RegisterFilePayload {
		return regPayload(path, "w1", hourA, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: 3, RowLo: lo, RowHi: hi, RowTotal: 100}}})
	}
	if k := x.register(rows("a", 0, 50)); k != "" {
		t.Fatal(k)
	}
	if k := x.register(rows("b", 50, 100)); k != "" {
		t.Fatalf("the complementary row range refused: %s", k)
	}
	if k := x.register(rows("c", 40, 60)); k != RefusedDuplicate {
		t.Fatalf("an overlapping row range: %q", k)
	}
	if k := x.register(regPayload("d", "w1", hourA, cov(7, "db/cpu", 3, 3))); k != RefusedDuplicate {
		t.Fatalf("the whole batch after its rows: %q", k)
	}
	chunk := func(path string, c uint16) RegisterFilePayload {
		return regPayload(path, "w1", hourA, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: 9, Chunk: c, Chunks: 2}}})
	}
	if x.register(chunk("e", 0)) != "" || x.register(chunk("f", 1)) != "" {
		t.Fatal("distinct chunks of one batch refused")
	}
	if k := x.register(chunk("g", 1)); k != RefusedDuplicate {
		t.Fatalf("a chunk twice: %q", k)
	}
}

// Two copies of one batch in one Raft entry: the second is refused (the
// pre-pass sees the first).
func TestDuplicateWithinOneBatch(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	ref := x.batch(regOp(t, regPayload("a", "w1", hourA, cov(7, "db/cpu", 1, 2))), regOp(t, regPayload("b", "w1", hourA, cov(7, "db/cpu", 2, 3))))
	if len(ref) != 1 || ref[0].Op != 1 || ref[0].Kind != RefusedDuplicate {
		t.Fatalf("refused %+v", ref)
	}
	// A refused op adds nothing: the next op of the same batch is judged
	// against what was accepted.
	ref = x.batch(regOp(t, regPayload("c", "w1", hourB, cov(7, "db/cpu", 1, 1))), regOp(t, regPayload("d", "w1", hourA, cov(7, "db/cpu", 1, 1))), regOp(t, regPayload("e", "w1", hourA, cov(7, "db/cpu", 9, 9))))
	if len(ref) != 0 {
		t.Fatalf("refused %+v", ref)
	}
}

// A rewrite of a pending file is sent as Register(new) then Delete(old): if
// the old one already committed, the rewrite moves its batches.
func TestRewriteSupersedingItsCommittedOriginal(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	if k := x.register(regPayload("old", "w1", hourA, cov(7, "db/cpu", 1, 4))); k != "" {
		t.Fatal(k)
	}
	rw := regPayload("new", "w1", hourA, cov(7, "db/cpu", 1, 4))
	rw.Supersedes = "old"
	if ref := x.batch(regOp(t, rw), delOp(t, "old", "delete-rewrite")); len(ref) != 0 {
		t.Fatalf("rewrite refused: %+v", ref)
	}
	if _, ok := x.f.GetFile("new"); !ok {
		t.Fatal("rewrite not registered")
	}
	// Without the delete in the same batch it is a duplicate.
	rw2 := regPayload("new2", "w1", hourA, cov(7, "db/cpu", 1, 4))
	rw2.Supersedes = "new"
	if ref := x.batch(regOp(t, rw2)); len(ref) != 1 || ref[0].Kind != RefusedDuplicate {
		t.Fatalf("refused %+v", ref)
	}
	// An unrelated delete in the batch subtracts nothing.
	rw3 := regPayload("new3", "w1", hourA, cov(7, "db/cpu", 1, 4))
	rw3.Supersedes = "other"
	if ref := x.batch(regOp(t, rw3), delOp(t, "new", "compaction")); len(ref) != 1 || ref[0].Kind != RefusedDuplicate {
		t.Fatalf("refused %+v", ref)
	}
}

// A DELETE rewrite of a registered file inherits its coverage and changes
// nothing in the index; compaction outputs carry none and the inputs stay
// settled.
func TestDeleteRewriteAndCompactionKeepTheIndex(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	if k := x.register(regPayload("f", "w1", hourA, cov(7, "db/cpu", 1, 4))); k != "" {
		t.Fatal(k)
	}
	rw := regPayload("f2", "w1", hourA)
	rw.InheritFrom = "f"
	rw.File.Coverage = []InstanceCoverage{cov(99, "spoofed", 1, 1)} // ignored
	if ref := x.batch(regOp(t, rw), delOp(t, "f", "delete-rewrite")); len(ref) != 0 {
		t.Fatalf("rewrite refused: %+v", ref)
	}
	e, _ := x.f.GetFile("f2")
	if len(e.Coverage) != 1 || e.Coverage[0].Instance != 7 {
		t.Fatalf("rewrite coverage %+v", e.Coverage)
	}
	if ref := x.batch(regOp(t, regPayload("compacted", "w1", hourA)), delOp(t, "f2", "compaction")); len(ref) != 0 {
		t.Fatalf("compaction refused: %+v", ref)
	}
	if !x.f.CoverageSettled(7, "db/cpu", CoveragePiece{CSeq: 2}, mustTime(hourA)) {
		t.Fatal("compaction unsettled the batches")
	}
	if k := x.register(regPayload("dup", "w1", hourA, cov(7, "db/cpu", 2, 2))); k != RefusedDuplicate {
		t.Fatalf("a replayed copy after compaction: %q", k)
	}
}

// A file that vanished before any node could pull it holds nothing: its
// batches are unsettled again. Other deletes keep them.
func TestVanishedFileUnsettles(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("v", "w1", hourA, cov(7, "db/cpu", 1, 2)))
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "v", Reason: DeleteReasonVanished})
	if x.f.CoverageSettled(7, "db/cpu", CoveragePiece{CSeq: 1}, mustTime(hourA)) {
		t.Fatal("still settled after the file vanished")
	}
	if k := x.register(regPayload("v2", "w1", hourA, cov(7, "db/cpu", 1, 2))); k != "" {
		t.Fatalf("re-registration refused: %s", k)
	}
	// A second vanished delete of an absent path changes nothing.
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: "v", Reason: DeleteReasonVanished})
	if !x.f.CoverageSettled(7, "db/cpu", CoveragePiece{CSeq: 1}, mustTime(hourA)) {
		t.Fatal("a delete of an absent file unsettled another file's batches")
	}
}

// Unknown and untracked instances are never refused as duplicates (they are
// not indexed), but claims still apply.
func TestUnknownInstancesAreNotIndexed(t *testing.T) {
	x := takeoverFSM(t)
	for _, p := range []string{"a", "b"} {
		if k := x.register(regPayload(p, "w1", hourA, cov(42, "db/cpu", 1, 1))); k != "" {
			t.Fatalf("unknown instance refused: %s", k)
		}
	}
	if x.inst(42) != nil {
		t.Fatal("an unknown instance was created by a registration")
	}
	// Announced without a fresh start: untracked.
	x.ok(CommandAnnounceInstance, AnnounceInstancePayload{Origin: "w2", Instance: 50, FreshStart: false})
	if in := x.inst(50); in == nil || !in.Untracked {
		t.Fatalf("instance %+v", in)
	}
	for _, p := range []string{"c", "d"} {
		if k := x.register(regPayload(p, "w2", hourA, cov(50, "db/cpu", 1, 1))); k != "" {
			t.Fatalf("untracked instance refused: %s", k)
		}
	}
}

func TestTruncatedCoverageUntracksTheInstance(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 1, 1)))
	if k := x.register(regPayload("b", "w1", hourA, InstanceCoverage{Instance: 7, Key: "db/cpu", Truncated: true})); k != "" {
		t.Fatalf("truncated refused: %s", k)
	}
	if in := x.inst(7); !in.Untracked {
		t.Fatal("not untracked")
	}
	if x.f.takeover.entries != 0 {
		t.Fatalf("index not freed: %d entries", x.f.takeover.entries)
	}
}

// ---- claims ----

func TestClaimRulesForTheOriginAndTheClaimant(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.register(regPayload("early", "w2", hourA, cov(7, "db/cpu", 1, 3)))
	// w1 is the designated primary: it takes over w2's current instance.
	epoch := x.claim("w1", "w2", 7, "db/cpu", 1, 10)
	c := x.f.StreamClaims()[0]
	if c.Epoch != epoch || c.By != "w1" {
		t.Fatalf("claim %+v", c)
	}
	// The origin's copy of claimed batches: refused, whatever else.
	if k := x.register(regPayload("o1", "w2", hourB, cov(7, "db/cpu", 9, 12))); k != RefusedClaimed {
		t.Fatalf("origin overlapping the claim: %q", k)
	}
	if k := x.register(regPayload("o2", "w2", hourB, InstanceCoverage{Instance: 7, Key: "db/cpu", Truncated: true})); k != RefusedClaimed {
		t.Fatalf("truncated origin coverage of a claimed instance: %q", k)
	}
	// Above the claim: the origin's.
	if k := x.register(regPayload("o3", "w2", hourB, cov(7, "db/cpu", 11, 12))); k != "" {
		t.Fatalf("origin above the claim: %s", k)
	}
	// The claimant's replay: accepted inside its set, stale outside it or
	// under another epoch, duplicate where the hour is registered.
	rp := func(path, hour string, lo, hi, e uint64) RegisterFilePayload {
		c := cov(7, "db/cpu", lo, hi)
		c.ClaimEpoch = e
		return regPayload(path, "w1", hour, c)
	}
	if k := x.register(rp("r1", hourB, 4, 6, epoch)); k != "" {
		t.Fatalf("claimant replay: %s", k)
	}
	if k := x.register(rp("r2", hourB, 9, 11, epoch)); k != RefusedStale {
		t.Fatalf("claimant outside its set: %q", k)
	}
	if k := x.register(rp("r3", hourB, 7, 7, epoch+1)); k != RefusedClaimed {
		t.Fatalf("claimed batches under another epoch: %q", k)
	}
	if k := x.register(rp("r3b", hourB, 13, 13, epoch+1)); k != RefusedStale {
		t.Fatalf("a claimant file under an epoch that is not the claim: %q", k)
	}
	if k := x.register(rp("r4", hourA, 2, 2, epoch)); k != RefusedDuplicate {
		t.Fatalf("claimant replaying a registered hour: %q", k)
	}
}

func TestTakeOverRules(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	claim := func(by, origin string, inst uint64) TakeOverStreamV2Payload {
		return TakeOverStreamV2Payload{Origin: origin, By: by, Now: t0, Claims: []ClaimSet{{Instance: inst, Keys: map[string]Intervals{"db/cpu": {{1, 5}}}}}}
	}
	x.refused(CommandTakeOverStreamV2, claim("w3", "w2", 7)) // current instance: only the primary
	x.refused(CommandTakeOverStreamV2, claim("w2", "w2", 7)) // own instance
	x.refused(CommandTakeOverStreamV2, claim("r1", "w2", 7)) // not a writer
	x.refused(CommandTakeOverStreamV2, claim("w1", "w3", 7)) // wrong origin
	x.refused(CommandTakeOverStreamV2, claim("w1", "w2", 8)) // unknown
	x.ok(CommandTakeOverStreamV2, claim("w1", "w2", 7))
	x.refused(CommandTakeOverStreamV2, claim("w1", "w2", 7))        // already claimed
	x.refused(CommandTakeOverStream, map[string]any{"instance": 7}) // the S4a command
	// An unhealthy claimant is refused.
	x.announce("w3", 20, Continuity{})
	x.announce("w3", 21, Continuity{}) // new disks: 20 orphaned
	x.ok(CommandUpdateNodeState, UpdateNodeStatePayload{NodeID: "w2", NewState: "unhealthy"})
	x.refused(CommandTakeOverStreamV2, claim("w2", "w3", 20))
	x.ok(CommandTakeOverStreamV2, claim("w1", "w3", 20))
}

// Before the flag, every takeover command is refused and registrations are
// not checked.
func TestTakeoverIsInertWithoutTheFlag(t *testing.T) {
	x := &tkFSM{t: t, f: NewClusterFSM(zerolog.Nop())}
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w1", Role: "writer", State: "healthy"}})
	x.refused(CommandAnnounceInstance, AnnounceInstancePayload{Origin: "w1", Instance: 7, FreshStart: true})
	x.refused(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 7})
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0, Claims: []ClaimSet{{Instance: 7}}})
	x.refused(CommandInstanceSettled, InstanceSettledPayload{Origin: "w1", Instance: 7})
	for _, p := range []string{"a", "b"} {
		if k := x.register(regPayload(p, "w1", hourA, cov(7, "db/cpu", 1, 1))); k != "" {
			t.Fatal(k)
		}
	}
	if e, _ := x.f.GetFile("a"); len(e.Coverage) != 0 {
		t.Fatal("coverage stored without the flag")
	}
}

// A claim never takes batches settled in every hour (at or below the floor
// and not lost); lost ones stay claimable.
func TestClaimExcludesWholeSettledBatches(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w2", Instance: 7, Keys: map[string]KeySettlement{"db/cpu": {Floor: 10, Lost: Intervals{{4, 5}}}}})
	x.claim("w1", "w2", 7, "db/cpu", 1, 20)
	got := x.f.StreamClaims()[0].Set["db/cpu"]
	if len(got) != 2 || got[0] != [2]uint64{4, 5} || got[1] != [2]uint64{11, 20} {
		t.Fatalf("claimed %v", got)
	}
}

func TestSettleCoverageCollapsesTheIndex(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 1, 8)))
	x.register(regPayload("b", "w1", hourB, cov(7, "db/cpu", 6, 9)))
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 7, Keys: map[string]KeySettlement{"db/cpu": {Floor: 8, Lost: Intervals{{3, 3}}}}})
	// Left: the lost batch where it registered (hour A), batch 9 (hour B),
	// and the lost interval.
	if h := x.f.takeover.index[7]["db/cpu"].Hours; x.f.takeover.entries != 3 || len(h) != 2 {
		t.Fatalf("index after the floor: %d entries, hours %v", x.f.takeover.entries, h)
	}
	for _, c := range []struct {
		cseq uint64
		hour string
		want bool
	}{{1, hourB, true}, {3, hourA, true}, {3, hourB, false}, {9, hourB, true}, {9, hourA, false}} {
		if got := x.f.CoverageSettled(7, "db/cpu", CoveragePiece{CSeq: c.cseq}, mustTime(c.hour)); got != c.want {
			t.Errorf("settled(%d, %s) = %v, want %v", c.cseq, c.hour, got, c.want)
		}
	}
	// Only the origin's current instance, and floors only rise.
	x.refused(CommandSettleCoverage, SettleCoveragePayload{Origin: "w2", Instance: 7, Keys: map[string]KeySettlement{"db/cpu": {Floor: 20}}})
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 7, Keys: map[string]KeySettlement{"db/cpu": {Floor: 2}}})
	if x.f.takeover.index[7]["db/cpu"].Floor != 8 {
		t.Fatal("floor lowered")
	}
}

// ---- lifecycle ----

func TestAnnounceClassifiesPreviousInstances(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 1, Continuity{})
	x.announce("w2", 2, Continuity{WAL: 1, Journal: 1, Storage: 1}, 1) // holds 1
	if in := x.inst(1); in.Status != InstanceAdopted || in.AdoptedBy != 2 {
		t.Fatalf("held previous: %+v", in)
	}
	x.announce("w2", 3, Continuity{WAL: 2, Journal: 2, Storage: 2}) // same disks, holds nothing
	if x.inst(1).Status != InstanceClosed || x.inst(2).Status != InstanceClosed {
		t.Fatalf("not closed: %+v %+v", x.inst(1), x.inst(2))
	}
	x.announce("w2", 4, Continuity{WAL: 3, Journal: 9, Storage: 3}) // one volume differs
	if x.inst(3).Status != InstanceOrphaned {
		t.Fatalf("3: %+v", x.inst(3))
	}
	// A retry of a committed announcement succeeds; another origin's
	// instance id is refused.
	x.announce("w2", 4, Continuity{})
	x.refused(CommandAnnounceInstance, AnnounceInstancePayload{Origin: "w3", Instance: 4, FreshStart: true})
	// An orphaned instance may be claimed by any healthy writer other than
	// its origin, not only the primary.
	x.claim("w3", "w2", 3, "db/cpu", 1, 5)
}

func TestClaimedInstanceStaysClaimedWhenItsOriginReturns(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.announce("w2", 8, Continuity{WAL: 7, Journal: 7, Storage: 7}, 7)
	// Adopted by the new process (an origin has one current instance), and
	// still claimed.
	if in := x.inst(7); in.Status != InstanceAdopted || x.f.takeover.current["w2"] != 8 || !x.f.takeover.claims[7].active() {
		t.Fatalf("instance %+v, current %d", in, x.f.takeover.current["w2"])
	}
	if k := x.register(regPayload("o", "w2", hourA, cov(7, "db/cpu", 3, 3))); k != RefusedClaimed {
		t.Fatalf("returning origin's copy: %q", k)
	}
	x.refused(CommandInstanceSettled, InstanceSettledPayload{Origin: "w2", Instance: 7})
}

func TestClaimDoneDropsUnreplayableAndCollectsClosedInstances(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{}) // new disks: 7 orphaned
	epoch := x.claim("w3", "w2", 7, "db/cpu", 1, 10)
	x.refused(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: epoch})
	x.refused(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w3", Epoch: epoch + 1})
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w3", Epoch: epoch, Unreplayable: map[string]Intervals{"db/cpu": {{4, 4}}}})
	// Orphaned and done: nothing will ever register it again.
	if x.inst(7) != nil || x.f.StreamClaimCount() != 0 {
		t.Fatalf("orphaned instance kept after its claim finished: %+v", x.inst(7))
	}

	// A current (dead) instance keeps its claim, minus the unreplayable
	// batch, which its origin may still register.
	y := takeoverFSM(t)
	y.announce("w2", 7, Continuity{})
	e := y.claim("w1", "w2", 7, "db/cpu", 1, 10)
	y.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e, Unreplayable: map[string]Intervals{"db/cpu": {{4, 4}}}})
	if k := y.register(regPayload("o", "w2", hourA, cov(7, "db/cpu", 4, 4))); k != "" {
		t.Fatalf("an unreplayable batch is still claimed: %s", k)
	}
	if k := y.register(regPayload("o2", "w2", hourA, cov(7, "db/cpu", 5, 5))); k != RefusedClaimed {
		t.Fatalf("a replayed batch: %q", k)
	}
}

func TestReleaseAndReclaim(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e1 := x.claim("w1", "w2", 7, "db/cpu", 1, 10)
	x.refused(CommandReleaseStreamClaim, ReleaseStreamClaimPayload{Instance: 7, Epoch: e1 + 1})
	x.ok(CommandReleaseStreamClaim, ReleaseStreamClaimPayload{Instance: 7, Epoch: e1})
	// Released: the origin's copy is no longer refused; the first
	// claimant's late files are stale.
	if k := x.register(regPayload("o", "w2", hourA, cov(7, "db/cpu", 3, 3))); k != "" {
		t.Fatalf("after release: %s", k)
	}
	late := cov(7, "db/cpu", 5, 5)
	late.ClaimEpoch = e1
	if k := x.register(regPayload("late", "w1", hourA, late)); k != RefusedStale {
		t.Fatalf("released claimant: %q", k)
	}
	// Re-claimed by the primary again (a new epoch).
	e2 := x.claim("w1", "w2", 7, "db/cpu", 1, 10)
	if e2 == e1 {
		t.Fatal("same epoch")
	}
}

func TestRemoveNodeReleasesAndOrphans(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{}) // 7 orphaned
	x.claim("w3", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3"}) // the claimant
	if c := x.f.StreamClaims()[0]; !c.Released {
		t.Fatal("a removed claimant's claim was not released")
	}
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2"}) // the origin
	if in := x.inst(8); in == nil || in.Status != InstanceOrphaned || !in.OriginGone {
		t.Fatalf("removed origin's current instance: %+v", in)
	}
	if x.inst(7) == nil {
		t.Fatal("a claimed instance was dropped with its origin")
	}
}

func TestReportedEmptyByEveryWriterRemovesAnOrphan(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{}) // 7 orphaned
	empty := func(by string) TakeOverStreamV2Payload {
		return TakeOverStreamV2Payload{Origin: "w2", By: by, Now: t0, Claims: []ClaimSet{{Instance: 7}}}
	}
	x.ok(CommandTakeOverStreamV2, empty("w1"))
	if x.inst(7) == nil {
		t.Fatal("removed before every writer reported")
	}
	x.ok(CommandTakeOverStreamV2, empty("w3"))
	if x.inst(7) != nil {
		t.Fatal("kept after every other writer reported nothing to claim")
	}
}

func TestInstanceSettled(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{WAL: 7, Journal: 7, Storage: 7}, 7)                   // 7 adopted
	x.refused(CommandInstanceSettled, InstanceSettledPayload{Origin: "w2", Instance: 8}) // current
	x.refused(CommandInstanceSettled, InstanceSettledPayload{Origin: "w3", Instance: 7}) // not its origin
	x.ok(CommandInstanceSettled, InstanceSettledPayload{Origin: "w2", Instance: 7})
	if x.inst(7) != nil {
		t.Fatal("settled instance kept")
	}
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{1, 1}}}}}})
}

// ---- snapshot and determinism ----

func TestTakeoverSurvivesSnapshot(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.register(regPayload("a", "w2", hourA, cov(7, "db/cpu", 1, 4)))
	x.claim("w1", "w2", 7, "db/cpu", 5, 9)
	g := roundTrip(t, x.f)
	if !g.StreamClaimsEnabled() || len(g.StreamInstances()) != 1 || g.StreamClaimCount() != 1 {
		t.Fatalf("restored: %+v %+v", g.StreamInstances(), g.StreamClaims())
	}
	if !g.CoverageSettled(7, "db/cpu", CoveragePiece{CSeq: 2}, mustTime(hourA)) {
		t.Fatal("index lost")
	}
	if g.takeover.entries != x.f.takeover.entries {
		t.Fatalf("entries %d after restore, %d before", g.takeover.entries, x.f.takeover.entries)
	}
	if e, _ := g.GetFile("a"); len(e.Coverage) != 1 {
		t.Fatal("file coverage lost")
	}
	// An older snapshot: nothing.
	h := roundTrip(t, NewClusterFSM(zerolog.Nop()))
	if h.StreamClaimsEnabled() || len(h.StreamInstances()) != 0 {
		t.Fatal("empty snapshot restored takeover state")
	}
}

// One log applied to three FSMs leaves byte-identical snapshots.
func TestTakeoverIsDeterministic(t *testing.T) {
	var snaps [][]byte
	for i := 0; i < 3; i++ {
		x := takeoverFSM(t)
		x.announce("w2", 7, Continuity{})
		x.announce("w3", 9, Continuity{})
		x.register(regPayload("a", "w2", hourA, cov(7, "db/cpu", 1, 4), cov(9, "db/cpu", 1, 2)))
		x.batch(regOp(t, regPayload("b", "w2", hourB, cov(7, "db/mem", 1, 4))), regOp(t, regPayload("c", "w2", hourB, cov(7, "db/mem", 4, 6))))
		x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w2", Instance: 7, Keys: map[string]KeySettlement{"db/cpu": {Floor: 2}, "db/mem": {Floor: 1}}})
		x.claim("w1", "w2", 7, "db/cpu", 1, 20)
		snaps = append(snaps, persist(t, x.f))
	}
	for i := 1; i < len(snaps); i++ {
		if !bytes.Equal(snaps[0], snaps[i]) {
			t.Fatal("snapshots differ")
		}
	}
}

func persist(t *testing.T, f *ClusterFSM) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func roundTrip(t *testing.T, f *ClusterFSM) *ClusterFSM {
	t.Helper()
	g := NewClusterFSM(zerolog.Nop())
	if err := g.Restore(io.NopCloser(bytes.NewReader(persist(t, f)))); err != nil {
		t.Fatal(err)
	}
	return g
}

func mustTime(s string) time.Time {
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return v
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Over a per-key bound the instance is untracked, never refused.
func TestIndexBoundUntracksInsteadOfRefusing(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	c := InstanceCoverage{Instance: 7, Key: "db/cpu"}
	for i := uint64(0); i <= maxKeyIntervals; i++ {
		c.Ranges = append(c.Ranges, [2]uint64{2*i + 1, 2*i + 1})
	}
	if k := x.register(regPayload("big", "w2", hourA, c)); k != "" {
		t.Fatalf("refused on the registration path: %s", k)
	}
	if !x.inst(7).Untracked || x.f.takeover.entries != 0 {
		t.Fatalf("not untracked: %+v, %d entries", x.inst(7), x.f.takeover.entries)
	}
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{1, 1}}}}}})
}

// ---- guards the mutation pass found untested ----

// A refused op adds nothing to the batch scratch: a later op overlapping it
// is judged against what was accepted.
func TestRefusedOpAddsNothingToTheBatch(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	ref := x.batch(regOp(t, regPayload("a", "w2", hourA, cov(7, "db/cpu", 5, 6))), regOp(t, regPayload("b", "w2", hourA, cov(7, "db/cpu", 6, 6))))
	if len(ref) != 1 || ref[0].Op != 0 || ref[0].Kind != RefusedClaimed {
		t.Fatalf("refused %+v, want only op 0 (claimed)", ref)
	}
}

// A lost batch below the floor is not settled in an hour it did not register:
// a whole range over it registers.
func TestLostBatchBelowTheFloorRegistersElsewhere(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w1", Instance: 7, Keys: map[string]KeySettlement{"db/cpu": {Floor: 10, Lost: Intervals{{4, 6}}}}})
	if k := x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 4, 6))); k != "" {
		t.Fatalf("lost batches refused: %s", k)
	}
	if k := x.register(regPayload("b", "w1", hourA, cov(7, "db/cpu", 6, 7))); k != RefusedDuplicate {
		t.Fatalf("a settled batch below the floor: %q", k)
	}
}

// Coverage is the FSM's: an update keeps it, a registration without Cov2
// stores none whatever the client put in the entry.
func TestFileCoverageIsTheFSMs(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 1, 2)))
	upd := regPayload("a", "w1", hourA).File
	upd.Tier = "cold"
	x.ok(CommandUpdateFile, UpdateFilePayload{File: upd})
	if e, _ := x.f.GetFile("a"); len(e.Coverage) != 1 || e.Tier != "cold" {
		t.Fatalf("after update: %+v", e)
	}
	spoof := regPayload("b", "w1", hourA)
	spoof.File.Coverage = []InstanceCoverage{cov(7, "db/cpu", 9, 9)}
	x.register(spoof)
	if e, _ := x.f.GetFile("b"); len(e.Coverage) != 0 {
		t.Fatalf("client coverage stored: %+v", e.Coverage)
	}
}

func TestNoWriterClaimsItsOwnInstance(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.announce("w1", 8, Continuity{}) // 7 orphaned; w1 is the primary
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w1", By: "w1", Now: t0,
		Claims: []ClaimSet{{Instance: 7, Keys: map[string]Intervals{"db/cpu": {{1, 1}}}}}})
}

func TestAFinishedClaimCannotBeReleased(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	e := x.claim("w1", "w2", 7, "db/cpu", 1, 5)
	x.ok(CommandClaimDone, ClaimDonePayload{Instance: 7, By: "w1", Epoch: e})
	x.refused(CommandReleaseStreamClaim, ReleaseStreamClaimPayload{Instance: 7, Epoch: e})
}

func TestInstanceSettledRefusals(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{WAL: 7, Journal: 7, Storage: 7}) // 7 closed
	x.refused(CommandInstanceSettled, InstanceSettledPayload{Origin: "w2", Instance: 7})
	// Adopted, then claimed together with its (dead) adopter: unfinished.
	x.announce("w3", 20, Continuity{})
	x.announce("w3", 21, Continuity{WAL: 20, Journal: 20, Storage: 20}, 20) // 20 adopted by 21
	x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w3", By: "w1", Now: t0, Claims: []ClaimSet{
		{Instance: 20, Keys: map[string]Intervals{"db/cpu": {{1, 1}}}},
		{Instance: 21, Keys: map[string]Intervals{"db/cpu": {{1, 1}}}},
	}})
	x.refused(CommandInstanceSettled, InstanceSettledPayload{Origin: "w3", Instance: 20})
}

// Registrations are not checked while the flag is off, whatever state exists.
func TestRegistrationRulesNeedTheFlag(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 1, 1)))
	x.f.mu.Lock()
	x.f.streamClaims = false
	x.f.mu.Unlock()
	if k := x.register(regPayload("b", "w1", hourA, cov(7, "db/cpu", 1, 1))); k != "" {
		t.Fatalf("checked with the flag off: %s", k)
	}
}

// ---- deep review regressions ----

// An instance listed twice in one claim is refused; it panicked in Apply on
// every node.
func TestTakeOverListingAnInstanceTwice(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{})
	x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0, Claims: []ClaimSet{{Instance: 7}}})
	x.refused(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w3", Now: t0, Claims: []ClaimSet{{Instance: 7}, {Instance: 7}}})
}

// A node restored from a snapshot decides like one that replayed the log,
// and keeps doing so as the log goes on.
func TestRestoredFSMAgreesWithReplay(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 900, Continuity{})
	x.claim("w1", "w2", 900, "db/cpu", 1, 5)
	x.announce("w2", 100, Continuity{WAL: 900, Journal: 900, Storage: 900}, 900)
	g := roundTrip(t, x.f)
	if g.takeover.current["w2"] != 100 || x.f.takeover.current["w2"] != 100 {
		t.Fatalf("current: replayed %d, restored %d", x.f.takeover.current["w2"], g.takeover.current["w2"])
	}
	y := &tkFSM{t: t, f: g, idx: x.idx}
	for _, z := range []*tkFSM{x, y} {
		z.ok(CommandSettleCoverage, SettleCoveragePayload{Origin: "w2", Instance: 100, Keys: map[string]KeySettlement{"db/cpu": {Floor: 3}}})
		z.register(regPayload("a", "w2", hourA, cov(100, "db/cpu", 5, 6)))
	}
	if !bytes.Equal(persist(t, x.f), persist(t, y.f)) {
		t.Fatal("restored and replayed FSMs diverged")
	}
}

// A coverage over the bounds is treated as truncated before any rule reads
// it: the cost of Apply does not depend on what one writer sent.
func TestOversizedCoverageIsTruncated(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	c := InstanceCoverage{Instance: 7, Key: "db/cpu"}
	for i := uint64(0); i <= maxCoverageEntries; i++ {
		c.Ranges = append(c.Ranges, [2]uint64{2*i + 1, 2*i + 1})
	}
	if k := x.register(regPayload("big", "w1", hourA, c)); k != "" {
		t.Fatalf("refused: %s", k)
	}
	if !x.inst(7).Untracked {
		t.Fatal("oversized coverage did not untrack its instance")
	}
	p := RegisterFilePayload{Cov2: make([]InstanceCoverage, maxCoverageInstances+1)}
	for i := range p.Cov2 {
		p.Cov2[i] = cov(uint64(i+1), "db/cpu", 1, 1)
	}
	sanitizeCoverage(&p)
	if !p.Cov2[0].Truncated || len(p.Cov2[0].Ranges) != 0 {
		t.Fatal("too many instances not truncated")
	}
	many := RegisterFilePayload{Cov2: []InstanceCoverage{c}}
	sanitizeCoverage(&many)
	if !many.Cov2[0].Truncated || len(many.Cov2[0].Ranges) != 0 {
		t.Fatal("too many entries not truncated")
	}
	long := RegisterFilePayload{Cov2: []InstanceCoverage{{Instance: 1, Key: string(make([]byte, maxTakeoverKeyLen+1)), Ranges: [][2]uint64{{1, 1}}}}}
	sanitizeCoverage(&long)
	if !long.Cov2[0].Truncated {
		t.Fatal("oversized key not truncated")
	}
}

func TestReRegistrationWithoutCoverageKeepsIt(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("a", "w1", hourA, cov(7, "db/cpu", 1, 2)))
	again := regPayload("a", "w1", hourA)
	again.File.SHA256 = "restored"
	x.register(again)
	if e, _ := x.f.GetFile("a"); len(e.Coverage) != 1 {
		t.Fatalf("coverage lost on re-registration: %+v", e.Coverage)
	}
}

func TestInheritOnlyFromTheSameMeasurementAndHour(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	x.register(regPayload("f", "w1", hourA, cov(7, "db/cpu", 1, 2)))
	other := regPayload("g", "w1", hourB)
	other.InheritFrom = "f"
	x.register(other)
	if e, _ := x.f.GetFile("g"); len(e.Coverage) != 0 {
		t.Fatal("inherited across partition hours")
	}
}

// The superseded file's batch held only in part is not subtracted: other
// rows of it in this hour may be in another file.
func TestSupersededPartialPieceIsNotSubtractedWhole(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w1", 7, Continuity{})
	half := func(path string, lo, hi int) RegisterFilePayload {
		return regPayload(path, "w1", hourA, InstanceCoverage{Instance: 7, Key: "db/cpu", Partial: []CoveragePiece{{CSeq: 3, RowLo: lo, RowHi: hi, RowTotal: 10}}})
	}
	x.register(half("a", 0, 5))
	x.register(half("b", 5, 10))
	rw := regPayload("c", "w1", hourA, cov(7, "db/cpu", 3, 3))
	rw.Supersedes = "a"
	if ref := x.batch(regOp(t, rw), delOp(t, "a", "delete-rewrite")); len(ref) != 1 || ref[0].Kind != RefusedDuplicate {
		t.Fatalf("whole batch over a superseded half: %+v", ref)
	}
}

// A writer removed from the cluster can no longer report: an orphan every
// remaining writer reported empty goes.
func TestRemovingTheLastSilentWriterCompletesTheReport(t *testing.T) {
	x := takeoverFSM(t)
	x.announce("w2", 7, Continuity{})
	x.announce("w2", 8, Continuity{}) // 7 orphaned
	x.ok(CommandTakeOverStreamV2, TakeOverStreamV2Payload{Origin: "w2", By: "w1", Now: t0, Claims: []ClaimSet{{Instance: 7}}})
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w3"})
	if x.inst(7) != nil {
		t.Fatal("kept after the only silent writer was removed")
	}

}
