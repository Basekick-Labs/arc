package raft

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// #1825 slice 4: the replicated re-home record.

const rhPrefix = "db/cpu/"

var rhNodes = []string{"w1", "w2", "r1"}

// rhFSM: three in-scope nodes, compaction commit and re-home on, the source
// X registered at db/cpu/2026/10/13/10/x.parquet.
func rhFSM(t *testing.T) *tkFSM {
	t.Helper()
	x := &tkFSM{t: t, f: NewClusterFSM(zerolog.Nop())}
	for _, n := range rhNodes {
		role := "writer"
		if n == "r1" {
			role = "reader"
		}
		x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: n, Role: role, State: "healthy",
			Capabilities: []string{CapabilityRehome, CapabilityRehomeInScope}}})
	}
	x.ok(CommandPromoteWriter, PromoteWriterPayload{NodeID: "w1"})
	x.ok(CommandEnableCompactionCommit, EnableCompactionCommitPayload{Nodes: rhNodes, Now: t0})
	x.ok(CommandEnableRehome, EnableRehomePayload{Nodes: rhNodes, Now: t0})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("x.parquet", "sx", 100)})
	return x
}

func rhSource() CompactionInput {
	return CompactionInput{Path: ccHour + "x.parquet", SizeBytes: 100, SHA256: "sx"}
}

func rhOutputs(job string) []RehomeOutput {
	return []RehomeOutput{
		{Key: "db/cpu/2026/10/12/09/" + job + "-0.parquet", StagedKey: RehomeStagedKey(job, 0), SizeBytes: 40, SHA256: "o0",
			PartitionTime: time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)},
		{Key: "db/cpu/2026/10/13/10/" + job + "-1.parquet", StagedKey: RehomeStagedKey(job, 1), SizeBytes: 60, SHA256: "o1",
			PartitionTime: time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)},
	}
}

func rhStage(job string) StageRehomePayload {
	return StageRehomePayload{Job: job, Origin: "w1", Database: "db", Measurement: "cpu",
		Source: rhSource(), Outputs: rhOutputs(job), Now: t0}
}

func (x *tkFSM) rhConflict(typ CommandType, p any) string {
	x.t.Helper()
	res := x.apply(typ, p)
	err, _ := res.(error)
	r := RehomeConflictReason(err)
	if r == "" {
		x.t.Fatalf("command %d: %v, want a re-home conflict", typ, res)
	}
	return r
}

func (x *tkFSM) rhAdvance(job, to string, now time.Time) interface{} {
	x.t.Helper()
	return x.apply(CommandAdvanceRehome, AdvanceRehomePayload{Job: job, By: "w1", To: to, Now: now})
}

func (x *tkFSM) rhAckAll(job, state string) {
	x.t.Helper()
	for _, n := range rhNodes {
		x.ok(CommandAckRehome, AckRehomePayload{Job: job, Node: n, State: state})
	}
}

func (x *tkFSM) manifestChanged(typ CommandType, p any) {
	x.t.Helper()
	res := x.apply(typ, p)
	err, _ := res.(error)
	if !IsManifestChanged(err) {
		x.t.Fatalf("command %d: %v, want a manifest-changed refusal", typ, res)
	}
}

func TestRehomeEnableNeedsCompactionCommitAndEveryNode(t *testing.T) {
	x := &tkFSM{t: t, f: NewClusterFSM(zerolog.Nop())}
	for _, n := range rhNodes {
		x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: n, Role: "writer", State: "healthy"}})
	}
	x.refused(CommandEnableRehome, EnableRehomePayload{Nodes: rhNodes, Now: t0})
	x.ok(CommandEnableCompactionCommit, EnableCompactionCommitPayload{Nodes: rhNodes, Now: t0})
	x.refused(CommandEnableRehome, EnableRehomePayload{Nodes: []string{"w1", "w2"}, Now: t0})
	if x.f.RehomeEnabled() {
		t.Fatal("enabled without every node")
	}
	x.ok(CommandEnableRehome, EnableRehomePayload{Nodes: rhNodes, Now: t0})
	x.ok(CommandEnableRehome, EnableRehomePayload{Nodes: nil, Now: t0}) // already on: a no-op
	if !x.f.RehomeEnabled() {
		t.Fatal("not enabled")
	}
}

func TestRehomeCommandsRefusedWithFlagOff(t *testing.T) {
	x := &tkFSM{t: t, f: NewClusterFSM(zerolog.Nop())}
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w1", Role: "writer", State: "healthy",
		Capabilities: []string{CapabilityRehome, CapabilityRehomeInScope}}})
	x.ok(CommandEnableCompactionCommit, EnableCompactionCommitPayload{Nodes: []string{"w1"}, Now: t0})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("x.parquet", "sx", 100)})
	x.refused(CommandStageRehome, rhStage("j1"))
	x.refused(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted, Now: t0})
	x.refused(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "w1", State: RehomeStaged})
	x.refused(CommandClaimRehomeFinisher, ClaimRehomeFinisherPayload{Job: "j1", By: "w1", Now: t0})
	x.refused(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w1", Now: t0})
	if x.f.RehomeOwns(ccHour + "x.parquet") {
		t.Fatal("owned with the flag off")
	}
}

// The slice 4a guarantee: with the flag on, no job stages unless every node
// advertised that its configuration is in scope, and nothing sets that yet.
func TestStageRehomeRefusedWhileANodeIsOutOfScope(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "r2", Role: "reader", State: "healthy",
		Capabilities: []string{CapabilityRehome}}})
	if r := x.rhConflict(CommandStageRehome, rhStage("j1")); r != RehomeOutOfScope {
		t.Fatalf("reason %q", r)
	}
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "r2"})
	x.ok(CommandStageRehome, rhStage("j1"))
}

func TestStageRehomeRefusedWithStreamClaims(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: rhNodes, Now: t0})
	if r := x.rhConflict(CommandStageRehome, rhStage("j1")); r != RehomeOutOfScope {
		t.Fatalf("reason %q", r)
	}
}

func TestStageRehomeRules(t *testing.T) {
	x := rhFSM(t)
	bad := func(mut func(p *StageRehomePayload)) {
		t.Helper()
		p := rhStage("j1")
		mut(&p)
		x.refused(CommandStageRehome, p)
	}
	bad(func(p *StageRehomePayload) { p.Origin = "nobody" })
	bad(func(p *StageRehomePayload) { p.Now = time.Time{} })
	bad(func(p *StageRehomePayload) { p.Outputs = nil })
	bad(func(p *StageRehomePayload) { p.Outputs[0].Key = "db/mem/2026/10/12/09/a.parquet" })
	bad(func(p *StageRehomePayload) { p.Outputs[0].Key = "db/cpu/../mem/a.parquet" })
	bad(func(p *StageRehomePayload) { p.Outputs[1].StagedKey = RehomeStagedKey("j1", 0) })
	bad(func(p *StageRehomePayload) { p.Outputs[1].Key = p.Outputs[0].Key })
	bad(func(p *StageRehomePayload) { p.Outputs[1].Key = p.Source.Path })
	bad(func(p *StageRehomePayload) { p.Outputs[0].SHA256 = "" })
	bad(func(p *StageRehomePayload) { p.Measurement = "mem" })

	changed := rhStage("j1")
	changed.Source.SHA256 = "other"
	if r := x.rhConflict(CommandStageRehome, changed); r != RehomeSourceChanged {
		t.Fatalf("reason %q", r)
	}
	taken := rhStage("j1")
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("j1-1.parquet", "else", 5)})
	if r := x.rhConflict(CommandStageRehome, taken); r != RehomeOutputExists {
		t.Fatalf("reason %q", r)
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: ccHour + "j1-1.parquet"})

	x.ok(CommandStageRehome, rhStage("j1"))
	x.ok(CommandStageRehome, rhStage("j1")) // a retry
	again := rhStage("j1")
	again.Source.SHA256 = "different"
	if r := x.rhConflict(CommandStageRehome, again); r != RehomeJobExists {
		t.Fatalf("reason %q", r)
	}
	// Another job over the same source.
	if r := x.rhConflict(CommandStageRehome, rhStage("j2")); r != RehomeOwned {
		t.Fatalf("reason %q", r)
	}
	j, _ := x.f.RehomeJob("j1")
	if j.FinisherNode != "w1" || !j.ClaimedAt.Equal(t0) || j.State != RehomeStaged {
		t.Fatalf("staged job %+v", j)
	}
	if keys, _ := x.f.RehomeExcludedFor(rhPrefix); len(keys) != 0 || x.f.RehomeOpen(rhPrefix) {
		t.Fatalf("a staged job leaves %v out", keys)
	}
}

func TestStageRehomeBoundByEncodedSize(t *testing.T) {
	x := rhFSM(t)
	p := rhStage("j1")
	p.Outputs = nil
	for i := 0; i < MaxRehomeOutputs; i++ {
		p.Outputs = append(p.Outputs, RehomeOutput{
			Key:       "db/cpu/2026/10/12/09/" + string(bytes.Repeat([]byte("k"), 200)) + "-" + time.Duration(i).String() + ".parquet",
			StagedKey: RehomeStagedKey("j1", i), SizeBytes: 1, SHA256: string(bytes.Repeat([]byte("a"), 64)),
		})
	}
	x.refused(CommandStageRehome, p)
	if _, ok := x.f.RehomeJob("j1"); ok {
		t.Fatal("an oversized stage applied")
	}
}

func TestRehomeAndCompactionRefuseEachOther(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("y.parquet", "sy", 10)})
	// A staged compaction over X: no job stages on it.
	x.ok(CommandStageCompaction, StageCompactionPayload{Job: "c1", Origin: "w2", Output: ccFile("o-c1.parquet", "so", 5),
		Inputs: []CompactionInput{rhSource(), {Path: ccHour + "y.parquet", SizeBytes: 10, SHA256: "sy"}}, Now: t0})
	if r := x.rhConflict(CommandStageRehome, rhStage("j1")); r != RehomeOwned {
		t.Fatalf("reason %q", r)
	}
	x.ok(CommandAbortCompaction, AbortCompactionPayload{Job: "c1", By: "w2"})
	// A job over X: no compaction stages over X, or onto an output key.
	x.ok(CommandStageRehome, rhStage("j1"))
	if r := x.conflict(CommandStageCompaction, StageCompactionPayload{Job: "c2", Origin: "w2", Output: ccFile("o-c2.parquet", "so2", 5),
		Inputs: []CompactionInput{rhSource(), {Path: ccHour + "y.parquet", SizeBytes: 10, SHA256: "sy"}}, Now: t0}); r != CompactionInputStaged {
		t.Fatalf("reason %q", r)
	}
	out := ccFile("j1-1.parquet", "so3", 5)
	if r := x.conflict(CommandStageCompaction, StageCompactionPayload{Job: "c3", Origin: "w2", Output: out,
		Inputs: []CompactionInput{{Path: ccHour + "y.parquet", SizeBytes: 10, SHA256: "sy"}}, Now: t0}); r != CompactionInputStaged {
		t.Fatalf("reason %q", r)
	}
}

func TestRehomeRefusesManifestChangesToOwnedPaths(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandStageRehome, rhStage("j1"))
	src := ccFile("x.parquet", "sx", 100)
	out0 := rhOutputs("j1")[0].Key

	x.manifestChanged(CommandDeleteFile, DeleteFilePayload{Path: src.Path, Reason: "delete"})
	upd := src
	upd.Tier = "cold"
	x.manifestChanged(CommandUpdateFile, UpdateFilePayload{File: upd})
	x.manifestChanged(CommandRegisterFile, RegisterFilePayload{File: src})
	onOut := ccFile("x.parquet", "zz", 1)
	onOut.Path = out0
	x.manifestChanged(CommandRegisterFile, RegisterFilePayload{File: onOut})
	adopt := onOut
	x.manifestChanged(CommandRegisterFile, RegisterFilePayload{File: adopt, Adopt: true})
	// A batch naming an owned path is refused whole, and names it.
	other := ccFile("free.parquet", "sf", 1)
	res := x.apply(CommandBatchFileOps, batch(t,
		op(t, CommandRegisterFile, RegisterFilePayload{File: other}),
		op(t, CommandDeleteFile, DeleteFilePayload{Path: src.Path, Reason: "retention:7d"})))
	me, ok := res.(*ManifestChangedError)
	if !ok || !reflect.DeepEqual(me.Paths, []string{src.Path}) {
		t.Fatalf("batch: %v", res)
	}
	if _, reg := x.has(other.Path); reg {
		t.Fatal("a refused batch applied part of itself")
	}
	if _, reg := x.has(src.Path); !reg {
		t.Fatal("the source left the manifest")
	}
	// A path no job owns is unaffected.
	x.ok(CommandRegisterFile, RegisterFilePayload{File: other})
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: other.Path, Reason: "delete"})
}

func TestRehomeLifecycle(t *testing.T) {
	x := rhFSM(t)
	var changed, ended []RehomeJob
	x.f.SetRehomeCallbacks(func(j RehomeJob) { changed = append(changed, j) }, func(j RehomeJob) { ended = append(ended, j) })
	x.ok(CommandStageRehome, rhStage("j1"))
	outs := rhOutputs("j1")
	src := ccHour + "x.parquet"

	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomePublished, Now: t0}); r != RehomeWrongState {
		t.Fatalf("staged -> published: %q", r)
	}
	g0 := x.f.RehomeGeneration()
	if res := x.rhAdvance("j1", RehomeCommitted, t0); res != nil {
		t.Fatal(res)
	}
	if x.f.RehomeGeneration() == g0 {
		t.Fatal("generation did not move on a state change")
	}
	keys, _ := x.f.RehomeExcludedFor(rhPrefix)
	if !reflect.DeepEqual(keys, []string{outs[0].Key, outs[1].Key}) || !x.f.RehomeOpen(rhPrefix) {
		t.Fatalf("committed leaves out %v", keys)
	}
	if k, _ := x.f.RehomeExcludedFor("db/mem/"); len(k) != 0 {
		t.Fatalf("another measurement leaves out %v", k)
	}
	// Every node's ack of the current state, and only that, counts.
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomePublished, Now: t0}); r != RehomeAcksMissing {
		t.Fatalf("published before acks: %q", r)
	}
	x.ok(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "w1", State: RehomeStaged}) // stale: ignored
	x.ok(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "nobody", State: RehomeCommitted})
	g1 := x.f.RehomeGeneration()
	x.ok(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "w1", State: RehomeCommitted})
	x.ok(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "w2", State: RehomeCommitted})
	if x.f.RehomeGeneration() != g1 {
		t.Fatal("an ack moved the generation")
	}
	j, _ := x.f.RehomeJob("j1")
	if !reflect.DeepEqual(j.MissingAcks(), []string{"r1"}) {
		t.Fatalf("missing %v", j.MissingAcks())
	}
	x.ok(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "r1", State: RehomeCommitted})
	if res := x.rhAdvance("j1", RehomePublished, t0); res != nil {
		t.Fatal(res)
	}
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeLatched, Now: t0}); r != RehomeAcksMissing {
		t.Fatalf("latched before acks: %q", r)
	}
	x.rhAckAll("j1", RehomePublished)
	latchAt := t0.Add(time.Minute)
	if res := x.rhAdvance("j1", RehomeLatched, latchAt); res != nil {
		t.Fatal(res)
	}
	if res := x.rhAdvance("j1", RehomeLatched, latchAt); res != nil {
		t.Fatalf("a retried latch: %v", res)
	}
	if _, reg := x.has(src); reg {
		t.Fatal("the source is still registered after the latch")
	}
	for _, o := range outs {
		e, reg := x.has(o.Key)
		if !reg || e.SHA256 != o.SHA256 || e.SizeBytes != o.SizeBytes || e.Tier != "hot" || e.Database != "db" ||
			e.Measurement != "cpu" || !e.PartitionTime.Equal(o.PartitionTime) || !e.CreatedAt.Equal(latchAt) || e.OriginNodeID != "w1" {
			t.Fatalf("output %s registered as %+v", o.Key, e)
		}
	}
	keys, _ = x.f.RehomeExcludedFor(rhPrefix)
	if !reflect.DeepEqual(keys, []string{src}) {
		t.Fatalf("latched leaves out %v", keys)
	}
	// Still owned: no other change to the outputs or the source's path.
	x.manifestChanged(CommandDeleteFile, DeleteFilePayload{Path: outs[0].Key, Reason: "delete"})
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeReverting, Now: t0}); r != RehomeWrongState {
		t.Fatalf("revert after latch: %q", r)
	}
	if r := x.rhConflict(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w1", Now: t0}); r != RehomeAcksMissing {
		t.Fatalf("end before acks: %q", r)
	}
	x.rhAckAll("j1", RehomeLatched)
	if r := x.rhConflict(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w2", Now: t0}); r != RehomeNotFinisher {
		t.Fatalf("end by another: %q", r)
	}
	x.ok(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w1", Now: t0})
	x.ok(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w1", Now: t0}) // a retry
	x.f.deliverCompactionEvents()
	if _, ok := x.f.RehomeJob("j1"); ok || x.f.RehomeOwns(src) || x.f.RehomeOwns(outs[0].Key) || x.f.RehomeOpen(rhPrefix) {
		t.Fatal("the job outlived its end")
	}
	x.ok(CommandDeleteFile, DeleteFilePayload{Path: outs[0].Key, Reason: "delete"})
	if len(ended) != 1 || ended[0].ID != "j1" || len(changed) == 0 {
		t.Fatalf("callbacks: %d changed, %v ended", len(changed), ended)
	}
}

func TestRehomeLatchRefusedWhenAnOutputKeyIsRegistered(t *testing.T) {
	// Owned keys refuse registrations, so this needs a restore of a
	// manifest that has one: a snapshot taken elsewhere.
	x := rhFSM(t)
	x.ok(CommandStageRehome, rhStage("j1"))
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted, Now: t0})
	x.rhAckAll("j1", RehomeCommitted)
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomePublished, Now: t0})
	x.rhAckAll("j1", RehomePublished)
	x.f.mu.Lock()
	e := ccFile("j1-1.parquet", "other", 1)
	x.f.files[e.Path] = &e
	x.f.mu.Unlock()
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeLatched, Now: t0}); r != RehomeOutputExists {
		t.Fatalf("reason %q", r)
	}
	if _, reg := x.has(ccHour + "x.parquet"); !reg {
		t.Fatal("a refused latch removed the source")
	}
}

func TestRehomeDriverAndLease(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandStageRehome, rhStage("j1"))
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w2", To: RehomeCommitted, Now: t0}); r != RehomeNotFinisher {
		t.Fatalf("advance by another: %q", r)
	}
	x.refused(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted})
	if r := x.rhConflict(CommandClaimRehomeFinisher, ClaimRehomeFinisherPayload{Job: "j1", By: "w2", Now: t0.Add(FinisherLease)}); r != RehomeClaimed {
		t.Fatalf("claim within the lease: %q", r)
	}
	x.ok(CommandClaimRehomeFinisher, ClaimRehomeFinisherPayload{Job: "j1", By: "w1", Now: t0.Add(FinisherLease)}) // renewed
	later := t0.Add(2*FinisherLease + time.Second)
	x.ok(CommandClaimRehomeFinisher, ClaimRehomeFinisherPayload{Job: "j1", By: "w2", Now: later})
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted, Now: later}); r != RehomeNotFinisher {
		t.Fatalf("the old holder advanced: %q", r)
	}
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w2", To: RehomeCommitted, Now: later})
	if r := x.rhConflict(CommandClaimRehomeFinisher, ClaimRehomeFinisherPayload{Job: "nope", By: "w2", Now: later}); r != RehomeUnknownJob {
		t.Fatalf("unknown job: %q", r)
	}
}

func TestRehomeRevert(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandStageRehome, rhStage("j1"))
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted, Now: t0})
	outs := rhOutputs("j1")
	want := []string{outs[0].Key, outs[1].Key}
	// No acks needed to revert: reads leave the outputs out throughout.
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeReverting, Now: t0})
	if keys, _ := x.f.RehomeExcludedFor(rhPrefix); !reflect.DeepEqual(keys, want) {
		t.Fatalf("reverting leaves out %v", keys)
	}
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeLatched, Now: t0}); r != RehomeWrongState {
		t.Fatalf("latch while reverting: %q", r)
	}
	x.refused(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w1"})
	revertedAt := t0.Add(time.Minute)
	x.ok(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w1", Now: revertedAt})
	j, _ := x.f.RehomeJob("j1")
	if j.State != RehomeReverted || !j.RevertedAt.Equal(revertedAt) {
		t.Fatalf("job %+v", j)
	}
	if keys, _ := x.f.RehomeExcludedFor(rhPrefix); !reflect.DeepEqual(keys, want) || !x.f.RehomeOwns(outs[0].Key) {
		t.Fatalf("reverted leaves out %v", keys)
	}
	if r := x.rhConflict(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w1", Now: revertedAt.Add(RehomeRevertedGrace - time.Second)}); r != RehomeTooSoon {
		t.Fatalf("end within the grace: %q", r)
	}
	x.ok(CommandEndRehome, EndRehomePayload{Job: "j1", By: "w1", Now: revertedAt.Add(RehomeRevertedGrace)})
	if _, ok := x.f.RehomeJob("j1"); ok {
		t.Fatal("job outlived its end")
	}
	if _, reg := x.has(ccHour + "x.parquet"); !reg {
		t.Fatal("a reverted job removed its source")
	}
	// The source is free for a new job.
	x.ok(CommandStageRehome, rhStage("j2"))
}

func TestRehomePauseRefusesStageAndAdvanceButNotRevert(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandStageRehome, rhStage("j1"))
	x.ok(CommandSetCompactionPause, SetCompactionPausePayload{Paused: true, Generation: 1, RequestedBy: "w1",
		Reason: "restore b1", Now: t0, ExpiresAt: t0.Add(time.Hour)})
	x.ok(CommandRegisterFile, RegisterFilePayload{File: ccFile("y.parquet", "sy", 10)})
	p := rhStage("j2")
	p.Source = CompactionInput{Path: ccHour + "y.parquet", SizeBytes: 10, SHA256: "sy"}
	if r := x.rhConflict(CommandStageRehome, p); r != RehomePaused {
		t.Fatalf("stage while paused: %q", r)
	}
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted, Now: t0}); r != RehomePaused {
		t.Fatalf("advance while paused: %q", r)
	}
	// A job staged before a pause never advances, resumed or not: the
	// restore may have changed what it read.
	x.ok(CommandSetCompactionPause, SetCompactionPausePayload{Paused: false, Generation: 1, RequestedBy: "w1", Now: t0.Add(time.Minute)})
	if r := x.rhConflict(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted, Now: t0.Add(time.Minute)}); r != RehomePaused {
		t.Fatalf("advance after a pause: %q", r)
	}
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeReverting, Now: t0.Add(time.Minute)})
	x.ok(CommandStageRehome, p) // resumed: a new job stages
}

func TestRehomeNodeRemoval(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandStageRehome, rhStage("j1"))
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted, Now: t0})
	x.ok(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "w1", State: RehomeCommitted})
	x.ok(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "w2", State: RehomeCommitted})
	if !x.f.ReferencesNode("r1") {
		t.Fatal("a node the job waits for is not referenced")
	}
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "r1"})
	if x.f.ReferencesNode("r1") {
		t.Fatal("a removed node is still referenced")
	}
	j, _ := x.f.RehomeJob("j1")
	if len(j.MissingAcks()) != 0 || !reflect.DeepEqual(j.Dropped, []string{"r1"}) {
		t.Fatalf("job %+v", j)
	}
	// An acked node's removal is not recorded as dropped.
	x.ok(CommandRemoveNode, RemoveNodePayload{NodeID: "w2"})
	j, _ = x.f.RehomeJob("j1")
	if !reflect.DeepEqual(j.Dropped, []string{"r1"}) || !reflect.DeepEqual(j.AckWanted, []string{"w1"}) {
		t.Fatalf("job %+v", j)
	}
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomePublished, Now: t0})
	j, _ = x.f.RehomeJob("j1")
	if len(j.Dropped) != 0 || !reflect.DeepEqual(j.AckWanted, []string{"w1"}) {
		t.Fatalf("a new state kept the last one's dropped nodes: %+v", j)
	}
}

func TestRehomeSnapshotRoundTrip(t *testing.T) {
	x := rhFSM(t)
	x.ok(CommandStageRehome, rhStage("j1"))
	x.ok(CommandAdvanceRehome, AdvanceRehomePayload{Job: "j1", By: "w1", To: RehomeCommitted, Now: t0})
	x.ok(CommandAckRehome, AckRehomePayload{Job: "j1", Node: "r1", State: RehomeCommitted})
	snap, err := x.f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	f2 := newTestFSM()
	var changed []RehomeJob
	f2.SetRehomeCallbacks(func(j RehomeJob) { changed = append(changed, j) }, nil)
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); err != nil {
		t.Fatal(err)
	}
	f2.deliverCompactionEvents()
	want, _ := x.f.RehomeJob("j1")
	got, ok := f2.RehomeJob("j1")
	if !ok || !reflect.DeepEqual(got, want) || !f2.RehomeEnabled() {
		t.Fatalf("restored %+v, want %+v", got, want)
	}
	if !f2.RehomeOwns(ccHour+"x.parquet") || !f2.RehomeOwns(rhOutputs("j1")[1].Key) {
		t.Fatal("the restored index lost a path")
	}
	if f2.RehomeGeneration() != x.f.RehomeGeneration() {
		t.Fatal("generation not restored")
	}
	if len(changed) != 1 || changed[0].ID != "j1" {
		t.Fatalf("restore announced %v", changed)
	}
	// A snapshot of a cluster that never enabled re-home has no field, and
	// restoring it turns the flag off and drops the job.
	x2 := &tkFSM{t: t, f: NewClusterFSM(zerolog.Nop())}
	snap2, _ := x2.f.Snapshot()
	var buf2 bytes.Buffer
	if err := snap2.Persist(&testSnapshotSink{Writer: &buf2}); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf2.Bytes(), []byte(`"rehome"`)) {
		t.Fatal("a snapshot of a cluster that never enabled re-home carries it")
	}
	var ended []RehomeJob
	f2.SetRehomeCallbacks(nil, func(j RehomeJob) { ended = append(ended, j) })
	if err := f2.Restore(io.NopCloser(bytes.NewReader(buf2.Bytes()))); err != nil {
		t.Fatal(err)
	}
	f2.deliverCompactionEvents()
	if f2.RehomeEnabled() || len(f2.RehomeJobs()) != 0 || f2.RehomeOwns(ccHour+"x.parquet") {
		t.Fatal("restoring an older snapshot kept re-home state")
	}
	if len(ended) != 1 || ended[0].ID != "j1" {
		t.Fatalf("restore announced ended %v", ended)
	}
}

func TestRehomeConflictReasonFromText(t *testing.T) {
	err := &RehomeConflictError{Job: "j1", Reason: RehomeAcksMissing}
	if r := RehomeConflictReason(errorString(err.Error())); r != RehomeAcksMissing {
		t.Fatalf("reason %q", r)
	}
	if RehomeConflictReason(errorString("other")) != "" || RehomeConflictReason(nil) != "" {
		t.Fatal("a reason from nothing")
	}
}

type errorString string

func (e errorString) Error() string { return string(e) }
