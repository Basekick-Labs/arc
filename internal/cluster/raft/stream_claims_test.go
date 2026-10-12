package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/rs/zerolog"
)

func applyCmd(t *testing.T, f *ClusterFSM, idx uint64, typ CommandType, payload any) interface{} {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(Command{Type: typ, Payload: raw})
	if err != nil {
		t.Fatal(err)
	}
	return f.Apply(&raft.Log{Index: idx, Data: data})
}

func claimsFSM(t *testing.T, nodes ...string) *ClusterFSM {
	t.Helper()
	f := NewClusterFSM(zerolog.Nop())
	for i, n := range nodes {
		if res := applyCmd(t, f, uint64(i+1), CommandAddNode, AddNodePayload{Node: NodeInfo{ID: n, Role: "writer"}}); res != nil {
			t.Fatalf("add %s: %v", n, res)
		}
	}
	return f
}

var t0 = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

func TestEnableStreamClaimsNeedsEveryNode(t *testing.T) {
	f := claimsFSM(t, "w1", "w2", "w3")
	if res := applyCmd(t, f, 10, CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: []string{"w1", "w2"}, Now: t0}); res == nil {
		t.Fatal("enabled with w3 unaccounted for")
	}
	if f.StreamClaimsEnabled() {
		t.Fatal("flag on after a refused enable")
	}
	if res := applyCmd(t, f, 11, CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: []string{"w1", "w2", "w3"}, Now: t0}); res != nil {
		t.Fatalf("enable: %v", res)
	}
	if !f.StreamClaimsEnabled() {
		t.Fatal("flag off after enable")
	}
}

func takeOver(by string, epoch uint64, w map[string]uint64) TakeOverStreamPayload {
	return TakeOverStreamPayload{Instance: 0xabc, Origin: "w1", By: by, Epoch: epoch, W: w, Now: t0}
}

func TestTakeOverStreamRules(t *testing.T) {
	f := claimsFSM(t, "w1", "w2", "r1")
	if res := applyCmd(t, f, 10, CommandTakeOverStream, takeOver("w2", 1, map[string]uint64{"db/a": 5})); res == nil {
		t.Fatal("claim accepted before the flag")
	}
	applyCmd(t, f, 11, CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: []string{"w1", "w2", "r1"}, Now: t0})
	for name, p := range map[string]TakeOverStreamPayload{
		"self claim":   takeOver("w1", 1, map[string]uint64{"db/a": 5}),
		"no keys":      takeOver("w2", 1, nil),
		"unknown node": takeOver("w9", 1, map[string]uint64{"db/a": 5}),
	} {
		if res := applyCmd(t, f, 12, CommandTakeOverStream, p); res == nil {
			t.Errorf("%s accepted", name)
		}
	}
	// A reader cannot claim.
	r := takeOver("r1", 1, map[string]uint64{"db/a": 5})
	f.mu.Lock()
	f.nodes["r1"].Role = "reader"
	f.mu.Unlock()
	if res := applyCmd(t, f, 13, CommandTakeOverStream, r); res == nil {
		t.Error("a reader claimed")
	}
	if res := applyCmd(t, f, 14, CommandTakeOverStream, takeOver("w2", 2, map[string]uint64{"db/a": 5})); res != nil {
		t.Fatalf("claim: %v", res)
	}
	if res := applyCmd(t, f, 15, CommandTakeOverStream, takeOver("w2", 2, map[string]uint64{"db/a": 9})); res == nil {
		t.Fatal("same epoch re-claim accepted")
	}
	if res := applyCmd(t, f, 16, CommandTakeOverStream, takeOver("w2", 3, map[string]uint64{"db/a": 9})); res != nil {
		t.Fatalf("later epoch re-claim: %v", res)
	}
	if c := f.StreamClaims(); len(c) != 1 || c[0].Epoch != 3 || c[0].W["db/a"] != 9 {
		t.Fatalf("claims = %+v", c)
	}
	// Expiry by the proposer's clock: a claim on another instance much later
	// drops the old one.
	late := TakeOverStreamPayload{Instance: 0xdef, Origin: "w1", By: "w2", Epoch: 1, W: map[string]uint64{"db/a": 1}, Now: t0.Add(8 * 24 * time.Hour)}
	if res := applyCmd(t, f, 17, CommandTakeOverStream, late); res != nil {
		t.Fatal(res)
	}
	if c := f.StreamClaims(); len(c) != 1 || c[0].Instance != 0xdef {
		t.Fatalf("expired claim kept: %+v", c)
	}
}

func regPayload(path, origin string, cov ...FileCoverage) RegisterFilePayload {
	return RegisterFilePayload{File: FileEntry{Path: path, SHA256: "s-" + path, SizeBytes: 1, Database: "db", Measurement: "a", OriginNodeID: origin, CreatedAt: t0}, Coverage: cov}
}

func claimedFSM(t *testing.T) *ClusterFSM {
	t.Helper()
	f := claimsFSM(t, "w1", "w2")
	applyCmd(t, f, 10, CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: []string{"w1", "w2"}, Now: t0})
	if res := applyCmd(t, f, 11, CommandTakeOverStream, takeOver("w2", 4, map[string]uint64{"db/a": 100})); res != nil {
		t.Fatal(res)
	}
	return f
}

func TestClaimRefusesTheDeadInstancesRowsAtOrBelowW(t *testing.T) {
	f := claimedFSM(t)
	cases := []struct {
		name    string
		p       RegisterFilePayload
		refused bool
	}{
		{"at or below W", regPayload("db/a/1.parquet", "w1", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 100}), true},
		{"above W", regPayload("db/a/2.parquet", "w1", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 101}), false},
		{"truncated coverage", regPayload("db/a/3.parquet", "w1", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 500, Truncated: true}), true},
		{"the claimant replay", regPayload("db/a/4.parquet", "w2", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 50, ClaimEpoch: 4}), false},
		{"a stale claim epoch", regPayload("db/a/5.parquet", "w2", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 50, ClaimEpoch: 3}), true},
		{"another key", regPayload("db/b/6.parquet", "w1", FileCoverage{Instance: 0xabc, Key: "db/b", MinCSeq: 1}), false},
		{"another instance", regPayload("db/a/7.parquet", "w1", FileCoverage{Instance: 0xdef, Key: "db/a", MinCSeq: 1}), false},
		{"no coverage", regPayload("db/a/8.parquet", "w1"), false},
	}
	for i, tc := range cases {
		res := applyCmd(t, f, uint64(20+i), CommandRegisterFile, tc.p)
		got := res != nil
		if got != tc.refused {
			t.Errorf("%s: refused=%v (%v), want %v", tc.name, got, res, tc.refused)
		}
		if tc.refused {
			err, _ := res.(error)
			if !IsStreamClaimConflict(err) || fmt.Sprint(StreamClaimConflictOps(err)) != "[0]" {
				t.Errorf("%s: error %v is not a claim conflict naming op 0", tc.name, res)
			}
			if _, ok := f.GetFile(tc.p.File.Path); ok {
				t.Errorf("%s: refused file is in the manifest", tc.name)
			}
		}
	}
}

// A retry of a registration that committed before the claim is not refused.
func TestClaimIdempotentRetry(t *testing.T) {
	f := claimsFSM(t, "w1", "w2")
	applyCmd(t, f, 10, CommandEnableStreamClaims, EnableStreamClaimsPayload{Nodes: []string{"w1", "w2"}, Now: t0})
	p := regPayload("db/a/1.parquet", "w1", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 5})
	if res := applyCmd(t, f, 11, CommandRegisterFile, p); res != nil {
		t.Fatal(res)
	}
	applyCmd(t, f, 12, CommandTakeOverStream, takeOver("w2", 1, map[string]uint64{"db/a": 100}))
	if res := applyCmd(t, f, 13, CommandRegisterFile, p); res != nil {
		t.Fatalf("a retry of a committed registration was refused: %v", res)
	}
}

// The batch is refused whole, naming every refused op; the outbox resends
// the rest.
func TestClaimBatchNamesEveryRefusedOp(t *testing.T) {
	f := claimedFSM(t)
	mk := func(p RegisterFilePayload) BatchFileOp {
		raw, _ := json.Marshal(p)
		return BatchFileOp{Type: CommandRegisterFile, Payload: raw}
	}
	del, _ := json.Marshal(DeleteFilePayload{Path: "db/a/old.parquet"})
	batch := BatchFileOpsPayload{Ops: []BatchFileOp{
		mk(regPayload("db/a/ok.parquet", "w1", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 200})),
		mk(regPayload("db/a/bad1.parquet", "w1", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 1})),
		{Type: CommandDeleteFile, Payload: del},
		mk(regPayload("db/a/bad2.parquet", "w1", FileCoverage{Instance: 0xabc, Key: "db/a", MinCSeq: 99})),
	}}
	res := applyCmd(t, f, 30, CommandBatchFileOps, batch)
	err, _ := res.(error)
	if !IsStreamClaimConflict(err) || fmt.Sprint(StreamClaimConflictOps(err)) != "[1 3]" {
		t.Fatalf("batch result %v, want a conflict naming ops [1 3]", res)
	}
	if _, ok := f.GetFile("db/a/ok.parquet"); ok {
		t.Fatal("a refused batch applied its other ops")
	}
	// The forwarded text form parses the same.
	if got := StreamClaimConflictOps(errors.New("leader rejected: " + err.Error())); fmt.Sprint(got) != "[1 3]" {
		t.Fatalf("ops from text = %v", got)
	}
}

func TestStreamClaimsSurviveSnapshotAndOldSnapshotsAreOff(t *testing.T) {
	f := claimedFSM(t)
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := snap.Persist(&testSnapshotSink{Writer: &buf}); err != nil {
		t.Fatal(err)
	}
	g := NewClusterFSM(zerolog.Nop())
	if err := g.Restore(io.NopCloser(&buf)); err != nil {
		t.Fatal(err)
	}
	if !g.StreamClaimsEnabled() || len(g.StreamClaims()) != 1 || g.StreamClaims()[0].W["db/a"] != 100 {
		t.Fatalf("restored: enabled=%v claims=%+v", g.StreamClaimsEnabled(), g.StreamClaims())
	}
	// A snapshot from an older build has no stream_claims: off, none — even
	// over a node that had them.
	old := claimsFSM(t, "w1")
	osnap, _ := old.Snapshot()
	var obuf bytes.Buffer
	_ = osnap.Persist(&testSnapshotSink{Writer: &obuf})
	if bytes.Contains(obuf.Bytes(), []byte("stream_claims")) {
		t.Fatal("a cluster that never enabled claims writes them into its snapshot")
	}
	if err := g.Restore(io.NopCloser(&obuf)); err != nil {
		t.Fatal(err)
	}
	if g.StreamClaimsEnabled() || len(g.StreamClaims()) != 0 {
		t.Fatal("an old snapshot left claims on")
	}
}

// The capabilities live on the node record, replaced on every join: a node
// that re-joins without them (rolled back) loses them.
func TestCapabilitiesAreReplacedOnEveryJoin(t *testing.T) {
	f := NewClusterFSM(zerolog.Nop())
	applyCmd(t, f, 1, CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w1", Role: "writer", Capabilities: []string{CapabilityStreamClaims}}})
	if n, _ := f.GetNode("w1"); len(n.Capabilities) != 1 {
		t.Fatal("capability not recorded")
	}
	applyCmd(t, f, 2, CommandAddNode, AddNodePayload{Node: NodeInfo{ID: "w1", Role: "writer"}})
	if n, _ := f.GetNode("w1"); len(n.Capabilities) != 0 {
		t.Fatal("a re-join without the capability kept it")
	}
}
