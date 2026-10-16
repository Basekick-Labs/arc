package cluster

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
)

var ccList = []string{raft.CapabilityCompactionCommit}

// #1207 d2: the leader enables compaction commit once every node in the FSM
// and every Raft server advertised it, with no setting needed.
func TestCompactionCommitEnableWaitsForEveryNode(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	capNodes(t, c, map[string][]string{"raft-1": ccList, "writer-1": ccList, "writer-2": ccList, "reader-1": capList})
	c.maybeEnableCompactionCommit()
	if c.CompactionCommitActive() {
		t.Fatal("enabled while reader-1 lacks the capability")
	}
	if st := c.CompactionCommitStatus(); len(st.Blocking) != 1 || st.Blocking[0] != "reader-1" {
		t.Fatalf("status = %+v, want reader-1 blocking", st)
	}
	capNodes(t, c, map[string][]string{"reader-1": ccList})
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	if st := c.CompactionCommitStatus(); !st.Enabled || len(st.Blocking) != 0 {
		t.Fatalf("status = %+v", st)
	}
}

// Every node may propose the flag, so a join always holds the gate's lock
// until it finishes; once the flag is on, a join without the capability is
// refused before it enters the Raft configuration.
func TestCompactionCommitJoinGate(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	why, release := c.compactionCommitJoinGate(&protocol.JoinRequest{NodeID: "w9"})
	if why != "" {
		t.Fatalf("refused before the flag: %s", why)
	}
	if c.compactionCommit.mu.TryLock() {
		t.Fatal("the gate released the lock before the join finished")
	}
	release()
	capNodes(t, c, map[string][]string{"raft-1": ccList, "writer-1": ccList, "writer-2": ccList, "reader-1": ccList})
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	if why, _ := c.compactionCommitJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: capList}); why == "" {
		t.Fatal("an older build joined a cluster with compaction commit on")
	}
	if !c.compactionCommit.mu.TryLock() {
		t.Fatal("a refused join left the gate locked")
	}
	c.compactionCommit.mu.Unlock()
	why, release = c.compactionCommitJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: ccList})
	if why != "" {
		t.Fatalf("a capable node refused: %s", why)
	}
	if c.compactionCommit.mu.TryLock() {
		t.Fatal("the gate released the lock before the join finished")
	}
	release()
}

// A forwarded compaction command must speak for the peer that signed it.
func TestCompactionCommandSpeaksFor(t *testing.T) {
	enc := func(p any) []byte {
		raw, _ := json.Marshal(p)
		return raw
	}
	cases := map[string]struct {
		cmd raft.Command
		ok  bool
	}{
		"stage self":      {raft.Command{Type: raft.CommandStageCompaction, Payload: enc(raft.StageCompactionPayload{Origin: "w1"})}, true},
		"stage other":     {raft.Command{Type: raft.CommandStageCompaction, Payload: enc(raft.StageCompactionPayload{Origin: "w2"})}, false},
		"commit self":     {raft.Command{Type: raft.CommandCommitCompaction, Payload: enc(raft.CommitCompactionPayload{Origin: "w1"})}, true},
		"commit other":    {raft.Command{Type: raft.CommandCommitCompaction, Payload: enc(raft.CommitCompactionPayload{Origin: "w2"})}, false},
		"held self":       {raft.Command{Type: raft.CommandCompactionHeld, Payload: enc(raft.CompactionHeldPayload{Node: "w1"})}, true},
		"held other":      {raft.Command{Type: raft.CommandCompactionHeld, Payload: enc(raft.CompactionHeldPayload{Node: "w2"})}, false},
		"abort self":      {raft.Command{Type: raft.CommandAbortCompaction, Payload: enc(raft.AbortCompactionPayload{By: "w1"})}, true},
		"abort other":     {raft.Command{Type: raft.CommandAbortCompaction, Payload: enc(raft.AbortCompactionPayload{By: "w2"})}, false},
		"finalize self":   {raft.Command{Type: raft.CommandFinalizeCompaction, Payload: enc(raft.FinalizeCompactionPayload{By: "w1"})}, true},
		"finalize other":  {raft.Command{Type: raft.CommandFinalizeCompaction, Payload: enc(raft.FinalizeCompactionPayload{By: "w2"})}, false},
		"revert self":     {raft.Command{Type: raft.CommandRevertCompaction, Payload: enc(raft.RevertCompactionPayload{By: "w1"})}, true},
		"revert other":    {raft.Command{Type: raft.CommandRevertCompaction, Payload: enc(raft.RevertCompactionPayload{By: "w2"})}, false},
		"revert nobody":   {raft.Command{Type: raft.CommandRevertCompaction, Payload: enc(raft.RevertCompactionPayload{})}, false},
		"claim self":      {raft.Command{Type: raft.CommandClaimFinisher, Payload: enc(raft.ClaimFinisherPayload{By: "w1"})}, true},
		"claim other":     {raft.Command{Type: raft.CommandClaimFinisher, Payload: enc(raft.ClaimFinisherPayload{By: "w2"})}, false},
		"cleanup self":    {raft.Command{Type: raft.CommandCleanupCompaction, Payload: enc(raft.CleanupCompactionPayload{By: "w1"})}, true},
		"cleanup other":   {raft.Command{Type: raft.CommandCleanupCompaction, Payload: enc(raft.CleanupCompactionPayload{By: "w2"})}, false},
		"stage malformed": {raft.Command{Type: raft.CommandStageCompaction, Payload: []byte("{")}, false},
	}
	for name, tc := range cases {
		err := manifestCommandSpeaksFor(&tc.cmd, "w1")
		if (err == nil) != tc.ok {
			t.Errorf("%s: err=%v, want ok=%v", name, err, tc.ok)
		}
	}
}

// Through the real forward handler: a compactor stages for itself, a reader
// may only report holding a staged output, and a stage naming another node
// is refused.
func TestForwardApplyCompactionWiring(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.raftFSM = c.raftNode.FSM()
	capNodes(t, c, map[string][]string{"raft-1": ccList, "writer-1": ccList, "writer-2": ccList, "reader-1": ccList})
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	in := makeFileEntry("testdb/cpu/2026/10/12/10/in.parquet", []byte("in"), "writer-1")
	in.CreatedAt = time.Now()
	if err := c.raftNode.RegisterFile(in, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	out := makeFileEntry("testdb/cpu/2026/10/12/10/out.parquet", []byte("out"), "writer-2")
	out.CreatedAt = time.Now()
	stage := func(origin string) *raft.Command {
		raw, _ := json.Marshal(raft.StageCompactionPayload{Job: "j1", Origin: origin, Output: out, Now: time.Now(),
			Inputs: []raft.CompactionInput{{Path: in.Path, SizeBytes: in.SizeBytes, SHA256: in.SHA256}}})
		return &raft.Command{Type: raft.CommandStageCompaction, Payload: raw}
	}
	if ack := forwardOnce(t, c, "writer-2", stage("writer-1")); ack.Status == "ok" || ack.Code != protocol.ForwardCodeAuth {
		t.Fatalf("stage for another node: %+v", ack)
	}
	if ack := forwardOnce(t, c, "reader-1", stage("reader-1")); ack.Status == "ok" || ack.Code != protocol.ForwardCodeAuth {
		t.Fatalf("stage by a reader: %+v", ack)
	}
	if ack := forwardOnce(t, c, "writer-2", stage("writer-2")); ack.Status != "ok" {
		t.Fatalf("stage: %+v", ack)
	}
	held, _ := json.Marshal(raft.CompactionHeldPayload{Job: "j1", Node: "reader-1", SHA256: out.SHA256})
	if ack := forwardOnce(t, c, "reader-1", &raft.Command{Type: raft.CommandCompactionHeld, Payload: held}); ack.Status != "ok" {
		t.Fatalf("held by a reader: %+v", ack)
	}
	commit, _ := json.Marshal(raft.CommitCompactionPayload{Job: "j1", Origin: "writer-2", Output: out.Path, SHA256: out.SHA256, Now: time.Now()})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandCommitCompaction, Payload: commit}); ack.Status != "ok" {
		t.Fatalf("commit: %+v", ack)
	}
	if _, ok := c.raftNode.FSM().GetFile(in.Path); ok {
		t.Fatal("input still registered after the commit")
	}
	// A forwarded refusal keeps its reason in the text.
	ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandCommitCompaction, Payload: func() []byte {
		raw, _ := json.Marshal(raft.CommitCompactionPayload{Job: "zz", Origin: "writer-2", Output: "x", SHA256: "y", Now: time.Now()})
		return raw
	}()})
	if ack.Status == "ok" || raft.CompactionConflictReason(&ForwardRejectedError{Code: ack.Code, Message: ack.Error}) != raft.CompactionUnknownJob {
		t.Fatalf("refused commit: %+v", ack)
	}
}

func ccJoin(id string, caps []string) *protocol.JoinRequest {
	req := &protocol.JoinRequest{NodeID: id, NodeName: id, Role: string(RoleReader), ClusterName: "test-cluster",
		RaftAddr: "127.0.0.1:1" + id[len(id)-1:], APIAddr: "127.0.0.1:2", CoordAddr: "127.0.0.1:3", Version: "v", CoreCount: 1, Capabilities: caps}
	nonce, _ := security.GenerateNonce()
	req.AuthTimestamp = time.Now().Unix()
	req.AuthNonce = nonce
	req.AuthHMAC = security.ComputeJoinHMAC(osSecret, nonce, req.AuthTimestamp, joinAuthFields(req))
	req.AuthHMAC2 = security.ComputeJoinCapsHMAC(osSecret, nonce, req.AuthTimestamp, joinAuthFields(req), caps)
	return req
}

// Through handleJoinRequest: every join releases the gate (a second join
// would hang otherwise), and once the flag is on a build without the
// capability never reaches the node table.
func TestCompactionCommitJoinThroughTheHandler(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	for _, id := range []string{"reader-9", "reader-8"} {
		if resp := deliverJoin(t, c, ccJoin(id, ccList)); resp == nil || !resp.Success {
			t.Fatalf("join %s: %+v", id, resp)
		}
	}
	if !c.compactionCommit.mu.TryLock() {
		t.Fatal("a finished join left the gate locked")
	}
	c.compactionCommit.mu.Unlock()
	capNodes(t, c, map[string][]string{"raft-1": ccList, "writer-1": ccList, "writer-2": ccList, "reader-1": ccList})
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	if resp := deliverJoin(t, c, ccJoin("reader-7", capList)); resp == nil || resp.Success {
		t.Fatalf("an older build joined with the flag on: %+v", resp)
	}
	if _, ok := c.raftNode.FSM().GetNode("reader-7"); ok {
		t.Fatal("the refused node is in the node table")
	}
	if resp := deliverJoin(t, c, ccJoin("reader-6", ccList)); resp == nil || !resp.Success {
		t.Fatalf("a capable join after a refusal: %+v", resp)
	}
}

// A Raft server whose node record never applied could run anything: it
// keeps the flag off even when every node in the table is capable.
func TestCompactionCommitEnableWaitsForRaftServers(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	capNodes(t, c, map[string][]string{"writer-1": ccList, "writer-2": ccList, "reader-1": ccList})
	c.maybeEnableCompactionCommit()
	time.Sleep(200 * time.Millisecond)
	if c.CompactionCommitActive() {
		t.Fatal("enabled with raft-1 unknown to the node table")
	}
	if st := c.CompactionCommitStatus(); len(st.Blocking) != 1 || st.Blocking[0] != "raft-1" {
		t.Fatalf("status = %+v", st)
	}
}

// A join without the capability waits for a barrier only while an enable
// proposal may still commit, and is refused when the barrier cannot run.
func TestCompactionCommitJoinGateBarriersWhileAProposalIsInFlight(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if err := c.raftNode.Stop(); err != nil {
		t.Fatal(err)
	}
	old := &protocol.JoinRequest{NodeID: "w9", Capabilities: capList}
	why, release := c.compactionCommitJoinGate(old)
	if why != "" {
		t.Fatalf("no proposal in flight, refused: %s", why)
	}
	release()
	c.compactionCommit.inFlight = true
	if why, _ := c.compactionCommitJoinGate(old); !strings.Contains(why, "could not confirm") {
		t.Fatalf("a proposal in flight and no barrier: %q", why)
	}
	if !c.compactionCommit.mu.TryLock() {
		t.Fatal("a refused join left the gate locked")
	}
	c.compactionCommit.mu.Unlock()
}
