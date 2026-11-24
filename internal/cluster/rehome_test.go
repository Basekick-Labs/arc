package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// #1825 slice 4a: the re-home record, coordinator side.

var rhList = []string{raft.CapabilityCompactionCommit, raft.CapabilityRehome}

// Every build advertises the rules; only a node told its configuration is in
// scope advertises that, and nothing tells it yet (the record is dark).
func TestRehomeCapabilities(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if !hasCapability(c.localCapabilities(), raft.CapabilityRehome) {
		t.Fatalf("capabilities %v lack %s", c.localCapabilities(), raft.CapabilityRehome)
	}
	if hasCapability(c.localCapabilities(), raft.CapabilityRehomeInScope) {
		t.Fatal("in scope without being told")
	}
	c.SetRehomeInScope(true)
	if !hasCapability(c.localCapabilities(), raft.CapabilityRehomeInScope) {
		t.Fatal("in scope not advertised once told")
	}
}

// The leader enables re-home once compaction commit is on and every node in
// the FSM and every Raft server advertised it.
func TestRehomeEnableWaitsForCompactionCommitAndEveryNode(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	capNodes(t, c, map[string][]string{"raft-1": ccList, "writer-1": ccList, "writer-2": ccList, "reader-1": ccList})
	c.maybeEnableRehome()
	if c.raftNode.FSM().RehomeEnabled() {
		t.Fatal("enabled before compaction commit")
	}
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	c.maybeEnableRehome()
	if c.raftNode.FSM().RehomeEnabled() {
		t.Fatal("enabled while no node advertises re-home")
	}
	capNodes(t, c, map[string][]string{"raft-1": rhList, "writer-1": rhList, "writer-2": rhList})
	c.maybeEnableRehome()
	if c.raftNode.FSM().RehomeEnabled() {
		t.Fatal("enabled while reader-1 lacks the capability")
	}
	if st := c.RehomeStatus(); len(st.Blocking) != 1 || st.Blocking[0] != "reader-1" {
		t.Fatalf("status = %+v, want reader-1 blocking", st)
	}
	capNodes(t, c, map[string][]string{"reader-1": rhList})
	c.maybeEnableRehome()
	waitFSM(t, c.raftNode.FSM().RehomeEnabled)
	if st := c.RehomeStatus(); !st.Enabled || len(st.Blocking) != 0 {
		t.Fatalf("status = %+v", st)
	}
	// On, and still dark: no node is in scope, so no job stages.
	in := makeFileEntry("testdb/cpu/2026/10/12/10/x.parquet", []byte("x"), "writer-1")
	in.CreatedAt = time.Now()
	if err := c.raftNode.RegisterFile(in, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(raft.StageRehomePayload{Job: "j1", Origin: "writer-1", Database: "testdb", Measurement: "cpu",
		Source:  raft.CompactionInput{Path: in.Path, SizeBytes: in.SizeBytes, SHA256: in.SHA256},
		Outputs: []raft.RehomeOutput{{Key: "testdb/cpu/2026/10/11/10/o.parquet", StagedKey: raft.RehomeStagedKey("j1", 0), SizeBytes: 1, SHA256: "s"}},
		Now:     time.Now()})
	err := c.raftNode.Apply(&raft.Command{Type: raft.CommandStageRehome, Payload: raw}, 5*time.Second)
	if raft.RehomeConflictReason(err) != raft.RehomeOutOfScope {
		t.Fatalf("stage on a dark cluster: %v", err)
	}
}

// Once re-home is on, a build without it never reaches the node table.
func TestRehomeJoinThroughTheHandler(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	for _, id := range []string{"reader-9", "reader-8"} {
		if resp := deliverJoin(t, c, ccJoin(id, rhList)); resp == nil || !resp.Success {
			t.Fatalf("join %s: %+v", id, resp)
		}
	}
	if !c.rehomeGate.mu.TryLock() {
		t.Fatal("a finished join left the gate locked")
	}
	c.rehomeGate.mu.Unlock()
	capNodes(t, c, map[string][]string{"raft-1": rhList, "writer-1": rhList, "writer-2": rhList, "reader-1": rhList})
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	c.maybeEnableRehome()
	waitFSM(t, c.raftNode.FSM().RehomeEnabled)
	if resp := deliverJoin(t, c, ccJoin("reader-7", ccList)); resp == nil || resp.Success {
		t.Fatalf("a build without re-home joined: %+v", resp)
	}
	if _, ok := c.raftNode.FSM().GetNode("reader-7"); ok {
		t.Fatal("a refused joiner reached the node table")
	}
	if !c.rehomeGate.mu.TryLock() {
		t.Fatal("a refused join left the gate locked")
	}
	c.rehomeGate.mu.Unlock()
	if resp := deliverJoin(t, c, ccJoin("reader-6", rhList)); resp == nil || !resp.Success {
		t.Fatalf("a capable node refused: %+v", resp)
	}
}

func TestRehomeCommandSpeaksFor(t *testing.T) {
	enc := func(p any) []byte {
		raw, _ := json.Marshal(p)
		return raw
	}
	cases := map[string]struct {
		cmd raft.Command
		ok  bool
	}{
		"stage self":    {raft.Command{Type: raft.CommandStageRehome, Payload: enc(raft.StageRehomePayload{Origin: "w1"})}, true},
		"stage other":   {raft.Command{Type: raft.CommandStageRehome, Payload: enc(raft.StageRehomePayload{Origin: "w2"})}, false},
		"advance self":  {raft.Command{Type: raft.CommandAdvanceRehome, Payload: enc(raft.AdvanceRehomePayload{By: "w1"})}, true},
		"advance other": {raft.Command{Type: raft.CommandAdvanceRehome, Payload: enc(raft.AdvanceRehomePayload{By: "w2"})}, false},
		"claim self":    {raft.Command{Type: raft.CommandClaimRehomeFinisher, Payload: enc(raft.ClaimRehomeFinisherPayload{By: "w1"})}, true},
		"claim other":   {raft.Command{Type: raft.CommandClaimRehomeFinisher, Payload: enc(raft.ClaimRehomeFinisherPayload{By: "w2"})}, false},
		"end self":      {raft.Command{Type: raft.CommandEndRehome, Payload: enc(raft.EndRehomePayload{By: "w1"})}, true},
		"end other":     {raft.Command{Type: raft.CommandEndRehome, Payload: enc(raft.EndRehomePayload{By: "w2"})}, false},
		"end nobody":    {raft.Command{Type: raft.CommandEndRehome, Payload: enc(raft.EndRehomePayload{})}, false},
		"ack self":      {raft.Command{Type: raft.CommandAckRehome, Payload: enc(raft.AckRehomePayload{Node: "w1"})}, true},
		"ack other":     {raft.Command{Type: raft.CommandAckRehome, Payload: enc(raft.AckRehomePayload{Node: "w2"})}, false},
		"enable":        {raft.Command{Type: raft.CommandEnableRehome, Payload: enc(raft.EnableRehomePayload{})}, false},
		"garbage":       {raft.Command{Type: raft.CommandAckRehome, Payload: []byte("{")}, false},
	}
	for name, tc := range cases {
		err := rehomeCommandSpeaksFor(&tc.cmd, "w1")
		if (err == nil) != tc.ok {
			t.Errorf("%s: err=%v, want ok=%v", name, err, tc.ok)
		}
	}
}

// Through the real forward handler: a reader may ack, but not drive a job;
// no node speaks for another; enable is never accepted from a peer.
func TestForwardApplyRehomeWiring(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.raftFSM = c.raftNode.FSM()
	inScope := append([]string{raft.CapabilityRehomeInScope}, rhList...)
	capNodes(t, c, map[string][]string{"raft-1": inScope, "writer-1": inScope, "writer-2": inScope, "reader-1": inScope})
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	enable, _ := json.Marshal(raft.EnableRehomePayload{Nodes: []string{"raft-1", "writer-1", "writer-2", "reader-1"}, Now: time.Now()})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandEnableRehome, Payload: enable}); ack.Status == "ok" {
		t.Fatalf("enable from a peer: %+v", ack)
	}
	c.maybeEnableRehome()
	waitFSM(t, c.raftNode.FSM().RehomeEnabled)
	in := makeFileEntry("testdb/cpu/2026/10/12/10/x.parquet", []byte("x"), "writer-2")
	in.CreatedAt = time.Now()
	if err := c.raftNode.RegisterFile(in, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	stage := func(origin string) *raft.Command {
		raw, _ := json.Marshal(raft.StageRehomePayload{Job: "j1", Origin: origin, Database: "testdb", Measurement: "cpu",
			Source:  raft.CompactionInput{Path: in.Path, SizeBytes: in.SizeBytes, SHA256: in.SHA256},
			Outputs: []raft.RehomeOutput{{Key: "testdb/cpu/2026/10/11/10/o.parquet", StagedKey: raft.RehomeStagedKey("j1", 0), SizeBytes: 1, SHA256: "s"}},
			Now:     time.Now()})
		return &raft.Command{Type: raft.CommandStageRehome, Payload: raw}
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
	adv, _ := json.Marshal(raft.AdvanceRehomePayload{Job: "j1", By: "writer-2", To: raft.RehomeCommitted, Now: time.Now()})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandAdvanceRehome, Payload: adv}); ack.Status != "ok" {
		t.Fatalf("advance: %+v", ack)
	}
	ack := func(node string) []byte {
		raw, _ := json.Marshal(raft.AckRehomePayload{Job: "j1", Node: node, State: raft.RehomeCommitted})
		return raw
	}
	if a := forwardOnce(t, c, "reader-1", &raft.Command{Type: raft.CommandAckRehome, Payload: ack("writer-2")}); a.Status == "ok" {
		t.Fatalf("ack for another node: %+v", a)
	}
	if a := forwardOnce(t, c, "reader-1", &raft.Command{Type: raft.CommandAckRehome, Payload: ack("reader-1")}); a.Status != "ok" {
		t.Fatalf("ack by a reader: %+v", a)
	}
	j, _ := c.raftNode.FSM().RehomeJob("j1")
	if !j.Acks["reader-1"] {
		t.Fatalf("ack not recorded: %+v", j)
	}
	// A forwarded refusal keeps its reason in the text.
	early, _ := json.Marshal(raft.AdvanceRehomePayload{Job: "j1", By: "writer-2", To: raft.RehomePublished, Now: time.Now()})
	a := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandAdvanceRehome, Payload: early})
	if a.Status == "ok" || raft.RehomeConflictReason(&ForwardRejectedError{Code: a.Code, Message: a.Error}) != raft.RehomeAcksMissing {
		t.Fatalf("early advance: %+v", a)
	}
}

// The source answers from the FSM, and fails closed while this node may not
// serve reads: it owns every key, and a strict read errors.
func TestRehomeSourceFailsClosedWhileNotSynced(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	src := c.RehomeSource()
	if src == nil {
		t.Fatal("no source with Raft")
	}
	ctx := context.Background()
	if src.Owns("testdb/cpu/a.parquet") || src.Active() {
		t.Fatal("owns a key with no job, synced")
	}
	if _, err := src.ExcludedForStrict(ctx, "testdb/cpu/"); err != nil {
		t.Fatal(err)
	}
	c.readLease = &readLease{why: "not synced"}
	if !src.Owns("testdb/cpu/a.parquet") || !src.Active() {
		t.Fatal("not synced, and does not own every key")
	}
	if _, err := src.ExcludedForStrict(ctx, "testdb/cpu/"); err == nil {
		t.Fatal("a strict read went on while not synced")
	}
	var none *Coordinator
	if none.RehomeSource() != nil {
		t.Fatal("a source without a coordinator")
	}
}

// While a job is in flight, a node that does not take part in jobs is refused
// at join, even with the record's capability.
func TestRehomeJoinGateRefusesANonParticipantMidJob(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	inScope := append([]string{raft.CapabilityRehomeInScope}, rhList...)
	capNodes(t, c, map[string][]string{"raft-1": inScope, "writer-1": inScope, "writer-2": inScope, "reader-1": inScope})
	c.maybeEnableCompactionCommit()
	waitFSM(t, c.CompactionCommitActive)
	c.maybeEnableRehome()
	waitFSM(t, c.raftNode.FSM().RehomeEnabled)
	if why, release := c.rehomeJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: rhList}); why != "" {
		t.Fatalf("refused with no job: %s", why)
	} else {
		release()
	}
	in := makeFileEntry("testdb/cpu/2026/10/12/10/x.parquet", []byte("x"), "writer-1")
	in.CreatedAt = time.Now()
	if err := c.raftNode.RegisterFile(in, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(raft.StageRehomePayload{Job: "j1", Origin: "writer-1", Database: "testdb", Measurement: "cpu",
		Source:  raft.CompactionInput{Path: in.Path, SizeBytes: in.SizeBytes, SHA256: in.SHA256},
		Outputs: []raft.RehomeOutput{{Key: "testdb/cpu/2026/10/11/10/o.parquet", StagedKey: raft.RehomeStagedKey("j1", 0), SizeBytes: 1, SHA256: "s"}},
		Now:     time.Now()})
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandStageRehome, Payload: raw}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if why, _ := c.rehomeJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: rhList}); why == "" {
		t.Fatal("a node that does not take part joined mid-job")
	}
	if !c.rehomeGate.mu.TryLock() {
		t.Fatal("a refused join left the gate locked")
	}
	c.rehomeGate.mu.Unlock()
	why, release := c.rehomeJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: inScope})
	if why != "" {
		t.Fatalf("a participant refused: %s", why)
	}
	release()
}

func TestRehomeStatusSaysWhyWhileCompactionCommitIsOff(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.maybeEnableRehome()
	if st := c.RehomeStatus(); st.Enabled || len(st.Blocking) != 1 || st.Blocking[0] != "(compaction commit is off)" {
		t.Fatalf("status = %+v", st)
	}
}
