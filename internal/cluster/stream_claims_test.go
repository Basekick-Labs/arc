package cluster

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/cluster/security"
)

func capNodes(t *testing.T, c *Coordinator, caps map[string][]string) {
	t.Helper()
	for id, cs := range caps {
		role := "writer"
		if id == "reader-1" {
			role = "reader"
		}
		if err := c.raftNode.AddNode(&raft.NodeInfo{ID: id, Name: id, Role: role, ClusterName: "test-cluster", Address: id + ":9100", State: "healthy", Capabilities: cs}, 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
}

var capList = []string{raft.CapabilityStreamClaims}

// The leader enables stream claims only once every node in the FSM and every
// Raft server advertised the capability.
func TestStreamClaimsEnableWaitsForEveryNode(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.cfg.WALCoverage = true
	// The rig's raft server is "raft-1": it must be a capable node too.
	capNodes(t, c, map[string][]string{"raft-1": capList, "writer-1": capList, "writer-2": capList, "reader-1": nil})
	c.maybeEnableStreamClaims()
	if c.StreamClaimsEnabled() {
		t.Fatal("enabled while reader-1 lacks the capability")
	}
	if st := c.StreamClaimsStatus(); len(st.Blocking) != 1 || st.Blocking[0] != "reader-1" {
		t.Fatalf("status = %+v, want reader-1 blocking", st)
	}
	capNodes(t, c, map[string][]string{"reader-1": capList})
	c.maybeEnableStreamClaims()
	waitFSM(t, c.StreamClaimsEnabled)
	if st := c.StreamClaimsStatus(); !st.Enabled || len(st.Blocking) != 0 {
		t.Fatalf("status = %+v", st)
	}
}

func TestStreamClaimsEnableNeedsCoverageOnTheLeader(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	capNodes(t, c, map[string][]string{"raft-1": capList, "writer-1": capList, "writer-2": capList, "reader-1": capList})
	c.maybeEnableStreamClaims()
	time.Sleep(200 * time.Millisecond)
	if c.StreamClaimsEnabled() {
		t.Fatal("enabled with cluster.wal_coverage off")
	}
}

// Once enabled, a join without the capability is refused before it enters
// the Raft configuration; a capable one goes through.
func TestStreamClaimsJoinGate(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	why, release := c.streamClaimsJoinGate(&protocol.JoinRequest{NodeID: "w9"})
	if why != "" {
		t.Fatalf("refused before stream claims: %s", why)
	}
	// Off, and no proposer: no lock taken, joins stay concurrent.
	if !c.streamClaimsMu.TryLock() {
		t.Fatal("the gate holds the lock with stream claims off")
	}
	c.streamClaimsMu.Unlock()
	release()
	c.cfg.WALCoverage = true
	capNodes(t, c, map[string][]string{"raft-1": capList, "writer-1": capList, "writer-2": capList, "reader-1": capList})
	c.maybeEnableStreamClaims()
	waitFSM(t, c.StreamClaimsEnabled)
	if why, _ := c.streamClaimsJoinGate(&protocol.JoinRequest{NodeID: "w9"}); why == "" {
		t.Fatal("an older build joined a cluster with stream claims on")
	}
	why, release = c.streamClaimsJoinGate(&protocol.JoinRequest{NodeID: "w9", Capabilities: capList})
	if why != "" {
		t.Fatalf("a capable node refused: %s", why)
	}
	if c.streamClaimsMu.TryLock() {
		t.Fatal("the gate released the lock before the join finished, with stream claims on")
	}
	release()
}

// A forwarded registration or claim must speak for the peer that signed it.
func TestManifestCommandSpeaksFor(t *testing.T) {
	reg := func(origin string) []byte {
		raw, _ := json.Marshal(raft.RegisterFilePayload{File: raft.FileEntry{Path: "p", OriginNodeID: origin}})
		return raw
	}
	batch, _ := json.Marshal(raft.BatchFileOpsPayload{Ops: []raft.BatchFileOp{
		{Type: raft.CommandRegisterFile, Payload: reg("w1")},
		{Type: raft.CommandRegisterFile, Payload: reg("w2")},
	}})
	take, _ := json.Marshal(raft.TakeOverStreamPayload{By: "w2"})
	for name, tc := range map[string]struct {
		cmd raft.Command
		ok  bool
	}{
		"own registration":     {raft.Command{Type: raft.CommandRegisterFile, Payload: reg("w1")}, true},
		"another origin":       {raft.Command{Type: raft.CommandRegisterFile, Payload: reg("w2")}, false},
		"no origin":            {raft.Command{Type: raft.CommandRegisterFile, Payload: reg("")}, false},
		"batch with a foreign": {raft.Command{Type: raft.CommandBatchFileOps, Payload: batch}, false},
		"claim for another":    {raft.Command{Type: raft.CommandTakeOverStream, Payload: take}, false},
	} {
		err := manifestCommandSpeaksFor(&tc.cmd, "w1")
		if (err == nil) != tc.ok {
			t.Errorf("%s: err=%v, want ok=%v", name, err, tc.ok)
		}
	}
}

// Through the real forward handler: a registration naming another origin is
// refused, and a claim conflict comes back with its own terminal code.
func TestForwardApplyClaimWiring(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.raftFSM = c.raftNode.FSM()
	c.cfg.WALCoverage = true
	if ack := forwardOnce(t, c, "writer-2", registerFileCommand(t, "testdb/cpu/2026/10/12/10/spoof.parquet", "writer-1")); ack.Status == "ok" || ack.Code != protocol.ForwardCodeAuth {
		t.Fatalf("spoofed origin: %+v", ack)
	}
	capNodes(t, c, map[string][]string{"raft-1": capList, "writer-1": capList, "writer-2": capList, "reader-1": capList})
	c.maybeEnableStreamClaims()
	waitFSM(t, c.StreamClaimsEnabled)
	take, _ := json.Marshal(raft.TakeOverStreamPayload{Instance: 7, Origin: "writer-1", By: "writer-2", Epoch: 1, W: map[string]uint64{"testdb/cpu": 10}, Now: time.Now()})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandTakeOverStream, Payload: take}); ack.Status != "ok" {
		t.Fatalf("claim: %+v", ack)
	}
	entry := makeFileEntry("testdb/cpu/2026/10/12/10/claimed.parquet", []byte("x"), "writer-2")
	entry.CreatedAt = time.Now()
	raw, _ := json.Marshal(raft.RegisterFilePayload{File: entry, Coverage: []raft.FileCoverage{{Instance: 7, Key: "testdb/cpu", MinCSeq: 3}}})
	ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandRegisterFile, Payload: raw})
	if ack.Status == "ok" || ack.Code != protocol.ForwardCodeClaimConflict {
		t.Fatalf("claimed rows: %+v, want claim_conflict", ack)
	}
}

func capsJoin(secret string, caps []string, sign2 bool) *protocol.JoinRequest {
	req := &protocol.JoinRequest{NodeID: "reader-9", NodeName: "reader-9", Role: string(RoleReader), ClusterName: "test-cluster",
		RaftAddr: "127.0.0.1:1", APIAddr: "127.0.0.1:2", CoordAddr: "127.0.0.1:3", Version: "v", CoreCount: 1, Capabilities: caps}
	nonce, _ := security.GenerateNonce()
	req.AuthTimestamp = time.Now().Unix()
	req.AuthNonce = nonce
	req.AuthHMAC = security.ComputeJoinHMAC(secret, nonce, req.AuthTimestamp, joinAuthFields(req))
	if sign2 {
		req.AuthHMAC2 = security.ComputeJoinCapsHMAC(secret, nonce, req.AuthTimestamp, joinAuthFields(req), caps)
	}
	return req
}

// Through handleJoinRequest: capabilities need the second MAC, and land on
// the node record when it verifies.
func TestJoinCarriesCapabilitiesToTheNodeRecord(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if resp := deliverJoin(t, c, capsJoin(osSecret, capList, false)); resp != nil && resp.Success {
		t.Fatal("capabilities without their MAC were accepted")
	}
	resp := deliverJoin(t, c, capsJoin(osSecret, capList, true))
	if resp == nil || !resp.Success {
		t.Fatalf("join refused: %+v", resp)
	}
	waitFSM(t, func() bool {
		n, ok := c.raftNode.FSM().GetNode("reader-9")
		return ok && hasCapability(n.Capabilities, raft.CapabilityStreamClaims)
	})
}

// A join from a writer without coverage in effect advertises nothing; with
// it, stream claims.
func TestLocalCapabilitiesFollowTheEffectiveCoverage(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.cfg.WALCoverage = true // the setting alone is not enough
	if caps := c.localCapabilities(); len(caps) != 0 {
		t.Fatalf("advertised %v without coverage in effect", caps)
	}
	c.SetWALCoverageActive(true)
	if caps := c.localCapabilities(); !hasCapability(caps, raft.CapabilityStreamClaims) {
		t.Fatalf("advertised %v with coverage in effect", caps)
	}
	reader := originRig(t, "reader-1", RoleReader)
	if caps := reader.localCapabilities(); !hasCapability(caps, raft.CapabilityStreamClaims) {
		t.Fatal("a reader does not advertise stream claims")
	}
}
