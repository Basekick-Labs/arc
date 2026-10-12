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
	c.cfg.StreamTakeover = true
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
	c.cfg.StreamTakeover = true
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
	take, _ := json.Marshal(raft.TakeOverStreamV2Payload{By: "w2"})
	done, _ := json.Marshal(raft.ClaimDonePayload{By: "w2"})
	ann, _ := json.Marshal(raft.AnnounceInstancePayload{Origin: "w2"})
	settle, _ := json.Marshal(raft.SettleCoveragePayload{Origin: "w2"})
	settled, _ := json.Marshal(raft.InstanceSettledPayload{Origin: "w2"})
	own, _ := json.Marshal(raft.AnnounceInstancePayload{Origin: "w1"})
	extend, _ := json.Marshal(raft.ExtendStreamClaimPayload{By: "w2"})
	ownExtend, _ := json.Marshal(raft.ExtendStreamClaimPayload{By: "w1"})
	release, _ := json.Marshal(raft.ReleaseStreamClaimPayload{By: "w2"})
	ownRelease, _ := json.Marshal(raft.ReleaseStreamClaimPayload{By: "w1"})
	operatorRelease, _ := json.Marshal(raft.ReleaseStreamClaimPayload{})
	for name, tc := range map[string]struct {
		cmd raft.Command
		ok  bool
	}{
		"own registration":     {raft.Command{Type: raft.CommandRegisterFile, Payload: reg("w1")}, true},
		"another origin":       {raft.Command{Type: raft.CommandRegisterFile, Payload: reg("w2")}, false},
		"no origin":            {raft.Command{Type: raft.CommandRegisterFile, Payload: reg("")}, false},
		"batch with a foreign": {raft.Command{Type: raft.CommandBatchFileOps, Payload: batch}, false},
		"claim for another":    {raft.Command{Type: raft.CommandTakeOverStreamV2, Payload: take}, false},
		"done for another":     {raft.Command{Type: raft.CommandClaimDone, Payload: done}, false},
		"announce another":     {raft.Command{Type: raft.CommandAnnounceInstance, Payload: ann}, false},
		"settle another":       {raft.Command{Type: raft.CommandSettleCoverage, Payload: settle}, false},
		"settled another":      {raft.Command{Type: raft.CommandInstanceSettled, Payload: settled}, false},
		"own announcement":     {raft.Command{Type: raft.CommandAnnounceInstance, Payload: own}, true},
		"extend for another":   {raft.Command{Type: raft.CommandExtendStreamClaim, Payload: extend}, false},
		"own extension":        {raft.Command{Type: raft.CommandExtendStreamClaim, Payload: ownExtend}, true},
		"release another":      {raft.Command{Type: raft.CommandReleaseStreamClaim, Payload: release}, false},
		"own release":          {raft.Command{Type: raft.CommandReleaseStreamClaim, Payload: ownRelease}, true},
		"operator release":     {raft.Command{Type: raft.CommandReleaseStreamClaim, Payload: operatorRelease}, false},
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
	c.cfg.StreamTakeover = true
	if ack := forwardOnce(t, c, "writer-2", registerFileCommand(t, "testdb/cpu/2026/10/12/10/spoof.parquet", "writer-1")); ack.Status == "ok" || ack.Code != protocol.ForwardCodeAuth {
		t.Fatalf("spoofed origin: %+v", ack)
	}
	capNodes(t, c, map[string][]string{"raft-1": capList, "writer-1": capList, "writer-2": capList, "reader-1": capList})
	c.maybeEnableStreamClaims()
	waitFSM(t, c.StreamClaimsEnabled)
	// writer-1 runs instance 7, restarts on new disks as 8: 7 is orphaned,
	// and any healthy writer may claim it.
	for _, inst := range []uint64{7, 8} {
		ann, _ := json.Marshal(raft.AnnounceInstancePayload{Origin: "writer-1", Instance: inst, FreshStart: true})
		if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandAnnounceInstance, Payload: ann}, 5*time.Second); err != nil {
			t.Fatalf("announce %d: %v", inst, err)
		}
	}
	take, _ := json.Marshal(raft.TakeOverStreamV2Payload{Origin: "writer-1", By: "writer-2", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 7, Keys: map[string]raft.Intervals{"testdb/cpu": {{1, 10}}}}}})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandTakeOverStreamV2, Payload: take}); ack.Status != "ok" {
		t.Fatalf("claim: %+v", ack)
	}
	entry := makeFileEntry("testdb/cpu/2026/10/12/10/claimed.parquet", []byte("x"), "writer-2")
	entry.CreatedAt = time.Now()
	raw, _ := json.Marshal(raft.RegisterFilePayload{File: entry, Cov2: []raft.InstanceCoverage{{Instance: 7, Key: "testdb/cpu", Ranges: [][2]uint64{{3, 3}}}}})
	ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandRegisterFile, Payload: raw})
	if ack.Status == "ok" || ack.Code != protocol.ForwardCodeClaimConflict {
		t.Fatalf("claimed rows: %+v, want claim_conflict", ack)
	}
	// The claimant extends and releases its own claim through the leader.
	var epoch uint64
	for _, cl := range c.raftNode.FSM().ClaimsBy("writer-2") {
		epoch = cl.Epoch
	}
	ext, _ := json.Marshal(raft.ExtendStreamClaimPayload{Instance: 7, By: "writer-2", Epoch: epoch, Keys: map[string]raft.Intervals{"testdb/mem": {{1, 2}}}})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandExtendStreamClaim, Payload: ext}); ack.Status != "ok" {
		t.Fatalf("extend: %+v", ack)
	}
	rel, _ := json.Marshal(raft.ReleaseStreamClaimPayload{Instance: 7, Epoch: epoch, By: "writer-2"})
	if ack := forwardOnce(t, c, "writer-2", &raft.Command{Type: raft.CommandReleaseStreamClaim, Payload: rel}); ack.Status != "ok" {
		t.Fatalf("release: %+v", ack)
	}
}

// The wire name pins the build generation that may enable takeover:
// builds advertising an older name (no claimant, no exit on a claim) block it.
func TestStreamClaimsCapabilityName(t *testing.T) {
	if raft.CapabilityStreamClaims != "stream_claims_v3" {
		t.Fatalf("capability %q", raft.CapabilityStreamClaims)
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

// A node advertises stream claims only with cluster.stream_takeover on; a
// writer also needs coverage in effect.
func TestLocalCapabilitiesFollowTheEffectiveCoverage(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.cfg.WALCoverage = true // the setting alone is not enough
	c.SetWALCoverageActive(true)
	if caps := c.localCapabilities(); len(caps) != 0 {
		t.Fatalf("advertised %v without cluster.stream_takeover", caps)
	}
	c.cfg.StreamTakeover = true
	c.SetWALCoverageActive(false)
	if caps := c.localCapabilities(); len(caps) != 0 {
		t.Fatalf("advertised %v without coverage in effect", caps)
	}
	c.SetWALCoverageActive(true)
	if caps := c.localCapabilities(); !hasCapability(caps, raft.CapabilityStreamClaims) {
		t.Fatalf("advertised %v with coverage in effect", caps)
	}
	reader := originRig(t, "reader-1", RoleReader)
	if caps := reader.localCapabilities(); len(caps) != 0 {
		t.Fatal("a reader advertised stream claims without cluster.stream_takeover")
	}
	reader.cfg.StreamTakeover = true
	if caps := reader.localCapabilities(); !hasCapability(caps, raft.CapabilityStreamClaims) {
		t.Fatal("a reader does not advertise stream claims")
	}
	// Claims name nodes: without an explicit node id (hostname-PID by
	// default) the node never advertises them.
	reader.cfg.NodeID = ""
	if caps := reader.localCapabilities(); len(caps) != 0 {
		t.Fatalf("advertised %v without an explicit cluster.node_id", caps)
	}
}

// An oversized capability list is refused at join even without a secret.
func TestJoinRefusesOversizedCapabilities(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	c.cfg.SharedSecret = ""
	many := make([]string, security.MaxJoinCapabilities+1)
	for i := range many {
		many[i] = "x"
	}
	req := &protocol.JoinRequest{NodeID: "reader-8", NodeName: "reader-8", Role: string(RoleReader), ClusterName: "test-cluster",
		RaftAddr: "127.0.0.1:1", APIAddr: "127.0.0.1:2", CoordAddr: "127.0.0.1:3", Capabilities: many}
	if resp := deliverJoin(t, c, req); resp != nil && resp.Success {
		t.Fatal("an oversized capability list joined")
	}
}
