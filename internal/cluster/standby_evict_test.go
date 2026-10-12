package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"

	"github.com/basekick-labs/arc/internal/cluster/originstream"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

func propose(t *testing.T, c *Coordinator, typ raft.CommandType, p any) {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.applyOrForward(context.Background(), &raft.Command{Type: typ, Payload: raw}); err != nil {
		t.Fatalf("command %d: %v", typ, err)
	}
}

func TestStandbyEvictPolicy(t *testing.T) {
	off, _ := takeoverOriginRig(t, false)
	p := &standbyEvictPolicy{c: off}
	if p.active() {
		t.Fatal("flag off: the policy applies")
	}

	c, _ := takeoverOriginRig(t, true)
	p = &standbyEvictPolicy{c: c}
	if !p.active() {
		t.Fatal("flag on: the policy does not apply")
	}
	ann := func(origin string, inst uint64, holds ...uint64) {
		propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: origin, Instance: inst, Holds: holds, FreshStart: true})
	}
	ann("writer-2", 0x10)       // current, then adopted by 0x11
	ann("writer-2", 0x11, 0x10) // current
	info := func(inst uint64) originstream.InstanceInfo {
		return originstream.InstanceInfo{Origin: "writer-2", Instance: inst, Floors: true, FloorsApplied: 1}
	}
	cases := []struct {
		name string
		in   originstream.InstanceInfo
		want int
	}{
		{"no floors yet", originstream.InstanceInfo{Origin: "writer-2", Instance: 0x11}, originstream.EvictNever},
		{"unknown, no floors yet", originstream.InstanceInfo{Origin: "writer-2", Instance: 0x99}, originstream.EvictNever},
		{"unknown, no floors, replaced", originstream.InstanceInfo{Origin: "writer-2", Instance: 0x99, Replaced: true}, originstream.EvictFirst},
		{"current instance", info(0x11), originstream.EvictNever},
		{"unknown, manifest caught up", info(0x99), originstream.EvictFirst},
		{"unknown, manifest behind", originstream.InstanceInfo{Origin: "writer-2", Instance: 0x99, Floors: true, FloorsApplied: 1 << 40}, originstream.EvictNever},
		{"unknown, replaced", originstream.InstanceInfo{Origin: "writer-2", Instance: 0x99, Floors: true, FloorsApplied: 1 << 40, Replaced: true}, originstream.EvictFirst},
		{"adopted, origin not heard from", info(0x10), originstream.EvictNever},
	}
	for _, tc := range cases {
		if got := p.tier(tc.in); got != tc.want {
			t.Errorf("%s: tier %d, want %d", tc.name, got, tc.want)
		}
	}
	// Adopted with a healthy origin: only under pressure.
	n, ok := c.registry.Get("writer-2")
	if !ok {
		n = NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
		if err := c.registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	n.UpdateState(StateHealthy)
	if got := p.tier(info(0x10)); got != originstream.EvictUnderPressure {
		t.Errorf("adopted, healthy origin: tier %d", got)
	}
	// Claimed and unfinished: never; finished: first.
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x11, Keys: map[string]raft.Intervals{"db/cpu": {{1, 5}}}}, {Instance: 0x10, Keys: map[string]raft.Intervals{"db/cpu": {{1, 5}}}}}})
	if got := p.tier(info(0x10)); got != originstream.EvictNever {
		t.Errorf("claimed: tier %d", got)
	}
	var epoch uint64
	for _, cl := range c.raftNode.FSM().ClaimsBy("writer-1") {
		if cl.Instance == 0x10 {
			epoch = cl.Epoch
		}
	}
	propose(t, c, raft.CommandClaimDone, raft.ClaimDonePayload{Instance: 0x10, By: "writer-1", Epoch: epoch})
	if got := p.tier(originstream.InstanceInfo{Origin: "writer-2", Instance: 0x10}); got != originstream.EvictFirst {
		t.Errorf("claim done, no floors: tier %d", got)
	}
	// Released: claimable again, kept.
	for _, cl := range c.raftNode.FSM().ClaimsBy("writer-1") {
		if cl.Instance == 0x11 {
			propose(t, c, raft.CommandReleaseStreamClaim, raft.ReleaseStreamClaimPayload{Instance: 0x11, Epoch: cl.Epoch, By: "writer-1"})
		}
	}
	if got := p.tier(info(0x11)); got != originstream.EvictNever {
		t.Errorf("released claim: tier %d", got)
	}
	// Untracked: can never be claimed, evictable even without floors.
	ann("writer-2", 0x12, 0x11)
	big := raft.InstanceCoverage{Instance: 0x12, Key: "db/x", Truncated: true}
	propose(t, c, raft.CommandRegisterFile, raft.RegisterFilePayload{File: raft.FileEntry{Path: "db/x/t.parquet", SHA256: "t", SizeBytes: 1,
		Database: "db", Measurement: "x", PartitionTime: time.Now(), OriginNodeID: "writer-2", CreatedAt: time.Now()}, Cov2: []raft.InstanceCoverage{big}})
	if got := p.tier(originstream.InstanceInfo{Origin: "writer-2", Instance: 0x12}); got != originstream.EvictFirst {
		t.Errorf("untracked: tier %d", got)
	}
	// 0x11 is adopted by 0x12 now, its claim released, its origin healthy:
	// its origin publishes it, so only under pressure.
	if got := p.tier(info(0x11)); got != originstream.EvictUnderPressure {
		t.Errorf("released claim, adopted, healthy origin: tier %d", got)
	}
}

// A trim pass keeps every record of an origin with an unfinished claim.
func TestTrimWaitsForAnUnfinishedClaim(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	c.storage = store
	pass := (&originStream{present: map[string]int64{}}).trimReady(c)
	idx := c.raftNode.FSM().AppliedDone()
	k := originstream.TrimKey{Origin: "writer-2", Key: "db/cpu", AppliedIndex: idx}
	ready := func() bool { return pass(context.Background(), []originstream.TrimKey{k})[k] }
	// An origin this node does not hear from may be dead: its records wait
	// for a claim (or its return).
	if ready() {
		t.Fatal("trimmed the records of an origin that is not heard from")
	}
	peer := NewNode("writer-2", "writer-2", RoleWriter, "test-cluster")
	peer.UpdateState(StateHealthy)
	if err := c.registry.Register(peer); err != nil {
		t.Fatal(err)
	}
	if !ready() {
		t.Fatal("no file and no claim: not ready")
	}
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x10, FreshStart: true})
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x10, Keys: map[string]raft.Intervals{"db/cpu": {{1, 5}}}}}})
	if ready() {
		t.Fatal("trimmed while a claim on the origin is unfinished")
	}
	epoch := c.raftNode.FSM().ClaimsBy("writer-1")[0].Epoch
	propose(t, c, raft.CommandClaimDone, raft.ClaimDonePayload{Instance: 0x10, By: "writer-1", Epoch: epoch})
	if !ready() {
		t.Fatal("not ready after the claim finished")
	}
}

// The leader expires a departure older than the TTL, as an operator removal
// would; a younger one stays.
func TestExpireDeparted(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x10, FreshStart: true})
	now := time.Now()
	if err := c.raftNode.RemoveNodeWithReason("writer-2", raft.RemoveReasonLeave, now.Add(-2*time.Hour), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.raftNode.RemoveNodeWithReason("reader-1", raft.RemoveReasonLeave, now, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	fsm := c.raftNode.FSM()
	if d := fsm.DepartedWriters(); len(d) != 1 {
		t.Fatalf("a reader leaving is not a departed writer: %v", d)
	}
	c.expireDeparted(now, 3*time.Hour)
	if len(fsm.DepartedWriters()) != 1 {
		t.Fatal("expired before the TTL")
	}
	c.expireDeparted(now, time.Hour)
	if len(fsm.DepartedWriters()) != 0 {
		t.Fatal("not expired after the TTL")
	}
	for _, in := range fsm.StreamInstances() {
		if in.Instance == 0x10 && (in.Status != raft.InstanceOrphaned || !in.OriginGone) {
			t.Fatalf("expired writer's instance %+v", in)
		}
	}
}

func TestStreamTakeoverView(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	c.cfg.StreamTakeoverDepartedTTL = time.Hour
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x10, FreshStart: true})
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x11, Holds: []uint64{0x10}, FreshStart: true})
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x10, Keys: map[string]raft.Intervals{"db/cpu": {{1, 5}}, "db/mem": {{2, 3}}}}, {Instance: 0x11}}})
	at := time.Now().UTC().Truncate(time.Second)
	if err := c.raftNode.RemoveNodeWithReason("writer-2", raft.RemoveReasonLeave, at, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	v := c.StreamTakeover()
	if !v.Enabled || len(v.Instances) != 2 || v.Instances[0].Instance != "0000000000000010" || v.Instances[0].AdoptedBy != "0000000000000011" {
		t.Fatalf("instances %+v", v.Instances)
	}
	if len(v.Claims) != 1 || v.Claims[0].Keys != 2 || v.Claims[0].Batches != 7 || v.Claims[0].By != "writer-1" {
		t.Fatalf("claims %+v", v.Claims)
	}
	if len(v.Departed) != 1 || v.Departed[0].NodeID != "writer-2" || !v.Departed[0].ExpiresAt.Equal(at.Add(time.Hour)) {
		t.Fatalf("departed %+v", v.Departed)
	}
}

// A writer whose current process is claimed is not eligible as primary.
func TestWriterEligibility(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x10, FreshStart: true})
	if !c.writerEligible("writer-2") {
		t.Fatal("ineligible without a claim")
	}
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-1", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x10, Keys: map[string]raft.Intervals{"db/cpu": {{1, 5}}}}}})
	if c.writerEligible("writer-2") || !c.writerEligible("writer-1") {
		t.Fatal("eligibility does not follow the claim")
	}
}

// A claim another writer finished may have lacked batches this copy holds
// (a follow-up claims them): evicted only under pressure.
func TestStandbyEvictPolicyAfterAnotherClaimant(t *testing.T) {
	c, _ := takeoverOriginRig(t, true)
	p := &standbyEvictPolicy{c: c}
	if err := c.raftNode.AddNode(&raft.NodeInfo{ID: "writer-3", Name: "writer-3", Role: string(RoleWriter), ClusterName: "test-cluster", Address: "writer-3:9100", State: string(StateHealthy)}, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := c.raftNode.PromoteWriter("writer-3", "writer-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFSM(t, func() bool { return c.designatedPrimaryWriterID() == "writer-3" })
	propose(t, c, raft.CommandAnnounceInstance, raft.AnnounceInstancePayload{Origin: "writer-2", Instance: 0x20, FreshStart: true})
	propose(t, c, raft.CommandTakeOverStreamV2, raft.TakeOverStreamV2Payload{Origin: "writer-2", By: "writer-3", Now: time.Now(),
		Claims: []raft.ClaimSet{{Instance: 0x20, Keys: map[string]raft.Intervals{"db/cpu": {{1, 5}}}}}})
	e := c.raftNode.FSM().ClaimsBy("writer-3")[0].Epoch
	propose(t, c, raft.CommandClaimDone, raft.ClaimDonePayload{Instance: 0x20, By: "writer-3", Epoch: e})
	if got := p.tier(originstream.InstanceInfo{Origin: "writer-2", Instance: 0x20, Floors: true}); got != originstream.EvictUnderPressure {
		t.Fatalf("another writer finished: tier %d", got)
	}
}
