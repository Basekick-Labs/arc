package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

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
