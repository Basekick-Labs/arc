package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/protocol"
	"github.com/basekick-labs/arc/internal/cluster/raft"
)

func TestCQPoliciesEnableWaitsForCapabilityAndGatesOldJoins(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	caps := []string{raft.CapabilityClusterPolicies, raft.CapabilityClusterCQ}
	capNodes(t, c, map[string][]string{"raft-1": caps, "writer-1": caps, "writer-2": caps, "reader-1": caps})
	c.maybeEnableClusterPolicies()
	waitFSM(t, c.ClusterPoliciesActive)
	c.maybeEnableCQPolicies()
	waitFSM(t, c.CQEnabled)
	old := &protocol.JoinRequest{NodeID: "legacy-writer", Capabilities: []string{raft.CapabilityClusterPolicies}}
	if why, release := c.clusterPoliciesJoinGate(old); why == "" || release != nil {
		if release != nil {
			release()
		}
		t.Fatalf("old node join gate = %q, release_nil=%t; expected refusal", why, release == nil)
	}
	newNode := &protocol.JoinRequest{NodeID: "cq-writer", Capabilities: caps}
	why, release := c.clusterPoliciesJoinGate(newNode)
	if why != "" {
		t.Fatalf("capable node refused: %s", why)
	}
	release()
}

func TestPlanCQSeedDeduplicatesDefinitionsAndFencesProgress(t *testing.T) {
	watermark := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	entry := func(id int64) raft.CQPolicyEntry {
		return raft.CQPolicyEntry{ID: id, Name: "hourly", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_hourly", Query: "SELECT count(*) FROM db.cpu", Interval: "1h", IsActive: true, LastProcessedTime: &watermark}
	}
	rows := map[string]CQLocalRows{
		"a": {Policies: []raft.CQPolicyEntry{entry(4)}, MaxIssuedID: 4, Completed: map[int64]bool{4: true}},
		"b": {Policies: []raft.CQPolicyEntry{entry(4)}, MaxIssuedID: 4, Completed: map[int64]bool{4: true}},
	}
	seed, why := planCQSeed(rows, nil)
	if why != "" {
		t.Fatal(why)
	}
	if len(seed.Entries) != 1 || seed.Entries[0].ID != 5 || seed.Floor != 5 {
		t.Fatalf("seed IDs/floor = %+v / %d, want collision-safe ID and floor 5", seed.Entries, seed.Floor)
	}
	if len(seed.Entries[0].Sources) != 2 {
		t.Fatalf("sources = %+v, want both nodes", seed.Entries[0].Sources)
	}

	other := watermark.Add(time.Hour)
	conflict := entry(4)
	conflict.LastProcessedTime = &other
	rows["b"] = CQLocalRows{Policies: []raft.CQPolicyEntry{conflict}, Completed: map[int64]bool{4: true}}
	if _, why := planCQSeed(rows, nil); why == "" {
		t.Fatal("conflicting completed watermarks were accepted")
	}
}

func TestPlanCQSeedRefusesNeverRunActiveAndHonorsReviewedChoice(t *testing.T) {
	e := raft.CQPolicyEntry{ID: 1, Name: "hourly", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "cpu_hourly", Query: "SELECT count(*) FROM db.cpu", Interval: "1h", IsActive: true}
	rows := map[string]CQLocalRows{"a": {Policies: []raft.CQPolicyEntry{e}, MaxIssuedID: 1}}
	if _, why := planCQSeed(rows, nil); why == "" {
		t.Fatal("active query with no completed run was accepted")
	}
	hash := CQRowsHash(rows["a"])
	seed, why := planCQSeed(rows, &raft.PolicyAdoptChoice{Node: "a", Hash: hash})
	if why != "" {
		t.Fatal(why)
	}
	if len(seed.Entries) != 1 || seed.Entries[0].IsActive {
		t.Fatalf("reviewed seed = %+v, expected non-running query to seed inactive", seed.Entries)
	}
	if _, why := planCQSeed(rows, &raft.PolicyAdoptChoice{Node: "a", Hash: "stale"}); why == "" {
		t.Fatal("stale reviewed inventory was accepted")
	}
}

func TestPlanCQSeedRespectsRaftForwardingSizeLimit(t *testing.T) {
	e := raft.CQPolicyEntry{ID: 1, Name: "large", Database: "db", SourceMeasurement: "cpu", DestinationMeasurement: "out", Query: strings.Repeat("x", raft.MaxSeedCQPayloadBytes), Interval: "1h", IsActive: false}
	_, why := planCQSeed(map[string]CQLocalRows{"a": {Policies: []raft.CQPolicyEntry{e}, MaxIssuedID: 1}}, nil)
	if why == "" || !strings.Contains(why, "forwarding limit") {
		t.Fatalf("oversized seed result=%q", why)
	}
}
