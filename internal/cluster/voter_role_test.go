package cluster

import (
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	hraft "github.com/hashicorp/raft"
)

// #862: a reader or compactor that wins Raft leadership stalls every singleton
// task in shared-storage mode, because IsPrimaryWriter is "Raft leader AND
// RoleWriter" and no node can then satisfy both halves. The fix is that only
// nodes which can ingest are voters.
func TestVotesInElections(t *testing.T) {
	tests := []struct {
		role NodeRole
		want bool
	}{
		{RoleWriter, true},
		// Standalone is the DEFAULT cluster.role and it ingests. A predicate
		// that excluded it would leave a default-configured cluster with zero
		// voters, which Raft rejects.
		{RoleStandalone, true},
		{RoleReader, false},
		{RoleCompactor, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.role), func(t *testing.T) {
			if got := tt.role.VotesInElections(); got != tt.want {
				t.Errorf("%q.VotesInElections() = %v, want %v", tt.role, got, tt.want)
			}
		})
	}
}

// At least one role must vote, or no cluster can ever elect a leader.
func TestSomeRoleVotes(t *testing.T) {
	voters := 0
	for _, r := range AllRoles() {
		if r.VotesInElections() {
			voters++
		}
	}
	if voters == 0 {
		t.Fatal("no role votes; a cluster could never elect a leader")
	}
}

// The predicate must be read through ParseRole, not off a raw string cast.
// The capabilities table's default branch returns the zero value, so an
// unknown role does not vote, while ParseRole maps it to standalone, which
// does. A migration meets exactly that input — a role recorded before
// validation existed — and stripping its vote silently is the dangerous
// direction: it is how a reconcile turns a healthy cluster into one with too
// few voters.
func TestUnknownRoleVotesWhenParsed(t *testing.T) {
	if NodeRole("writter").VotesInElections() {
		t.Error("a raw unknown role should not vote; the zero capabilities are the fail-safe")
	}
	if !ParseRole("writter").VotesInElections() {
		t.Error("parsed through ParseRole an unknown role becomes standalone and must keep its vote")
	}
	if !ParseRole("").VotesInElections() {
		t.Error("an empty role parses to standalone and must keep its vote")
	}
}

// Med-4 from review: DemoteVoter's self-refusal is the safety-critical half of
// the wrapper and had no test. raft.DefaultConfig sets ShutdownOnRemove, and a
// demotion trips the same stepDown path as a removal, so a leader that demotes
// itself shuts its own Raft down — while Arc's wrapper keeps running=true and
// n.raft non-nil, so IsLeader() quietly returns false, Apply returns
// ErrRaftShutdown, and Start() refuses to rebuild. A silent zombie that only a
// process restart fixes. The guard lives in the wrapper for that reason, and
// this pins it there.
func TestDemoteVoterRefusesSelf(t *testing.T) {
	n := startRaftNode(t, "solo-node", allocFreePort(t), true)
	t.Cleanup(func() { _ = n.Stop() })
	if err := n.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("test Raft node never became leader: %v", err)
	}

	err := n.DemoteVoter("solo-node", 5*time.Second)
	if !errors.Is(err, raft.ErrCannotDemoteSelf) {
		t.Fatalf("DemoteVoter on self = %v, want ErrCannotDemoteSelf", err)
	}

	// The refusal must be a no-op, not a partial change: the node is still a
	// leader and still a voter.
	if !n.IsLeader() {
		t.Error("the refused demotion cost this node its leadership")
	}
	cfg, cfgErr := n.GetConfiguration()
	if cfgErr != nil {
		t.Fatalf("GetConfiguration: %v", cfgErr)
	}
	for _, srv := range cfg.Servers {
		if string(srv.ID) == "solo-node" && srv.Suffrage != hraft.Voter {
			t.Errorf("suffrage after a refused self-demotion = %v, want Voter", srv.Suffrage)
		}
	}
}

// AddNonvoter on a server that is ALREADY a voter updates its address and
// leaves suffrage alone. That is hashicorp/raft's documented behaviour and the
// whole reason DemoteVoter has to exist, and it is also why a cluster upgraded
// in place does not converge just by re-adding nodes.
func TestAddNonvoterDoesNotDemoteAnExistingVoter(t *testing.T) {
	leader := startRaftNode(t, "leader-node", allocFreePort(t), true)
	t.Cleanup(func() { _ = leader.Stop() })
	if err := leader.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("no leader: %v", err)
	}

	peerAddr := allocFreePort(t)
	peer := startRaftNode(t, "peer-node", peerAddr, false)
	t.Cleanup(func() { _ = peer.Stop() })

	if err := leader.AddVoter("peer-node", peerAddr, 10*time.Second); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	if err := leader.AddNonvoter("peer-node", peerAddr, 10*time.Second); err != nil {
		t.Fatalf("AddNonvoter on an existing voter: %v", err)
	}

	cfg, err := leader.GetConfiguration()
	if err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}
	for _, srv := range cfg.Servers {
		if string(srv.ID) == "peer-node" && srv.Suffrage != hraft.Voter {
			t.Fatalf("AddNonvoter demoted an existing voter (suffrage %v); the migration story in the release notes depends on it NOT doing that", srv.Suffrage)
		}
	}

	// DemoteVoter is what actually changes it.
	if err := leader.DemoteVoter("peer-node", 10*time.Second); err != nil {
		t.Fatalf("DemoteVoter: %v", err)
	}
	cfg, err = leader.GetConfiguration()
	if err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}
	for _, srv := range cfg.Servers {
		if string(srv.ID) == "peer-node" && srv.Suffrage != hraft.Nonvoter {
			t.Errorf("DemoteVoter left suffrage %v, want Nonvoter", srv.Suffrage)
		}
	}
}
