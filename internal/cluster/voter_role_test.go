package cluster

import "testing"

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
