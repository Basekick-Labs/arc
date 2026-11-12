package governance

import (
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// benchGovernanceWriter turns cluster governance policies on in the
// benchmark's FSM and seeds them with one token's policy, then returns a
// write that replaces that policy: BenchmarkAdmitParallel applies one every
// 1000 file registrations, as an admin's policy change lands among ingest
// applies. (A build without cluster governance policies returns a no-op.)
func benchGovernanceWriter(b *testing.B, apply func(raft.CommandType, any) (uint64, interface{})) func(i int) {
	b.Helper()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	must := func(typ raft.CommandType, p any) uint64 {
		idx, res := apply(typ, p)
		if res != nil {
			b.Fatal(res)
		}
		return idx
	}
	must(raft.CommandEnableClusterPolicies, raft.EnableClusterPoliciesPayload{Nodes: []string{"w1"}, Now: now})
	must(raft.CommandEnableGovernancePolicies, raft.EnableGovernancePoliciesPayload{Nodes: []string{"w1"}, Now: now})
	token := int64(must(raft.CommandCreateToken, raft.CreateTokenPayload{Token: raft.TokenEntry{Name: "bench", Permissions: "read",
		TokenHash: "bench-hash", TokenPrefix: "bench-prefix", CreatedAtUnixNano: now.UnixNano(), Enabled: true}}))
	epoch := must(raft.CommandCollectPolicies, raft.CollectPoliciesPayload{Kind: raft.PolicyKindGovernance, Leader: "w1", Now: now})
	must(raft.CommandSeedPolicies, raft.SeedPoliciesPayload{Kind: raft.PolicyKindGovernance, Epoch: epoch, GovernanceFloor: 1,
		Governance: []raft.GovernancePolicyEntry{{TokenID: token, MaxRowsPerQuery: 1}}, Now: now})
	// Called from the apply goroutine: Error, not Fatal.
	return func(i int) {
		if _, res := apply(raft.CommandSetGovernancePolicy, raft.SetGovernancePolicyPayload{TokenID: token, MaxRowsPerQuery: i%1000 + 1, Now: now}); res != nil {
			b.Error(res)
		}
	}
}
