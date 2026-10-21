package cluster

import (
	"context"
	"encoding/json"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/tiering"
)

// TieringPolicySource adapts the coordinator's tiering policies to
// tiering.ClusterPolicySource (#1324).
type TieringPolicySource struct{ c *Coordinator }

// NewTieringPolicySource returns the cluster's copy of the tiering policies,
// or nil without Raft (a standalone node keeps its local table).
func NewTieringPolicySource(c *Coordinator) tiering.ClusterPolicySource {
	if c == nil || c.raftNode == nil {
		return nil
	}
	return &TieringPolicySource{c: c}
}

// Seeded implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Seeded() bool { return s.c.PoliciesSeeded(raft.PolicyKindTiering) }

// Collecting implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Collecting() bool {
	return s.c.PolicyCollecting(raft.PolicyKindTiering)
}

// Get implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Get(database string) (*tiering.DatabasePolicy, bool) {
	e, ok := s.c.TieringPolicy(database)
	if !ok {
		return nil, false
	}
	p := tieringPolicyFromEntry(e)
	return &p, true
}

// List implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) List() []tiering.DatabasePolicy {
	entries := s.c.TieringPolicies()
	out := make([]tiering.DatabasePolicy, 0, len(entries))
	for _, e := range entries {
		out = append(out, tieringPolicyFromEntry(e))
	}
	return out
}

// Set implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Set(ctx context.Context, p *tiering.DatabasePolicy) error {
	return s.c.SetTieringPolicy(ctx, p.Database, p.HotOnly, p.HotMaxAgeDays)
}

// Delete implements tiering.ClusterPolicySource.
func (s *TieringPolicySource) Delete(ctx context.Context, database string) error {
	err := s.c.DeleteTieringPolicy(ctx, database)
	if raft.IsPolicyNotFound(err) {
		return tiering.ErrPolicyNotFound
	}
	return err
}

func tieringPolicyFromEntry(e raft.TieringPolicyEntry) tiering.DatabasePolicy {
	return tiering.DatabasePolicy{
		ID: e.ID, Database: e.Database, HotOnly: e.HotOnly, HotMaxAgeDays: e.HotMaxAgeDays,
		CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt,
	}
}

// TieringLocalLister reads this node's own tiering policies for adoption,
// under the store's write lock (tiering.PolicyStore.LocalPolicies).
func TieringLocalLister(store *tiering.PolicyStore) LocalPolicyLister {
	return func(ctx context.Context) (json.RawMessage, error) {
		policies, err := store.LocalPolicies(ctx)
		if err != nil {
			return nil, err
		}
		entries := make([]raft.TieringPolicyEntry, 0, len(policies))
		for _, p := range policies {
			entries = append(entries, raft.TieringPolicyEntry{
				Database: p.Database, HotOnly: p.HotOnly, HotMaxAgeDays: p.HotMaxAgeDays,
				CreatedAt: p.CreatedAt, UpdatedAt: p.UpdatedAt,
			})
		}
		return json.Marshal(entries)
	}
}
