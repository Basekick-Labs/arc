package tiering

import (
	"context"
	"errors"
)

// ClusterPolicySource is the cluster's copy of the tiering policies (#1324).
//
// In a cluster every node used to keep its own tiering_policies rows, while
// only the primary writer migrates: a policy set on any other node never
// applied, and a failover lost them. Once the cluster has seeded its tiering
// policies (Seeded), the store reads and writes them here instead of the
// local table, so every node sees the same policies. Until then, and on a
// standalone node, the local table is used exactly as before.
type ClusterPolicySource interface {
	// Seeded reports whether the cluster holds the tiering policies.
	Seeded() bool
	// Get returns database's policy.
	Get(database string) (*DatabasePolicy, bool)
	// List returns every policy.
	List() []DatabasePolicy
	// Set creates or replaces a policy cluster-wide and returns once this
	// node sees it.
	Set(ctx context.Context, policy *DatabasePolicy) error
	// Delete removes a policy cluster-wide; ErrPolicyNotFound when there is
	// none.
	Delete(ctx context.Context, database string) error
}

// ErrPolicyNotFound: a delete of a database with no custom policy.
var ErrPolicyNotFound = errors.New("policy not found")

type clusterSourceHolder struct{ src ClusterPolicySource }

// SetClusterSource installs the cluster's copy of the policies. nil removes
// it (standalone).
func (s *PolicyStore) SetClusterSource(src ClusterPolicySource) {
	if src == nil {
		s.cluster.Store(nil)
		return
	}
	s.cluster.Store(&clusterSourceHolder{src: src})
}

// clusterSource returns the cluster's copy when it holds the policies, nil
// otherwise (then the local table applies).
func (s *PolicyStore) clusterSource() ClusterPolicySource {
	h := s.cluster.Load()
	if h == nil || !h.src.Seeded() {
		return nil
	}
	return h.src
}
