package tiering

import (
	"context"
	"errors"
	"fmt"
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
	// Collecting reports whether the cluster is reading every node's own
	// policies to take them over: a local write now would be missed.
	Collecting() bool
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

// ErrPoliciesBeingAdopted: a write while the cluster takes over the nodes'
// own policies. It lasts seconds; retry.
var ErrPoliciesBeingAdopted = errors.New("tiering policies are being adopted by the cluster; retry in a few seconds")

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

// localWriteLocked decides a local write with s.mu held: the cluster's copy
// when it now holds the policies, a refusal while it is collecting them, or
// neither (write the local table).
func (s *PolicyStore) localWriteLocked() (ClusterPolicySource, error) {
	h := s.cluster.Load()
	if h == nil {
		return nil, nil
	}
	if h.src.Seeded() {
		return h.src, nil
	}
	if h.src.Collecting() {
		return nil, ErrPoliciesBeingAdopted
	}
	return nil, nil
}

func (s *PolicyStore) setInCluster(ctx context.Context, src ClusterPolicySource, policy *DatabasePolicy) error {
	if err := src.Set(ctx, policy); err != nil {
		return fmt.Errorf("failed to set policy: %w", err)
	}
	s.logger.Info().Str("database", policy.Database).Bool("hot_only", policy.HotOnly).
		Msg("Tiering policy updated cluster-wide")
	return nil
}

func (s *PolicyStore) deleteInCluster(ctx context.Context, src ClusterPolicySource, database string) error {
	if err := src.Delete(ctx, database); err != nil {
		if errors.Is(err, ErrPolicyNotFound) {
			return fmt.Errorf("policy not found for database: %s: %w", database, ErrPolicyNotFound)
		}
		return fmt.Errorf("failed to delete policy: %w", err)
	}
	s.logger.Info().Str("database", database).
		Msg("Tiering policy deleted cluster-wide, reverting to global defaults")
	return nil
}

// LocalPolicies returns this node's own rows, for the cluster to take over
// (#1324). It reads under the lock local writes take, after this node has
// applied the collect: a write either finished before the read, or sees the
// collect and is refused.
func (s *PolicyStore) LocalPolicies(ctx context.Context) ([]DatabasePolicy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listFromDB(ctx)
}
