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

// ErrInvalidPolicy: the cluster refused the policy itself (a database name
// too long, say). Not worth retrying.
var ErrInvalidPolicy = errors.New("invalid tiering policy")

// ErrClusterUnavailable: the cluster could not take the write right now (no
// leader, leadership changing, the leader unreachable). Retry.
var ErrClusterUnavailable = errors.New("the cluster could not take the tiering policy write right now; retry")

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
	// Collecting first: a seed ends the collect and marks the kind seeded in
	// one apply, so if it lands between the two reads, Seeded sees it. In the
	// other order the write could read "not seeded", then "not collecting",
	// and land in this table after the seed, where nothing reads it.
	if h.src.Collecting() {
		return nil, ErrPoliciesBeingAdopted
	}
	if h.src.Seeded() {
		return h.src, nil
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

// MirrorClusterPolicies replaces this node's table with the cluster's
// policies (#1324). The cluster's copy is the one every read uses once seeded;
// the table is kept equal to it so a backup of this node's database, and a
// restore of it onto a standalone node or a fresh cluster, carries the
// current policies.
func (s *PolicyStore) MirrorClusterPolicies(ctx context.Context, policies []DatabasePolicy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("mirror tiering policies: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM tiering_policies`); err != nil {
		return fmt.Errorf("mirror tiering policies: %w", err)
	}
	for _, p := range policies {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO tiering_policies (database, hot_only, hot_max_age_days, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`,
			p.Database, p.HotOnly, p.HotMaxAgeDays, p.CreatedAt.UTC(), p.UpdatedAt.UTC()); err != nil {
			return fmt.Errorf("mirror tiering policies: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("mirror tiering policies: %w", err)
	}
	// The cache is the local table's; keep it equal too.
	s.cache = make(map[string]*DatabasePolicy, len(policies))
	for i := range policies {
		p := policies[i]
		s.cache[p.Database] = &p
	}
	s.gen++
	return nil
}
