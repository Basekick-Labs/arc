package raft

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// Cluster policies (#1324).
//
// Tiering policies, retention policies and continuous queries were stored
// only in the SQLite of the node that took the API call, while only the
// primary writer runs them. A policy created anywhere else never ran, and a
// failover left the new primary with none of them. With the flag on, the
// definitions live here: every node materialises them into its own SQLite,
// and execution stays primary-only.
//
// The flag is applied only when every node supports it: an older build skips
// the new commands and drops the new snapshot field, so its policies would
// differ from every other node's.

// CapabilityClusterPolicies is advertised at join by every build that applies
// the policy commands.
const CapabilityClusterPolicies = "cluster_policies_v1"

// clusterPoliciesState is the snapshot form of the flag and the policies. nil
// in an older snapshot, or before the flag was applied.
type clusterPoliciesState struct {
	Enabled bool `json:"enabled"`
}

// EnableClusterPoliciesPayload turns the flag on; see
// EnableStreamClaimsPayload for the Nodes rule.
type EnableClusterPoliciesPayload struct {
	Nodes []string  `json:"nodes"`
	Now   time.Time `json:"now"`
}

// ClusterPoliciesEnabled reports whether the flag is applied.
func (f *ClusterFSM) ClusterPoliciesEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.clusterPolicies
}

func (f *ClusterFSM) applyEnableClusterPolicies(payload []byte) interface{} {
	var p EnableClusterPoliciesPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal enable cluster policies payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.clusterPolicies {
		return nil
	}
	listed := map[string]bool{}
	for _, n := range p.Nodes {
		listed[n] = true
	}
	var missing []string
	for id := range f.nodes {
		if !listed[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("enable cluster policies: nodes %v are not known to support it", missing)
	}
	f.clusterPolicies = true
	f.logger.Info().Int("nodes", len(f.nodes)).Msg("Cluster policies enabled")
	return nil
}

func (f *ClusterFSM) snapshotClusterPoliciesLocked() *clusterPoliciesState {
	if !f.clusterPolicies {
		return nil
	}
	return &clusterPoliciesState{Enabled: true}
}

func (f *ClusterFSM) restoreClusterPoliciesLocked(st *clusterPoliciesState) {
	f.clusterPolicies = st != nil && st.Enabled
}
