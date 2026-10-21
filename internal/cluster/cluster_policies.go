package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Cluster policies, coordinator side (#1324). See raft/cluster_policies.go.
//
// The flag follows the compaction commit pattern (compaction_commit.go): every
// build that applies the policy commands advertises the capability, the
// leader proposes the flag when every node in the FSM table and every Raft
// server advertised it, and once it is on (or may be being proposed) a join
// without it is refused before the node enters the Raft configuration.

const clusterPoliciesEnableEvery = 10 * time.Second

// clusterPoliciesGate serializes the join gate with the enable proposal.
// inFlight (under mu) is set while an enable proposal may still commit (one
// that timed out): until the flag is seen, a join without the capability
// waits for a barrier first.
type clusterPoliciesGate struct {
	mu       sync.Mutex
	inFlight bool
	blockers atomic.Pointer[[]string]
}

// clusterPoliciesJoinGate is compactionCommitJoinGate for the cluster
// policies flag.
func (c *Coordinator) clusterPoliciesJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := &c.clusterPolicies
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), raft.CapabilityClusterPolicies) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && fsm.ClusterPoliciesEnabled()
	if !enabled && g.inFlight {
		// An enable proposal that timed out may still commit: a barrier
		// appended after it applies it here before the flag is trusted off.
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its cluster policies state; retry the join", nil
		}
		enabled = fsm != nil && fsm.ClusterPoliciesEnabled()
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "cluster policies are enabled in this cluster: the node must run a release that supports them", nil
}

// clusterPoliciesEnableLoop proposes the flag from the leader once every node
// supports it.
func (c *Coordinator) clusterPoliciesEnableLoop(ctx context.Context) {
	t := time.NewTicker(clusterPoliciesEnableEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.maybeEnableClusterPolicies()
			c.maybeSeedPolicies(ctx)
		}
	}
}

// maybeEnableClusterPolicies is one pass of the enable loop.
func (c *Coordinator) maybeEnableClusterPolicies() {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || fsm.ClusterPoliciesEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.clusterPolicies
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityClusterPolicies)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableClusterPoliciesPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	// Until the outcome is known the proposal may commit later: joins
	// without the capability barrier first.
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableClusterPolicies, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing cluster policies failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Cluster policies enabled: every node supports them")
}

// ClusterPoliciesActive reports whether the flag is applied here.
func (c *Coordinator) ClusterPoliciesActive() bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.ClusterPoliciesEnabled()
}

// ClusterPoliciesStatus is the /cluster status view.
type ClusterPoliciesStatus struct {
	Enabled  bool                        `json:"enabled"`
	Blocking []string                    `json:"blocking_nodes,omitempty"`
	Kinds    map[string]PolicyKindStatus `json:"kinds,omitempty"`
}

// PolicyKindStatus is one kind's adoption: seeded (the cluster holds it),
// collecting (local writes refused for seconds), or awaiting adoption with
// the nodes that did not answer the last collect.
type PolicyKindStatus struct {
	Seeded     bool      `json:"seeded"`
	Collecting bool      `json:"collecting,omitempty"`
	Missing    []string  `json:"missing_nodes,omitempty"`
	LastError  string    `json:"last_error,omitempty"`
	Attempts   int       `json:"failed_attempts,omitempty"`
	LastTry    time.Time `json:"last_attempt,omitempty"`
}

// ClusterPoliciesStatus reports the flag and, on the leader, the nodes keeping
// it off.
func (c *Coordinator) ClusterPoliciesStatus() ClusterPoliciesStatus {
	var st ClusterPoliciesStatus
	if c.raftNode == nil {
		return st
	}
	st.Enabled = c.ClusterPoliciesActive()
	if b := c.clusterPolicies.blockers.Load(); b != nil && !st.Enabled && c.raftNode.IsLeader() {
		st.Blocking = *b
	}
	if fsm := c.fsmOrNil(); fsm != nil && st.Enabled {
		st.Kinds = map[string]PolicyKindStatus{}
		for _, kind := range policyKinds {
			k := PolicyKindStatus{Seeded: fsm.PoliciesSeeded(kind)}
			_, k.Collecting = fsm.PolicyCollecting(kind)
			if a, ok := fsm.PolicyAwaitingAdoption(kind); ok {
				k.Missing, k.LastError, k.Attempts, k.LastTry = a.Missing, a.LastError, a.Attempts, a.At
			}
			st.Kinds[kind] = k
		}
	}
	return st
}

// policyProposeTimeout bounds one policy write, as proposeTimeout does for
// auth writes.
const policyProposeTimeout = 5 * time.Second

// proposePolicy applies a policy command on the leader, or forwards it and
// waits until this node has applied it, so the caller reads its own write.
func (c *Coordinator) proposePolicy(ctx context.Context, cmdType raft.CommandType, payload interface{}) error {
	if c == nil || c.raftNode == nil {
		return errors.New("cluster policies: raft not initialized")
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	cmd := &raft.Command{Type: cmdType, Payload: raw}
	if c.raftNode.IsLeader() {
		return c.raftNode.Apply(cmd, policyProposeTimeout)
	}
	ctx, cancel := context.WithTimeout(ctx, policyProposeTimeout)
	defer cancel()
	return c.forwardAndWaitHere(ctx, cmd)
}

// PoliciesSeeded reports whether kind's policies live in the cluster state.
// Until then they stay in each node's SQLite, as before #1324.
func (c *Coordinator) PoliciesSeeded(kind string) bool {
	if c == nil || c.raftNode == nil {
		return false
	}
	fsm := c.raftNode.FSM()
	return fsm != nil && fsm.ClusterPoliciesEnabled() && fsm.PoliciesSeeded(kind)
}

// TieringPolicy returns database's tiering policy from the cluster state.
func (c *Coordinator) TieringPolicy(database string) (raft.TieringPolicyEntry, bool) {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.TieringPolicy(database)
	}
	return raft.TieringPolicyEntry{}, false
}

// TieringPolicies returns every tiering policy in the cluster state.
func (c *Coordinator) TieringPolicies() []raft.TieringPolicyEntry {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.TieringPolicies()
	}
	return nil
}

// SetTieringPolicy creates or replaces database's tiering policy cluster-wide.
func (c *Coordinator) SetTieringPolicy(ctx context.Context, database string, hotOnly bool, hotMaxAgeDays *int) error {
	return c.proposePolicy(ctx, raft.CommandSetTieringPolicy, raft.SetTieringPolicyPayload{
		Database: database, HotOnly: hotOnly, HotMaxAgeDays: hotMaxAgeDays, Now: time.Now().UTC(),
	})
}

// DeleteTieringPolicy removes database's tiering policy cluster-wide. A
// missing policy is raft.ErrPolicyNotFound (raft.IsPolicyNotFound).
func (c *Coordinator) DeleteTieringPolicy(ctx context.Context, database string) error {
	return c.proposePolicy(ctx, raft.CommandDeleteTieringPolicy, raft.DeleteTieringPolicyPayload{Database: database})
}

func (c *Coordinator) fsmOrNil() *raft.ClusterFSM {
	if c == nil || c.raftNode == nil {
		return nil
	}
	return c.raftNode.FSM()
}
