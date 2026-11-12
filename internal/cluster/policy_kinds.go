package cluster

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Policy kinds with a flag of their own (#1324 stages 2 and 3, #1627).
//
// Tiering needs only the cluster policies flag. Every other kind also has a
// flag and a capability of its own: a build that knows the cluster policies
// but not that kind would refuse its commands and drop its state from its
// snapshots, so the kind is enabled only once no such build can be in the
// cluster, and from then on such a build cannot join. One table drives that
// for every kind: the enable pass, the join gate, whether the kind is active
// and the error an admin's choice gets before it is.

// policyKindSpec is one kind with a flag of its own.
type policyKindSpec struct {
	kind string
	// capability is what a build that applies the kind's commands
	// advertises at join.
	capability string
	// gate is the kind's join gate state on the coordinator.
	gate func(c *Coordinator) *clusterPoliciesGate
	// enabled reads the kind's flag (the cluster policies flag included).
	enabled func(fsm *raft.ClusterFSM) bool
	// enableCmd turns the flag on; its payload is raft.EnablePolicyKindPayload.
	enableCmd raft.CommandType
	// notEnabled is the API's error before the flag is on.
	notEnabled error
	// joinRefused, unconfirmed: the join gate's refusals, once the flag is
	// on and while an enable proposal may still commit without a barrier.
	joinRefused, unconfirmed string
	// proposeFailed, enabledMsg: the enable pass's log lines.
	proposeFailed, enabledMsg string
}

// ErrGovernancePoliciesNotEnabled: cluster-wide governance policies are not on
// yet.
var ErrGovernancePoliciesNotEnabled = errors.New("cluster-wide governance policies are not enabled yet: every node must run a release that supports them")

// policyKindSpecs, in the order their join gates are taken.
//
// Governance's capability is not advertised by this build
// (localCapabilities) and it is not in policyKinds: its flag cannot be turned
// on, and nothing adopts it, until the build that reads the cluster's
// governance policies (#1627 part 2) advertises it. Its join gate is in place
// so that once it is on no build without it can join.
var policyKindSpecs = []policyKindSpec{
	{
		kind:          raft.PolicyKindRetention,
		capability:    raft.CapabilityClusterRetention,
		gate:          func(c *Coordinator) *clusterPoliciesGate { return &c.retentionPolicies },
		enabled:       (*raft.ClusterFSM).RetentionPoliciesEnabled,
		enableCmd:     raft.CommandEnableRetentionPolicies,
		notEnabled:    ErrRetentionPoliciesNotEnabled,
		joinRefused:   "cluster-wide retention policies are enabled in this cluster: the node must run a release that supports them",
		unconfirmed:   "the cluster could not confirm its retention policies state; retry the join",
		proposeFailed: "Proposing cluster-wide retention policies failed; retrying",
		enabledMsg:    "Cluster-wide retention policies enabled: every node supports them",
	},
	{
		kind:          raft.PolicyKindCQ,
		capability:    raft.CapabilityClusterCQ,
		gate:          func(c *Coordinator) *clusterPoliciesGate { return &c.cqPolicies },
		enabled:       (*raft.ClusterFSM).CQPoliciesEnabled,
		enableCmd:     raft.CommandEnableCQPolicies,
		notEnabled:    ErrCQPoliciesNotEnabled,
		joinRefused:   "cluster-wide continuous queries are enabled in this cluster: the node must run a release that supports them",
		unconfirmed:   "the cluster could not confirm its continuous query policies state; retry the join",
		proposeFailed: "Proposing cluster-wide continuous queries failed; retrying",
		enabledMsg:    "Cluster-wide continuous queries enabled: every node supports them",
	},
	{
		kind:          raft.PolicyKindGovernance,
		capability:    raft.CapabilityClusterGovernance,
		gate:          func(c *Coordinator) *clusterPoliciesGate { return &c.governancePolicies },
		enabled:       (*raft.ClusterFSM).GovernancePoliciesEnabled,
		enableCmd:     raft.CommandEnableGovernancePolicies,
		notEnabled:    ErrGovernancePoliciesNotEnabled,
		joinRefused:   "cluster-wide governance policies are enabled in this cluster: the node must run a release that supports them",
		unconfirmed:   "the cluster could not confirm its governance policies state; retry the join",
		proposeFailed: "Proposing cluster-wide governance policies failed; retrying",
		enabledMsg:    "Cluster-wide governance policies enabled: every node supports them",
	},
}

// policyKindSpecFor returns kind's spec, or nil for tiering or an unknown
// kind.
func policyKindSpecFor(kind string) *policyKindSpec {
	for i := range policyKindSpecs {
		if policyKindSpecs[i].kind == kind {
			return &policyKindSpecs[i]
		}
	}
	return nil
}

// policyKindJoinGate is clusterPoliciesJoinGate for one kind's own flag. On
// success it returns holding the kind's gate lock; release drops it.
func (c *Coordinator) policyKindJoinGate(s *policyKindSpec, req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := s.gate(c)
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), s.capability) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && s.enabled(fsm)
	if !enabled && g.inFlight {
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return s.unconfirmed, nil
		}
		enabled = fsm != nil && s.enabled(fsm)
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return s.joinRefused, nil
}

// policyKindsJoinGate takes every kind's join gate, in table order. A refusal
// releases the gates already taken, in reverse; on success release drops them
// all, in reverse.
func (c *Coordinator) policyKindsJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	releases := make([]func(), 0, len(policyKindSpecs))
	releaseAll := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	for i := range policyKindSpecs {
		why, rel := c.policyKindJoinGate(&policyKindSpecs[i], req)
		if why != "" {
			releaseAll()
			return why, nil
		}
		releases = append(releases, rel)
	}
	return "", releaseAll
}

// maybeEnablePolicyKind proposes kind's own flag from the leader once every
// node supports it.
func (c *Coordinator) maybeEnablePolicyKind(s *policyKindSpec) {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.ClusterPoliciesEnabled() || s.enabled(fsm) || !c.raftNode.IsLeader() {
		return
	}
	g := s.gate(c)
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, s.capability)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnablePolicyKindPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: s.enableCmd, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg(s.proposeFailed)
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg(s.enabledMsg)
}

// maybeEnablePolicyKinds is one enable pass over every kind's own flag.
func (c *Coordinator) maybeEnablePolicyKinds() {
	for i := range policyKindSpecs {
		c.maybeEnablePolicyKind(&policyKindSpecs[i])
	}
}

// maybeEnableRetentionPolicies / maybeEnableCQPolicies: one kind's pass. No
// production caller (the loop runs maybeEnablePolicyKinds); kept for the
// retention and CQ tests, which predate the table.
func (c *Coordinator) maybeEnableRetentionPolicies() {
	c.maybeEnablePolicyKind(policyKindSpecFor(raft.PolicyKindRetention))
}

func (c *Coordinator) maybeEnableCQPolicies() {
	c.maybeEnablePolicyKind(policyKindSpecFor(raft.PolicyKindCQ))
}

// policyKindActive reports whether kind can be adopted here: the cluster
// policies flag, and for a kind with a flag of its own that flag too. An
// unknown kind never is.
func (c *Coordinator) policyKindActive(kind string) bool {
	fsm := c.fsmOrNil()
	if fsm == nil || !fsm.ClusterPoliciesEnabled() {
		return false
	}
	if kind == raft.PolicyKindTiering {
		return true
	}
	if s := policyKindSpecFor(kind); s != nil {
		return s.enabled(fsm)
	}
	return false
}

// policyKindNotEnabled is the error an admin's choice of kind gets before the
// kind is active. A kind outside the table keeps the retention error it got
// before the table; the API validates the kind before it gets here.
func policyKindNotEnabled(kind string) error {
	if s := policyKindSpecFor(kind); s != nil {
		return s.notEnabled
	}
	return ErrRetentionPoliciesNotEnabled
}
