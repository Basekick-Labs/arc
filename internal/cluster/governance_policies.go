package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/governance"
)

// Query governance policies, cluster-wide (#1627): the coordinator side. See
// raft/governance_policies.go and governance/cluster.go.

// GovernancePoliciesSeeded reports whether governance policies live in the
// cluster state.
func (c *Coordinator) GovernancePoliciesSeeded() bool {
	return c.policyKindActive(raft.PolicyKindGovernance) && c.PoliciesSeeded(raft.PolicyKindGovernance)
}

// UseGovernance makes m's policies cluster-wide once the cluster adopts them:
// m reads the cluster's view, rebuilt on the FSM goroutine after every policy
// change; m's writes are proposed to the cluster state; m's table answers the
// adoption and is kept as the cluster's backup copy.
func (c *Coordinator) UseGovernance(m *governance.Manager) {
	fsm := c.fsmOrNil()
	if fsm == nil || m == nil {
		return
	}
	m.SetClusterSource(&governancePolicySource{c: c})
	// The view is rebuilt from the FSM's own state: only memory, no lock
	// but the build's own, so the callback never blocks the FSM.
	fsm.AddPoliciesChangedCallback(func() { m.SyncClusterView(fsm) })
	m.SyncClusterView(fsm)
	c.SetLocalPolicyLister(raft.PolicyKindGovernance, func(ctx context.Context) (json.RawMessage, error) {
		rows, err := m.LocalPolicyRows(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(rows)
	})
	c.RunPolicyMirror(raft.PolicyKindGovernance, func(ctx context.Context) error {
		return m.MirrorClusterPolicies(ctx, fsm)
	})
}

// GovernanceViewSource returns the cluster state governance policies are read
// from, when this node has one (a coordinator without Raft has none).
func (c *Coordinator) GovernanceViewSource() (governance.ClusterViewSource, bool) {
	fsm := c.fsmOrNil()
	if fsm == nil {
		return nil, false
	}
	return fsm, true
}

// governancePolicySource is the cluster state governance reads and writes.
type governancePolicySource struct{ c *Coordinator }

func (s *governancePolicySource) Seeded() bool { return s.c.GovernancePoliciesSeeded() }
func (s *governancePolicySource) Collecting() bool {
	return s.c.PolicyCollecting(raft.PolicyKindGovernance)
}

func (s *governancePolicySource) Get(tokenID int64) (*governance.Policy, bool) {
	fsm := s.c.fsmOrNil()
	if fsm == nil {
		return nil, false
	}
	e, ok := fsm.GovernancePolicy(tokenID)
	if !ok {
		return nil, false
	}
	return governance.PolicyFromEntry(e), true
}

// checkClusterToken refuses a write for a token that is not cluster token id:
// no such cluster token, or a different token at that ID (a node's
// pre-cluster local token). Checked here, against this node's cluster state;
// a rotate in between keeps the token, and a delete is refused by the FSM.
func (s *governancePolicySource) checkClusterToken(id int64, tokenFP string) error {
	fsm := s.c.fsmOrNil()
	if fsm == nil {
		return governance.ErrClusterUnavailable
	}
	hash, ok := fsm.GovernanceTokenHash(id)
	if !ok || tokenFP == "" || auth.TokenFingerprint(hash) != tokenFP {
		return fmt.Errorf("%w: token %d is not a cluster token", governance.ErrTokenNotCluster, id)
	}
	return nil
}

func (s *governancePolicySource) Set(ctx context.Context, p *governance.Policy, create bool, tokenFP string) error {
	if err := s.checkClusterToken(p.TokenID, tokenFP); err != nil {
		return err
	}
	return classifyGovernanceError(s.c.proposePolicy(ctx, raft.CommandSetGovernancePolicy, raft.SetGovernancePolicyPayload{
		TokenID: p.TokenID, Create: create,
		RateLimitPerMinute: p.RateLimitPerMinute, RateLimitPerHour: p.RateLimitPerHour,
		MaxQueriesPerHour: p.MaxQueriesPerHour, MaxQueriesPerDay: p.MaxQueriesPerDay,
		MaxRowsPerQuery: p.MaxRowsPerQuery, MaxScanDurationSec: p.MaxScanDurationSec,
		Now: time.Now().UTC(),
	}))
}

func (s *governancePolicySource) Delete(ctx context.Context, tokenID int64, tokenFP string) error {
	if err := s.checkClusterToken(tokenID, tokenFP); err != nil {
		return err
	}
	return classifyGovernanceError(s.c.proposePolicy(ctx, raft.CommandDeleteGovernancePolicy, raft.DeleteGovernancePolicyPayload{TokenID: tokenID}))
}

var governanceWriteErrors = policyWriteErrors{
	notFound: governance.ErrPolicyNotFound, invalid: governance.ErrInvalidPolicy,
	unavailable: governance.ErrClusterUnavailable, text: "governance policy",
}

// classifyGovernanceError sorts a governance write's error for the API: the
// governance-only refusals first, then as every kind's.
func classifyGovernanceError(err error) error {
	switch {
	case err == nil:
		return nil
	case raft.IsGovernancePolicyExists(err):
		return fmt.Errorf("%w: %v", governance.ErrPolicyExists, err)
	case raft.IsGovernanceTokenNotCluster(err):
		return fmt.Errorf("%w: %v", governance.ErrTokenNotCluster, err)
	}
	return classifyPolicyErrorFor(err, governanceWriteErrors)
}

// --- Adoption ---

// GovernanceRowsHash identifies a node's governance definitions (IDs and
// limits; timestamps and token reports not): an admin's choice names it.
func GovernanceRowsHash(rows []governance.LocalRow) string {
	type def struct {
		ID, Token          int64
		M, H, QH, QD, R, S int
	}
	defs := make([]def, 0, len(rows))
	for _, r := range rows {
		defs = append(defs, def{r.ID, r.TokenID, r.RateLimitPerMinute, r.RateLimitPerHour, r.MaxQueriesPerHour, r.MaxQueriesPerDay, r.MaxRowsPerQuery, r.MaxScanDurationSec})
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].ID < defs[j].ID })
	raw, _ := json.Marshal(defs)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func decodeGovernanceRows(raw json.RawMessage) (governance.LocalRows, error) {
	var r governance.LocalRows
	if len(raw) == 0 || string(raw) == "null" {
		return r, nil
	}
	err := json.Unmarshal(raw, &r)
	return r, err
}

// governanceLimits is a policy's definition without its ID or timestamps:
// two nodes' rows for one token with the same limits are the same policy.
type governanceLimits struct{ m, h, qh, qd, r, s int }

func limitsOf(r governance.LocalRow) governanceLimits {
	return governanceLimits{r.RateLimitPerMinute, r.RateLimitPerHour, r.MaxQueriesPerHour, r.MaxQueriesPerDay, r.MaxRowsPerQuery, r.MaxScanDurationSec}
}

// adoptChoiceHint names the route an admin chooses one node's policies with.
const adoptChoiceHint = `POST /api/v1/cluster/policies/adopt with "kind": "governance"`

// planGovernanceSeed decides a governance seed from every node's rows, or why
// none can be made yet. clusterToken returns the fingerprint of cluster token
// id's stored hash. warnings are worth logging when the seed is applied.
//
// A row is seeded only for the cluster token it names: its node holds that
// token as the cluster's (materialised from the cluster state) with the
// cluster token's hash. A row is dropped only when its token is certainly
// gone: absent on its node, no other node reporting it, and not in the
// cluster state; such a policy limits no one today. Any other doubt waits:
// a token the node cannot report, a token that is a different one than the
// cluster's at that ID (a pre-cluster local token), a cluster token not yet
// on the node. Two nodes defining one token differently wait for an admin's
// choice of one node's policies. IDs are kept or renumbered as retention's.
func planGovernanceSeed(perNode map[string]governance.LocalRows, choice *raft.PolicyAdoptChoice, clusterToken func(id int64) (string, time.Time, bool)) (seed raft.SeedPoliciesPayload, why string, warnings []string) {
	var maxID int64
	issued := make(map[string]int64, len(perNode))
	reported := map[int64]string{} // token -> a node that holds it
	nodes := make([]string, 0, len(perNode))
	for id, n := range perNode {
		nodes = append(nodes, id)
		issued[id] = n.MaxIssuedID
		for _, r := range n.Policies {
			issued[id] = max(issued[id], r.ID)
			if r.Token == governance.TokenPresent {
				reported[r.TokenID] = id
			}
		}
		maxID = max(maxID, issued[id])
	}
	sort.Strings(nodes)
	considered := nodes
	if choice != nil {
		rows, ok := perNode[choice.Node]
		if !ok {
			return seed, fmt.Sprintf("the chosen node %s did not answer", choice.Node), nil
		}
		if GovernanceRowsHash(rows.Policies) != choice.Hash {
			return seed, fmt.Sprintf("the governance policies on %s changed since they were chosen; look again and choose again", choice.Node), nil
		}
		considered = []string{choice.Node}
	}
	type acc struct {
		row     governance.LocalRow
		limits  governanceLimits
		sources []raft.RetentionSource
	}
	byToken := map[int64]*acc{}
	for _, node := range considered {
		for _, r := range perNode[node].Policies {
			fp, created, inCluster := clusterToken(r.TokenID)
			switch r.Token {
			case governance.TokenPresent:
				if !inCluster || !r.RaftMaterialised || r.TokenFP != fp {
					return seed, fmt.Sprintf("node %s holds a governance policy for token %d, which is not the cluster's token %d there (a token created on that node before it joined the cluster); re-issue the token through the cluster, delete that policy on %s, or choose another node's policies (%s)", node, r.TokenID, r.TokenID, node, adoptChoiceHint), nil
				}
				// A policy older than the cluster token was set for an
				// earlier token at that ID (a local token since deleted,
				// whose policy stayed): never the cluster token's.
				if r.CreatedAt.IsZero() || r.CreatedAt.Before(created.Add(-policyClockSkew)) {
					return seed, fmt.Sprintf("node %s holds a governance policy for token %d created before cluster token %d existed (it was set for an earlier token at that ID); delete that policy on %s, or choose another node's policies (%s)", node, r.TokenID, r.TokenID, node, adoptChoiceHint), nil
				}
			case governance.TokenAbsent:
				if inCluster {
					return seed, fmt.Sprintf("cluster token %d is not on node %s yet; retrying (if it never arrives there, choose another node's policies: %s)", r.TokenID, node, adoptChoiceHint), nil
				}
				if other, ok := reported[r.TokenID]; ok {
					return seed, fmt.Sprintf("node %s holds a governance policy for token %d, which only node %s has and the cluster does not; re-issue the token through the cluster, delete those policies, or choose another node's policies (%s)", node, r.TokenID, other, adoptChoiceHint), nil
				}
				warnings = append(warnings, fmt.Sprintf("dropped node %s's governance policy %d: its token %d no longer exists", node, r.ID, r.TokenID))
				continue
			default:
				return seed, fmt.Sprintf("node %s cannot tell whether token %d exists (it has no token table); choose another node's policies (%s)", node, r.TokenID, adoptChoiceHint), nil
			}
			if err := validLimits(r); err != nil {
				return seed, fmt.Sprintf("governance policy %d on %s cannot be adopted (%v); fix it on that node first", r.ID, node, err), nil
			}
			a, ok := byToken[r.TokenID]
			if !ok {
				a = &acc{row: r, limits: limitsOf(r)}
				byToken[r.TokenID] = a
			} else if a.limits != limitsOf(r) {
				return seed, fmt.Sprintf("the nodes hold different governance policies for token %d; choose one node's policies", r.TokenID), nil
			}
			a.sources = append(a.sources, raft.RetentionSource{Node: node, ID: r.ID})
			if !r.CreatedAt.IsZero() && (a.row.CreatedAt.IsZero() || r.CreatedAt.Before(a.row.CreatedAt)) {
				a.row.CreatedAt = r.CreatedAt
			}
			if r.UpdatedAt.After(a.row.UpdatedAt) {
				a.row.UpdatedAt = r.UpdatedAt
			}
		}
	}
	if len(byToken) > raft.MaxSeedGovernancePolicies {
		return seed, fmt.Sprintf("%d governance policies exceed the %d one seed installs", len(byToken), raft.MaxSeedGovernancePolicies), nil
	}
	tokens := make([]int64, 0, len(byToken))
	for t := range byToken {
		tokens = append(tokens, t)
	}
	sort.Slice(tokens, func(i, j int) bool { return tokens[i] < tokens[j] })
	seed = raft.SeedPoliciesPayload{GovernanceFloor: maxID + 1, GovernanceKeepIDs: true}
	sourceNode := ""
	kept := map[int64]bool{}
	for _, t := range tokens {
		a := byToken[t]
		if len(a.sources) != 1 || (sourceNode != "" && a.sources[0].Node != sourceNode) {
			seed.GovernanceKeepIDs = false
		}
		sourceNode = a.sources[0].Node
		kept[a.sources[0].ID] = true
	}
	if seed.GovernanceKeepIDs {
		for _, id := range nodes {
			if id == sourceNode {
				continue
			}
			for k := range kept {
				if k <= issued[id] {
					seed.GovernanceKeepIDs = false
				}
			}
		}
	}
	for _, t := range tokens {
		a := byToken[t]
		e := raft.GovernancePolicyEntry{
			TokenID: t, RateLimitPerMinute: a.row.RateLimitPerMinute, RateLimitPerHour: a.row.RateLimitPerHour,
			MaxQueriesPerHour: a.row.MaxQueriesPerHour, MaxQueriesPerDay: a.row.MaxQueriesPerDay,
			MaxRowsPerQuery: a.row.MaxRowsPerQuery, MaxScanDurationSec: a.row.MaxScanDurationSec,
			CreatedAt: a.row.CreatedAt, UpdatedAt: a.row.UpdatedAt,
		}
		if seed.GovernanceKeepIDs {
			e.ID = a.sources[0].ID
		}
		seed.Governance = append(seed.Governance, e)
	}
	if differ := differingDefaults(perNode); len(differ) > 0 {
		warnings = append(warnings, "governance.default_* differ between nodes "+strings.Join(differ, ", ")+": set them the same on every node")
	}
	return seed, "", warnings
}

func validLimits(r governance.LocalRow) error {
	for _, v := range []int{r.RateLimitPerMinute, r.RateLimitPerHour, r.MaxQueriesPerHour, r.MaxQueriesPerDay, r.MaxRowsPerQuery, r.MaxScanDurationSec} {
		if v < 0 {
			return errors.New("a negative limit")
		}
	}
	if r.TokenID <= 0 {
		return errors.New("no token")
	}
	return nil
}

// differingDefaults names the nodes whose governance.default_* differ from
// the first node's (by ID), when any do. Nodes without governance report none.
func differingDefaults(perNode map[string]governance.LocalRows) []string {
	nodes := make([]string, 0, len(perNode))
	for id, n := range perNode {
		if n.Defaults != nil {
			nodes = append(nodes, id)
		}
	}
	sort.Strings(nodes)
	if len(nodes) < 2 {
		return nil
	}
	first := *perNode[nodes[0]].Defaults
	var out []string
	for _, id := range nodes[1:] {
		if *perNode[id].Defaults != first {
			out = append(out, id)
		}
	}
	if len(out) > 0 {
		out = append([]string{nodes[0]}, out...)
	}
	return out
}

// clusterTokenFP is planGovernanceSeed's view of the cluster tokens: the
// fingerprint of cluster token id's stored hash, and when it was created.
func (c *Coordinator) clusterTokenFP(id int64) (string, time.Time, bool) {
	fsm := c.fsmOrNil()
	if fsm == nil {
		return "", time.Time{}, false
	}
	hash, created, ok := fsm.GovernanceToken(id)
	if !ok {
		return "", time.Time{}, false
	}
	return auth.TokenFingerprint(hash), created, true
}

// policyClockSkew is how far a policy's created_at, on its node's clock, may
// precede its cluster token's creation, on the proposer's clock, and still be
// taken as that token's.
const policyClockSkew = time.Minute

// mergeAnswers is mergePolicyAnswers with the cluster state the governance
// merge needs; logged says to log the merge's warnings (the collect's merge,
// not the probe's).
func (c *Coordinator) mergeAnswers(kind string, rows map[string]json.RawMessage, choice *raft.PolicyAdoptChoice, logged bool) (raft.SeedPoliciesPayload, error) {
	if kind != raft.PolicyKindGovernance {
		return mergePolicyAnswers(kind, rows, choice)
	}
	perNode := make(map[string]governance.LocalRows, len(rows))
	for id, raw := range rows {
		r, err := decodeGovernanceRows(raw)
		if err != nil {
			return raft.SeedPoliciesPayload{}, fmt.Errorf("node %s: %w", id, err)
		}
		perNode[id] = r
	}
	seed, why, warnings := planGovernanceSeed(perNode, choice, c.clusterTokenFP)
	if why != "" {
		return raft.SeedPoliciesPayload{}, errors.New(why)
	}
	if logged {
		for _, w := range warnings {
			c.logger.Warn().Str("kind", kind).Msg("Governance adoption: " + w)
		}
	}
	return seed, nil
}

// GovernanceCandidates is GET /api/v1/cluster/policies?kind=governance: each
// node's own governance policies, read live, with the hash an adoption choice
// names.
type GovernanceCandidates struct {
	Seeded   bool                         `json:"seeded"`
	Awaiting *raft.PolicyAwaiting         `json:"awaiting,omitempty"`
	Choice   *raft.PolicyAdoptChoice      `json:"pending_choice,omitempty"`
	Nodes    map[string]GovernanceNodeSet `json:"nodes,omitempty"`
}

// GovernanceNodeSet is one node's governance policies, or why they could not
// be read.
type GovernanceNodeSet struct {
	Error    string                `json:"error,omitempty"`
	Hash     string                `json:"hash,omitempty"`
	Policies []governance.LocalRow `json:"policies,omitempty"`
	Defaults *governance.Defaults  `json:"defaults,omitempty"`
}

// GovernanceCandidates reads every node's own governance policies. Leader
// only.
func (c *Coordinator) GovernanceCandidates(ctx context.Context) (GovernanceCandidates, error) {
	var out GovernanceCandidates
	if !c.policyKindActive(raft.PolicyKindGovernance) {
		return out, ErrGovernancePoliciesNotEnabled
	}
	if !c.raftNode.IsLeader() {
		return out, ErrNotLeaderForTopology
	}
	fsm := c.fsmOrNil()
	out.Seeded = fsm.PoliciesSeeded(raft.PolicyKindGovernance)
	if a, ok := fsm.PolicyAwaitingAdoption(raft.PolicyKindGovernance); ok {
		out.Awaiting = &a
	}
	if ch, ok := fsm.PolicyAdoptChoiceFor(raft.PolicyKindGovernance); ok {
		out.Choice = &ch
	}
	if out.Seeded {
		return out, nil
	}
	answers := c.queryAllNodes(ctx, raft.PolicyKindGovernance, 0)
	out.Nodes = make(map[string]GovernanceNodeSet, len(answers))
	for id, a := range answers {
		if a.err != nil {
			out.Nodes[id] = GovernanceNodeSet{Error: a.err.Error()}
			continue
		}
		rows, err := decodeGovernanceRows(a.rows)
		if err != nil {
			out.Nodes[id] = GovernanceNodeSet{Error: err.Error()}
			continue
		}
		out.Nodes[id] = GovernanceNodeSet{Hash: GovernanceRowsHash(rows.Policies), Policies: rows.Policies, Defaults: rows.Defaults}
	}
	return out, nil
}
