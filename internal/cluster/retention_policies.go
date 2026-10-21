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

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// Retention policies, cluster-wide (#1324 stage 2): the coordinator side. See
// raft/retention_policies.go.

// retentionPoliciesJoinGate is clusterPoliciesJoinGate for retention's own
// flag.
func (c *Coordinator) retentionPoliciesJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := &c.retentionPolicies
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), raft.CapabilityClusterRetention) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && fsm.RetentionPoliciesEnabled()
	if !enabled && g.inFlight {
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its retention policies state; retry the join", nil
		}
		enabled = fsm != nil && fsm.RetentionPoliciesEnabled()
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "cluster-wide retention policies are enabled in this cluster: the node must run a release that supports them", nil
}

// maybeEnableRetentionPolicies proposes retention's flag from the leader once
// every node supports it.
func (c *Coordinator) maybeEnableRetentionPolicies() {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.ClusterPoliciesEnabled() || fsm.RetentionPoliciesEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.retentionPolicies
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityClusterRetention)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableRetentionPoliciesPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableRetentionPolicies, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing cluster-wide retention policies failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Cluster-wide retention policies enabled: every node supports them")
}

// policyKindActive reports whether kind can be adopted here: the flag, and
// for retention its own flag too.
func (c *Coordinator) policyKindActive(kind string) bool {
	fsm := c.fsmOrNil()
	if fsm == nil || !fsm.ClusterPoliciesEnabled() {
		return false
	}
	if kind == raft.PolicyKindRetention {
		return fsm.RetentionPoliciesEnabled()
	}
	return true
}

// RetentionPoliciesSeeded reports whether retention policies live in the
// cluster state. Until then they stay in each node's SQLite, as before.
func (c *Coordinator) RetentionPoliciesSeeded() bool {
	return c.policyKindActive(raft.PolicyKindRetention) && c.PoliciesSeeded(raft.PolicyKindRetention)
}

// RetentionPoliciesCollecting reports an open collect of retention policies:
// local retention writes are refused until it ends.
func (c *Coordinator) RetentionPoliciesCollecting() bool {
	return c.PolicyCollecting(raft.PolicyKindRetention)
}

// RetentionPolicy returns retention policy id from the cluster state.
func (c *Coordinator) RetentionPolicy(id int64) (raft.RetentionPolicyEntry, bool) {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.RetentionPolicy(id)
	}
	return raft.RetentionPolicyEntry{}, false
}

// RetentionPolicyByName returns the retention policy named name.
func (c *Coordinator) RetentionPolicyByName(name string) (raft.RetentionPolicyEntry, bool) {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.RetentionPolicyByName(name)
	}
	return raft.RetentionPolicyEntry{}, false
}

// RetentionPolicies returns every retention policy in the cluster state.
func (c *Coordinator) RetentionPolicies() []raft.RetentionPolicyEntry {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.RetentionPolicies()
	}
	return nil
}

// RetentionMirrorState returns what this node's retention mirror reconciles to.
func (c *Coordinator) RetentionMirrorState() raft.RetentionMirror {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.RetentionMirrorState()
	}
	return raft.RetentionMirror{}
}

// CreateRetentionPolicy creates a retention policy cluster-wide.
func (c *Coordinator) CreateRetentionPolicy(ctx context.Context, p raft.CreateRetentionPolicyPayload) error {
	p.Now = time.Now().UTC()
	return c.proposePolicy(ctx, raft.CommandCreateRetentionPolicy, p)
}

// UpdateRetentionPolicy changes the fields p carries, cluster-wide.
func (c *Coordinator) UpdateRetentionPolicy(ctx context.Context, p raft.UpdateRetentionPolicyPayload) error {
	p.Now = time.Now().UTC()
	return c.proposePolicy(ctx, raft.CommandUpdateRetentionPolicy, p)
}

// DeleteRetentionPolicy removes retention policy id cluster-wide.
func (c *Coordinator) DeleteRetentionPolicy(ctx context.Context, id int64) error {
	return c.proposePolicy(ctx, raft.CommandDeleteRetentionPolicy, raft.DeleteRetentionPolicyPayload{ID: id})
}

// AddPoliciesChangedCallback signals fn after any policy change applied here
// (or a snapshot restored). fn must not block.
func (c *Coordinator) AddPoliciesChangedCallback(fn func()) {
	if fsm := c.fsmOrNil(); fsm != nil {
		fsm.AddPoliciesChangedCallback(fn)
	}
}

// RetentionLocalRow is one of a node's own retention policies, as it answers
// a collect: the definition, and how many runs of it completed on this node
// since it was last changed.
type RetentionLocalRow struct {
	ID            int64     `json:"id"`
	Name          string    `json:"name"`
	Database      string    `json:"database"`
	Measurement   *string   `json:"measurement,omitempty"`
	RetentionDays int       `json:"retention_days"`
	BufferDays    int       `json:"buffer_days"`
	IsActive      bool      `json:"is_active"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	Executions    int64     `json:"executions"`
}

// RetentionLocalRows is one node's answer for the retention kind.
type RetentionLocalRows struct {
	Policies []RetentionLocalRow `json:"policies"`
	// MaxIssuedID is the highest ID the node ever issued, deleted policies
	// included: a client may still hold any of them.
	MaxIssuedID int64 `json:"max_issued_id,omitempty"`
}

// RetentionRowsHash identifies a node's retention definitions (IDs included,
// timestamps and run counts not): an admin's choice names the hash of the
// rows they saw, and the leader seeds only if the node still holds those.
func RetentionRowsHash(rows []RetentionLocalRow) string {
	type def struct {
		ID            int64  `json:"i"`
		Name          string `json:"n"`
		Database      string `json:"d"`
		Measurement   string `json:"m"`
		RetentionDays int    `json:"r"`
		BufferDays    int    `json:"b"`
		IsActive      bool   `json:"a"`
	}
	defs := make([]def, 0, len(rows))
	for _, r := range rows {
		defs = append(defs, def{r.ID, r.Name, r.Database, measurementKey(r.Measurement), r.RetentionDays, r.BufferDays, r.IsActive})
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].ID < defs[j].ID })
	raw, _ := json.Marshal(defs)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func measurementKey(m *string) string {
	if m == nil {
		return ""
	}
	return *m
}

// retentionContent is a policy's definition without its ID or timestamps:
// two nodes' rows with the same content are the same policy.
type retentionContent struct {
	name, database, measurement string
	retentionDays, bufferDays   int
	isActive                    bool
}

func contentOf(r RetentionLocalRow) retentionContent {
	return retentionContent{r.Name, r.Database, measurementKey(r.Measurement), r.RetentionDays, r.BufferDays, r.IsActive}
}

// planRetentionSeed decides a retention seed from every node's rows, or why
// none can be made yet.
//
// With choice (an admin's), the seed is exactly the chosen node's rows, and
// only if they still hash to what the admin saw. Without one, the leader
// seeds on its own only when the nodes agree (no name with two definitions)
// and every active policy already ran on some node: a policy that never ran
// is never activated without an admin choosing it.
//
// IDs are kept when every policy comes from one node and no other node uses
// any of those IDs, so the IDs clients hold for that node keep naming the
// same policies; otherwise every policy is numbered above the highest ID any
// node holds, so no old ID names a different policy.
func planRetentionSeed(perNode map[string]RetentionLocalRows, choice *raft.PolicyAdoptChoice) (raft.SeedPoliciesPayload, string) {
	// issued is, per node, the highest ID it ever handed out.
	var maxID int64
	issued := make(map[string]int64, len(perNode))
	for id, n := range perNode {
		issued[id] = n.MaxIssuedID
		for _, r := range n.Policies {
			issued[id] = max(issued[id], r.ID)
		}
		maxID = max(maxID, issued[id])
	}
	type acc struct {
		row     RetentionLocalRow
		sources []raft.RetentionSource
		runs    int64
	}
	byContent := map[retentionContent]*acc{}
	var order []retentionContent
	add := func(node string, r RetentionLocalRow) {
		k := contentOf(r)
		a, ok := byContent[k]
		if !ok {
			a = &acc{row: r}
			byContent[k] = a
			order = append(order, k)
		}
		a.sources = append(a.sources, raft.RetentionSource{Node: node, ID: r.ID})
		a.runs += r.Executions
		if !r.CreatedAt.IsZero() && (a.row.CreatedAt.IsZero() || r.CreatedAt.Before(a.row.CreatedAt)) {
			a.row.CreatedAt = r.CreatedAt
		}
		if r.UpdatedAt.After(a.row.UpdatedAt) {
			a.row.UpdatedAt = r.UpdatedAt
		}
	}
	nodes := make([]string, 0, len(perNode))
	for id := range perNode {
		nodes = append(nodes, id)
	}
	sort.Strings(nodes)
	if choice != nil {
		rows, ok := perNode[choice.Node]
		if !ok {
			return raft.SeedPoliciesPayload{}, fmt.Sprintf("the chosen node %s did not answer", choice.Node)
		}
		if RetentionRowsHash(rows.Policies) != choice.Hash {
			return raft.SeedPoliciesPayload{}, fmt.Sprintf("the retention policies on %s changed since they were chosen; look again and choose again", choice.Node)
		}
		for _, r := range rows.Policies {
			add(choice.Node, r)
		}
	} else {
		for _, id := range nodes {
			for _, r := range perNode[id].Policies {
				add(id, r)
			}
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i].name < order[j].name })
	names := map[string]bool{}
	for _, k := range order {
		a := byContent[k]
		if names[k.name] {
			return raft.SeedPoliciesPayload{}, fmt.Sprintf("the nodes hold different definitions of retention policy %q; choose one node's policies", k.name)
		}
		names[k.name] = true
		if err := raft.ValidRetentionPolicy(a.row.Name, a.row.Database, a.row.Measurement, a.row.RetentionDays, a.row.BufferDays); err != nil {
			return raft.SeedPoliciesPayload{}, fmt.Sprintf("retention policy %q on %s cannot be adopted (%v); fix it on that node first", a.row.Name, a.sources[0].Node, err)
		}
		if choice == nil && a.row.IsActive && a.runs == 0 {
			return raft.SeedPoliciesPayload{}, fmt.Sprintf("retention policy %q is active and has never run on any node; choose a node's policies to adopt it", a.row.Name)
		}
	}
	if len(order) > raft.MaxSeedRetentionPolicies {
		return raft.SeedPoliciesPayload{}, fmt.Sprintf("%d retention policies exceed the %d one seed installs", len(order), raft.MaxSeedRetentionPolicies)
	}
	seed := raft.SeedPoliciesPayload{RetentionFloor: maxID + 1, RetentionKeepIDs: true}
	sourceNode := ""
	kept := map[int64]bool{}
	for _, k := range order {
		a := byContent[k]
		if len(a.sources) != 1 || (sourceNode != "" && a.sources[0].Node != sourceNode) {
			seed.RetentionKeepIDs = false
		}
		sourceNode = a.sources[0].Node
		kept[a.sources[0].ID] = true
	}
	if seed.RetentionKeepIDs {
		// No other node may ever have handed out a kept ID, even for a
		// policy it has since deleted: a client of that node could hold it.
		for _, id := range nodes {
			if id == sourceNode {
				continue
			}
			for k := range kept {
				if k <= issued[id] {
					seed.RetentionKeepIDs = false
				}
			}
		}
	}
	for _, k := range order {
		a := byContent[k]
		e := raft.RetentionPolicyEntry{
			Name: a.row.Name, Database: a.row.Database, Measurement: a.row.Measurement,
			RetentionDays: a.row.RetentionDays, BufferDays: a.row.BufferDays, IsActive: a.row.IsActive,
			CreatedAt: a.row.CreatedAt, UpdatedAt: a.row.UpdatedAt, Sources: a.sources,
		}
		if seed.RetentionKeepIDs {
			e.ID = a.sources[0].ID
		}
		seed.Retention = append(seed.Retention, e)
	}
	return seed, ""
}

// ErrPolicyAdoptionNotNeeded: an adoption choice for a kind already seeded.
var ErrPolicyAdoptionNotNeeded = errors.New("the cluster already holds these policies")

// ErrRetentionPoliciesNotEnabled: cluster-wide retention is not on yet.
var ErrRetentionPoliciesNotEnabled = errors.New("cluster-wide retention policies are not enabled yet: every node must run a release that supports them")

// ErrUnknownPolicyNode: an adoption choice naming a node not in the cluster.
var ErrUnknownPolicyNode = errors.New("no such node in the cluster")

// RetentionCandidates is GET /api/v1/cluster/policies: each node's own
// retention policies, read live, with the hash an adoption choice names.
type RetentionCandidates struct {
	Seeded   bool                        `json:"seeded"`
	Awaiting *raft.PolicyAwaiting        `json:"awaiting,omitempty"`
	Choice   *raft.PolicyAdoptChoice     `json:"pending_choice,omitempty"`
	Nodes    map[string]RetentionNodeSet `json:"nodes,omitempty"`
}

// RetentionNodeSet is one node's retention policies, or why they could not be
// read.
type RetentionNodeSet struct {
	Error    string              `json:"error,omitempty"`
	Hash     string              `json:"hash,omitempty"`
	Policies []RetentionLocalRow `json:"policies,omitempty"`
}

// RetentionCandidates reads every node's own retention policies. Leader only:
// the nodes answer only the leader.
func (c *Coordinator) RetentionCandidates(ctx context.Context) (RetentionCandidates, error) {
	var out RetentionCandidates
	if !c.policyKindActive(raft.PolicyKindRetention) {
		return out, ErrRetentionPoliciesNotEnabled
	}
	if !c.raftNode.IsLeader() {
		return out, ErrNotLeaderForTopology
	}
	fsm := c.fsmOrNil()
	out.Seeded = fsm.PoliciesSeeded(raft.PolicyKindRetention)
	if a, ok := fsm.PolicyAwaitingAdoption(raft.PolicyKindRetention); ok {
		out.Awaiting = &a
	}
	if ch, ok := fsm.PolicyAdoptChoiceFor(raft.PolicyKindRetention); ok {
		out.Choice = &ch
	}
	if out.Seeded {
		return out, nil
	}
	answers := c.queryAllNodes(ctx, raft.PolicyKindRetention, 0)
	out.Nodes = make(map[string]RetentionNodeSet, len(answers))
	for id, a := range answers {
		if a.err != nil {
			out.Nodes[id] = RetentionNodeSet{Error: a.err.Error()}
			continue
		}
		rows, err := decodeRetentionRows(a.rows)
		if err != nil {
			out.Nodes[id] = RetentionNodeSet{Error: err.Error()}
			continue
		}
		out.Nodes[id] = RetentionNodeSet{Hash: RetentionRowsHash(rows.Policies), Policies: rows.Policies}
	}
	return out, nil
}

// ChooseRetentionAdoption records an admin's choice of node whose retention
// policies the cluster adopts; the leader's adoption loop acts on it. Leader
// only, so the choice never travels the peer forwarding path.
func (c *Coordinator) ChooseRetentionAdoption(node, hash, by string) error {
	if !c.policyKindActive(raft.PolicyKindRetention) {
		return ErrRetentionPoliciesNotEnabled
	}
	if !c.raftNode.IsLeader() {
		return ErrNotLeaderForTopology
	}
	fsm := c.fsmOrNil()
	if fsm.PoliciesSeeded(raft.PolicyKindRetention) {
		return ErrPolicyAdoptionNotNeeded
	}
	if _, ok := fsm.GetNode(node); !ok {
		return fmt.Errorf("%w: %q", ErrUnknownPolicyNode, node)
	}
	_, err := c.proposeLeader(raft.CommandChoosePolicyAdoption, raft.ChoosePolicyAdoptionPayload{
		Kind: raft.PolicyKindRetention, Node: node, Hash: strings.TrimSpace(hash), By: by, Now: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	c.nudgeAdoption()
	return nil
}

func decodeRetentionRows(raw json.RawMessage) (RetentionLocalRows, error) {
	var r RetentionLocalRows
	if len(raw) == 0 || string(raw) == "null" {
		return r, nil
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	return r, nil
}
