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
	"github.com/basekick-labs/arc/internal/cluster/security"
)

// Continuous queries, cluster-wide (#1324 stage 3): the coordinator side. See
// raft/cq_policies.go.

// ErrCQPoliciesNotEnabled: cluster-wide continuous queries are not on yet.
var ErrCQPoliciesNotEnabled = errors.New("cluster-wide continuous queries are not enabled yet: every node must run a release that supports them")

// cqPoliciesJoinGate is retentionPoliciesJoinGate for the CQ flag.
func (c *Coordinator) cqPoliciesJoinGate(req interface {
	GetNodeID() string
	GetCapabilities() []string
}) (why string, release func()) {
	fsm := c.raftNode.FSM()
	g := &c.cqPolicies
	g.mu.Lock()
	if hasCapability(req.GetCapabilities(), raft.CapabilityClusterCQ) {
		return "", g.mu.Unlock
	}
	enabled := fsm != nil && fsm.CQPoliciesEnabled()
	if !enabled && g.inFlight {
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its continuous query policies state; retry the join", nil
		}
		enabled = fsm != nil && fsm.CQPoliciesEnabled()
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "cluster-wide continuous queries are enabled in this cluster: the node must run a release that supports them", nil
}

// maybeEnableCQPolicies proposes the CQ flag from the leader once every node
// supports it.
func (c *Coordinator) maybeEnableCQPolicies() {
	if c.raftNode == nil {
		return
	}
	fsm := c.raftNode.FSM()
	if fsm == nil || !fsm.ClusterPoliciesEnabled() || fsm.CQPoliciesEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.cqPolicies
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(fsm.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityClusterCQ)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableCQPoliciesPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableCQPolicies, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing cluster-wide continuous queries failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Cluster-wide continuous queries enabled: every node supports them")
}

// CQPoliciesSeeded reports whether continuous queries live in the cluster
// state. Until then they stay in each node's SQLite, as before.
func (c *Coordinator) CQPoliciesSeeded() bool {
	return c.policyKindActive(raft.PolicyKindCQ) && c.PoliciesSeeded(raft.PolicyKindCQ)
}

// CQPoliciesCollecting reports an open collect of continuous queries.
func (c *Coordinator) CQPoliciesCollecting() bool {
	return c.PolicyCollecting(raft.PolicyKindCQ)
}

// ClusterCQ returns continuous query id from the cluster state.
func (c *Coordinator) ClusterCQ(id int64) (raft.CQEntry, bool) {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.CQ(id)
	}
	return raft.CQEntry{}, false
}

// ClusterCQByName returns the continuous query named name.
func (c *Coordinator) ClusterCQByName(name string) (raft.CQEntry, bool) {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.CQByName(name)
	}
	return raft.CQEntry{}, false
}

// ClusterCQs returns every continuous query in the cluster state.
func (c *Coordinator) ClusterCQs() []raft.CQEntry {
	if fsm := c.fsmOrNil(); fsm != nil {
		return fsm.CQs()
	}
	return nil
}

// CreateCQ creates a continuous query cluster-wide.
func (c *Coordinator) CreateCQ(ctx context.Context, p raft.CreateCQPayload) error {
	p.Now = time.Now().UTC()
	return c.proposePolicy(ctx, raft.CommandCreateCQ, p)
}

// UpdateCQ changes the fields p carries, cluster-wide.
func (c *Coordinator) UpdateCQ(ctx context.Context, p raft.UpdateCQPayload) error {
	p.Now = time.Now().UTC()
	return c.proposePolicy(ctx, raft.CommandUpdateCQ, p)
}

// DeleteCQ removes continuous query id cluster-wide.
func (c *Coordinator) DeleteCQ(ctx context.Context, id int64) error {
	return c.proposePolicy(ctx, raft.CommandDeleteCQ, raft.DeleteCQPayload{ID: id})
}

// SetCQWatermark proposes the compare-and-set of a CQ's watermark from this
// node. A forwarded proposal returns once this node applied it, or once the
// local wait timed out: the caller reads the outcome from its own FSM.
func (c *Coordinator) SetCQWatermark(ctx context.Context, id int64, from *time.Time, to time.Time) error {
	return c.proposePolicy(ctx, raft.CommandSetCQWatermark, raft.SetCQWatermarkPayload{
		Proposer: c.LocalNodeID(), Shared: c.cfg != nil && c.cfg.SharedStorageMode, ID: id, From: from, To: to,
	})
}

// CQReadBarrier returns once this node has applied everything the leader had
// committed when it was called, so a watermark read afterwards is not older
// than one the cluster already holds. A run reads its watermark only after it
// (a new leader, or a primary that was cut off, could otherwise run from a
// stale one and aggregate a committed window again).
func (c *Coordinator) CQReadBarrier(ctx context.Context) error {
	if c.raftNode == nil {
		return errors.New("raft not initialized")
	}
	if c.raftNode.IsLeader() {
		return c.barrierWithin(policyProposeTimeout)
	}
	token, err := security.GenerateNonce()
	if err != nil {
		return err
	}
	raw, err := json.Marshal(raft.BarrierPayload{Token: token, NodeID: c.LocalNodeID()})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, policyProposeTimeout)
	defer cancel()
	index, err := c.forwardApplyToLeaderIndex(ctx, &raft.Command{Type: raft.CommandBarrier, Payload: raw}, true)
	if err != nil {
		return err
	}
	if index == 0 {
		return errors.New("the leader did not say where its barrier applied")
	}
	return c.waitAppliedHere(ctx, index)
}

// CQLocalRow is one of a node's own continuous queries, as it answers a
// collect: the definition, its watermark, and how many runs completed here.
type CQLocalRow struct {
	ID                     int64      `json:"id"`
	Name                   string     `json:"name"`
	Description            *string    `json:"description,omitempty"`
	Database               string     `json:"database"`
	SourceMeasurement      string     `json:"source_measurement"`
	DestinationMeasurement string     `json:"destination_measurement"`
	Query                  string     `json:"query"`
	Interval               string     `json:"interval"`
	TagColumns             []string   `json:"tag_columns,omitempty"`
	RetentionDays          *int       `json:"retention_days,omitempty"`
	DeleteSourceAfterDays  *int       `json:"delete_source_after_days,omitempty"`
	IsActive               bool       `json:"is_active"`
	LastProcessed          *time.Time `json:"last_processed,omitempty"`
	CreatedAt              time.Time  `json:"created_at"`
	UpdatedAt              time.Time  `json:"updated_at"`
	Executions             int64      `json:"executions"`
}

// CQLocalRows is one node's answer for the CQ kind.
type CQLocalRows struct {
	Policies    []CQLocalRow `json:"policies"`
	MaxIssuedID int64        `json:"max_issued_id,omitempty"`
}

// cqContent is a CQ's definition without its ID, timestamps or watermark.
// nil and empty optional values are the same (a NULL and an empty
// description, no tag columns and an empty list).
type cqContent string

func cqContentOf(r CQLocalRow) cqContent {
	opt := func(p *int) string {
		if p == nil {
			return ""
		}
		return fmt.Sprint(*p)
	}
	desc := ""
	if r.Description != nil {
		desc = *r.Description
	}
	raw, _ := json.Marshal([]string{
		r.Name, desc, r.Database, r.SourceMeasurement, r.DestinationMeasurement, r.Query, r.Interval,
		strings.Join(r.TagColumns, "\x00"), opt(r.RetentionDays), opt(r.DeleteSourceAfterDays), fmt.Sprint(r.IsActive),
	})
	return cqContent(raw)
}

// CQRowsHash identifies a node's continuous query definitions, IDs included,
// for an admin's choice.
func CQRowsHash(rows []CQLocalRow) string {
	type def struct {
		ID      int64     `json:"i"`
		Content cqContent `json:"c"`
	}
	defs := make([]def, 0, len(rows))
	for _, r := range rows {
		defs = append(defs, def{r.ID, cqContentOf(r)})
	}
	sort.Slice(defs, func(i, j int) bool { return defs[i].ID < defs[j].ID })
	raw, _ := json.Marshal(defs)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func decodeCQRows(raw json.RawMessage) (CQLocalRows, error) {
	var r CQLocalRows
	if len(raw) == 0 || string(raw) == "null" {
		return r, nil
	}
	err := json.Unmarshal(raw, &r)
	return r, err
}

// planCQSeed is planRetentionSeed for continuous queries, with two
// differences. The evidence is a completed run of the CQ at any time: a CQ
// writes aggregates and deletes nothing, so an edit on the primary does not
// need an admin. And every seeded CQ takes the newest watermark any node holds
// for the same definition, so choosing a node that never ran it opens no gap.
func planCQSeed(perNode map[string]CQLocalRows, choice *raft.PolicyAdoptChoice) (raft.SeedPoliciesPayload, string) {
	var maxID int64
	issued := make(map[string]int64, len(perNode))
	newest := map[cqContent]*time.Time{}
	for id, n := range perNode {
		issued[id] = n.MaxIssuedID
		for _, r := range n.Policies {
			issued[id] = max(issued[id], r.ID)
			if r.LastProcessed != nil {
				k := cqContentOf(r)
				if cur := newest[k]; cur == nil || r.LastProcessed.After(*cur) {
					t := *r.LastProcessed
					newest[k] = &t
				}
			}
		}
		maxID = max(maxID, issued[id])
	}
	type acc struct {
		row     CQLocalRow
		sources []raft.RetentionSource
		runs    int64
	}
	byContent := map[cqContent]*acc{}
	var order []cqContent
	add := func(node string, r CQLocalRow) {
		k := cqContentOf(r)
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
		if CQRowsHash(rows.Policies) != choice.Hash {
			return raft.SeedPoliciesPayload{}, fmt.Sprintf("the continuous queries on %s changed since they were chosen; look again and choose again", choice.Node)
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
	sort.Slice(order, func(i, j int) bool { return byContent[order[i]].row.Name < byContent[order[j]].row.Name })
	names := map[string]bool{}
	for _, k := range order {
		a := byContent[k]
		if names[a.row.Name] {
			return raft.SeedPoliciesPayload{}, fmt.Sprintf("the nodes hold different definitions of continuous query %q; choose one node's continuous queries", a.row.Name)
		}
		names[a.row.Name] = true
		if choice == nil && a.row.IsActive && a.runs == 0 {
			return raft.SeedPoliciesPayload{}, fmt.Sprintf("continuous query %q is active and has never run on any node; choose a node's continuous queries to adopt it", a.row.Name)
		}
	}
	if len(order) > raft.MaxSeedCQs {
		return raft.SeedPoliciesPayload{}, fmt.Sprintf("%d continuous queries exceed the %d one seed installs", len(order), raft.MaxSeedCQs)
	}
	seed := raft.SeedPoliciesPayload{CQFloor: maxID + 1, CQKeepIDs: true}
	sourceNode := ""
	kept := map[int64]bool{}
	for _, k := range order {
		a := byContent[k]
		if len(a.sources) != 1 || (sourceNode != "" && a.sources[0].Node != sourceNode) {
			seed.CQKeepIDs = false
		}
		sourceNode = a.sources[0].Node
		kept[a.sources[0].ID] = true
	}
	if seed.CQKeepIDs {
		for _, id := range nodes {
			if id == sourceNode {
				continue
			}
			for k := range kept {
				if k <= issued[id] {
					seed.CQKeepIDs = false
				}
			}
		}
	}
	for _, k := range order {
		a := byContent[k]
		r := a.row
		e := raft.CQEntry{
			Name: r.Name, Description: r.Description, Database: r.Database, SourceMeasurement: r.SourceMeasurement,
			DestinationMeasurement: r.DestinationMeasurement, Query: r.Query, Interval: r.Interval,
			TagColumns: r.TagColumns, RetentionDays: r.RetentionDays, DeleteSourceAfterDays: r.DeleteSourceAfterDays,
			IsActive: r.IsActive, LastProcessed: newest[k], CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt, Sources: a.sources,
		}
		if seed.CQKeepIDs {
			e.ID = a.sources[0].ID
		}
		seed.CQ = append(seed.CQ, e)
	}
	return seed, ""
}

// CQCandidates is the CQ part of GET /api/v1/cluster/policies.
type CQCandidates struct {
	Seeded   bool                    `json:"seeded"`
	Awaiting *raft.PolicyAwaiting    `json:"awaiting,omitempty"`
	Choice   *raft.PolicyAdoptChoice `json:"pending_choice,omitempty"`
	Nodes    map[string]CQNodeSet    `json:"nodes,omitempty"`
}

// CQNodeSet is one node's continuous queries, or why they could not be read.
type CQNodeSet struct {
	Error    string       `json:"error,omitempty"`
	Hash     string       `json:"hash,omitempty"`
	Policies []CQLocalRow `json:"policies,omitempty"`
}

// CQCandidates reads every node's own continuous queries. Leader only.
func (c *Coordinator) CQCandidates(ctx context.Context) (CQCandidates, error) {
	var out CQCandidates
	if !c.policyKindActive(raft.PolicyKindCQ) {
		return out, ErrCQPoliciesNotEnabled
	}
	if !c.raftNode.IsLeader() {
		return out, ErrNotLeaderForTopology
	}
	fsm := c.fsmOrNil()
	out.Seeded = fsm.PoliciesSeeded(raft.PolicyKindCQ)
	if a, ok := fsm.PolicyAwaitingAdoption(raft.PolicyKindCQ); ok {
		out.Awaiting = &a
	}
	if ch, ok := fsm.PolicyAdoptChoiceFor(raft.PolicyKindCQ); ok {
		out.Choice = &ch
	}
	if out.Seeded {
		return out, nil
	}
	answers := c.queryAllNodes(ctx, raft.PolicyKindCQ, 0)
	out.Nodes = make(map[string]CQNodeSet, len(answers))
	for id, a := range answers {
		if a.err != nil {
			out.Nodes[id] = CQNodeSet{Error: a.err.Error()}
			continue
		}
		rows, err := decodeCQRows(a.rows)
		if err != nil {
			out.Nodes[id] = CQNodeSet{Error: err.Error()}
			continue
		}
		out.Nodes[id] = CQNodeSet{Hash: CQRowsHash(rows.Policies), Policies: rows.Policies}
	}
	return out, nil
}
