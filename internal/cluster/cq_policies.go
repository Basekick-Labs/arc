package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

type CQLocalRows struct {
	Policies    []raft.CQPolicyEntry `json:"policies"`
	MaxIssuedID int64                `json:"max_issued_id,omitempty"`
	Completed   map[int64]bool       `json:"completed,omitempty"`
}

type CQCandidateNode struct {
	Error string      `json:"error,omitempty"`
	Hash  string      `json:"hash,omitempty"`
	Rows  CQLocalRows `json:"rows,omitempty"`
}

type CQCandidates struct {
	Seeded   bool                       `json:"seeded"`
	Awaiting *raft.PolicyAwaiting       `json:"awaiting,omitempty"`
	Choice   *raft.PolicyAdoptChoice    `json:"pending_choice,omitempty"`
	Nodes    map[string]CQCandidateNode `json:"nodes,omitempty"`
}

var ErrCQPoliciesNotEnabled = errors.New("cluster-wide continuous queries are not enabled yet: every node must run a release that supports them")

// CQCandidates exposes the read-only inventories and hashes used when an
// operator must choose one node's CQ definitions during adoption.
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
	if choice, ok := fsm.PolicyAdoptChoiceFor(raft.PolicyKindCQ); ok {
		out.Choice = &choice
	}
	if out.Seeded {
		return out, nil
	}
	answers := c.queryAllNodes(ctx, raft.PolicyKindCQ, 0)
	out.Nodes = make(map[string]CQCandidateNode, len(answers))
	for id, answer := range answers {
		if answer.err != nil {
			out.Nodes[id] = CQCandidateNode{Error: answer.err.Error()}
			continue
		}
		rows, err := decodeCQLocalRows(answer.rows)
		if err != nil {
			out.Nodes[id] = CQCandidateNode{Error: err.Error()}
			continue
		}
		out.Nodes[id] = CQCandidateNode{Hash: CQRowsHash(rows), Rows: rows}
	}
	return out, nil
}

func (c *Coordinator) ChooseCQAdoption(node, hash, by string) error {
	if !c.policyKindActive(raft.PolicyKindCQ) {
		return ErrCQPoliciesNotEnabled
	}
	if !c.raftNode.IsLeader() {
		return ErrNotLeaderForTopology
	}
	fsm := c.fsmOrNil()
	if fsm.PoliciesSeeded(raft.PolicyKindCQ) {
		return ErrPolicyAdoptionNotNeeded
	}
	if _, ok := fsm.GetNode(node); !ok {
		return fmt.Errorf("%w: %q", ErrUnknownPolicyNode, node)
	}
	_, err := c.proposeLeader(raft.CommandChoosePolicyAdoption, raft.ChoosePolicyAdoptionPayload{Kind: raft.PolicyKindCQ, Node: node, Hash: strings.TrimSpace(hash), By: by, Now: time.Now().UTC()})
	return err
}

func CQRowsHash(rows CQLocalRows) string {
	b, _ := json.Marshal(rows)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// planCQSeed refuses ambiguous definitions or progress. An explicit choice
// names a node and the exact inventory hash the administrator reviewed.
func planCQSeed(perNode map[string]CQLocalRows, choice *raft.PolicyAdoptChoice) (raft.SeedCQPayload, string) {
	var floor int64
	for _, n := range perNode {
		floor = max(floor, n.MaxIssuedID)
		for _, e := range n.Policies {
			floor = max(floor, e.ID)
		}
	}
	if choice != nil {
		n, ok := perNode[choice.Node]
		if !ok || CQRowsHash(n) != choice.Hash {
			return raft.SeedCQPayload{}, "chosen continuous-query inventory changed"
		}
		entries := append([]raft.CQPolicyEntry(nil), n.Policies...)
		for i := range entries {
			entries[i].Sources = []raft.CQSource{{Node: choice.Node, ID: entries[i].ID}}
			if entries[i].IsActive && !n.Completed[entries[i].ID] {
				entries[i].IsActive = false
			}
		}
		seed := raft.SeedCQPayload{Entries: entries, Floor: floor}
		if why := validateCQSeedSize(seed); why != "" {
			return raft.SeedCQPayload{}, why
		}
		return seed, ""
	}
	type value struct {
		entry     raft.CQPolicyEntry
		watermark string
		ran       bool
		sources   []raft.CQSource
	}
	byName := map[string]*value{}
	for node, n := range perNode {
		for _, e := range n.Policies {
			v, ok := byName[e.Name]
			wm := ""
			if e.LastProcessedTime != nil {
				wm = e.LastProcessedTime.UTC().Format(time.RFC3339Nano)
			}
			if !ok {
				x := e
				byName[e.Name] = &value{entry: x, watermark: wm, ran: n.Completed[e.ID], sources: []raft.CQSource{{Node: node, ID: e.ID}}}
				continue
			}
			if !sameCQDefinition(v.entry, e) {
				return raft.SeedCQPayload{}, fmt.Sprintf("continuous query %q has conflicting definitions", e.Name)
			}
			if n.Completed[e.ID] {
				if v.ran && wm != v.watermark {
					return raft.SeedCQPayload{}, fmt.Sprintf("continuous query %q has conflicting progress", e.Name)
				}
				if !v.ran {
					v.watermark = wm
					v.entry.LastProcessedTime = e.LastProcessedTime
				}
				v.ran = true
			}
			v.sources = append(v.sources, raft.CQSource{Node: node, ID: e.ID})
		}
	}
	var entries []raft.CQPolicyEntry
	for _, v := range byName {
		e := v.entry
		e.Sources = v.sources
		if e.IsActive && !v.ran {
			return raft.SeedCQPayload{}, fmt.Sprintf("continuous query %q has never run", e.Name)
		}
		if !v.ran {
			e.IsActive = false
		}
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	if len(entries) > raft.MaxSeedCQPolicies {
		return raft.SeedCQPayload{}, fmt.Errorf("%d continuous queries exceed the %d seed limit", len(entries), raft.MaxSeedCQPolicies).Error()
	}
	// IDs are safe only if every local ID is unique across all nodes.
	used := map[int64]bool{}
	safe := true
	for _, n := range perNode {
		for _, e := range n.Policies {
			if used[e.ID] {
				safe = false
			}
			used[e.ID] = true
		}
	}
	if !safe {
		next := floor
		for i := range entries {
			next++
			entries[i].ID = next
		}
	}
	seed := raft.SeedCQPayload{Entries: entries, Floor: func() int64 {
		if safe {
			return floor
		}
		return floor + int64(len(entries))
	}()}
	if why := validateCQSeedSize(seed); why != "" {
		return raft.SeedCQPayload{}, why
	}
	return seed, ""
}

func validateCQSeedSize(seed raft.SeedCQPayload) string {
	encoded, err := json.Marshal(seed)
	if err != nil {
		return fmt.Sprintf("could not encode continuous-query seed: %v", err)
	}
	if len(encoded) > raft.MaxSeedCQPayloadBytes {
		return fmt.Sprintf("continuous-query seed is %d bytes, above the %d-byte Raft forwarding limit; reduce the number or size of definitions", len(encoded), raft.MaxSeedCQPayloadBytes)
	}
	return ""
}

func sameCQDefinition(a, b raft.CQPolicyEntry) bool {
	a.ID = 0
	b.ID = 0
	a.Version = 0
	b.Version = 0
	a.CreatedAt = time.Time{}
	b.CreatedAt = time.Time{}
	a.UpdatedAt = time.Time{}
	b.UpdatedAt = time.Time{}
	a.LastProcessedTime = nil
	b.LastProcessedTime = nil
	a.Sources = nil
	b.Sources = nil
	return reflect.DeepEqual(a, b)
}

func decodeCQLocalRows(raw json.RawMessage) (CQLocalRows, error) {
	var r CQLocalRows
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &r); err != nil {
			return r, err
		}
	}
	if r.Completed == nil {
		r.Completed = map[int64]bool{}
	}
	return r, nil
}

func mergeCQAnswers(rows map[string]json.RawMessage, choice *raft.PolicyAdoptChoice) (raft.SeedCQPayload, error) {
	perNode := map[string]CQLocalRows{}
	for id, raw := range rows {
		r, err := decodeCQLocalRows(raw)
		if err != nil {
			return raft.SeedCQPayload{}, fmt.Errorf("node %s: %w", id, err)
		}
		perNode[id] = r
	}
	seed, why := planCQSeed(perNode, choice)
	if why != "" {
		return seed, errors.New(why)
	}
	return seed, nil
}

func (c *Coordinator) cqPoliciesJoinGate(req interface{ GetCapabilities() []string }) (string, func()) {
	g := &c.cqPolicies
	g.mu.Lock()
	f := c.fsmOrNil()
	if hasCapability(req.GetCapabilities(), raft.CapabilityClusterCQ) {
		return "", g.mu.Unlock
	}
	enabled := f != nil && f.CQEnabled()
	if !enabled && g.inFlight {
		if err := c.barrierWithin(streamClaimsJoinBarrier); err != nil {
			g.mu.Unlock()
			return "the cluster could not confirm its continuous-query state; retry the join", nil
		}
		enabled = f != nil && f.CQEnabled()
	}
	if !enabled {
		return "", g.mu.Unlock
	}
	g.mu.Unlock()
	return "cluster-wide continuous queries are enabled: the node must run a compatible release", nil
}

func (c *Coordinator) maybeEnableCQPolicies() {
	if c.raftNode == nil {
		return
	}
	f := c.fsmOrNil()
	if f == nil || !f.ClusterPoliciesEnabled() || f.CQEnabled() || !c.raftNode.IsLeader() {
		return
	}
	g := &c.cqPolicies
	g.mu.Lock()
	defer g.mu.Unlock()
	nodes, blockers := capabilityCandidates(f.GetAllNodes(), c.raftNode.ConfigurationServerIDs, raft.CapabilityClusterCQ)
	g.blockers.Store(&blockers)
	if len(blockers) > 0 {
		return
	}
	raw, err := json.Marshal(raft.EnableCQPayload{Nodes: nodes, Now: time.Now().UTC()})
	if err != nil {
		return
	}
	g.inFlight = true
	if err := c.raftNode.Apply(&raft.Command{Type: raft.CommandEnableCQ, Payload: raw}, 5*time.Second); err != nil {
		c.logger.Warn().Err(err).Msg("Proposing cluster-wide continuous queries failed; retrying")
		return
	}
	c.logger.Info().Int("nodes", len(nodes)).Msg("Cluster-wide continuous queries enabled")
}

func (c *Coordinator) CQEnabled() bool { f := c.fsmOrNil(); return f != nil && f.CQEnabled() }
func (c *Coordinator) CQsSeeded() bool { f := c.fsmOrNil(); return f != nil && f.CQsSeeded() }
func (c *Coordinator) CQ(id int64) (raft.CQPolicyEntry, bool) {
	f := c.fsmOrNil()
	if f == nil {
		return raft.CQPolicyEntry{}, false
	}
	return f.CQ(id)
}
func (c *Coordinator) CQByName(name string) (raft.CQPolicyEntry, bool) {
	f := c.fsmOrNil()
	if f == nil {
		return raft.CQPolicyEntry{}, false
	}
	return f.CQByName(name)
}
func (c *Coordinator) CQs() []raft.CQPolicyEntry {
	f := c.fsmOrNil()
	if f == nil {
		return nil
	}
	return f.CQs()
}
func (c *Coordinator) CQMirrorState() raft.CQMirror {
	f := c.fsmOrNil()
	if f == nil {
		return raft.CQMirror{}
	}
	return f.CQMirrorState()
}
func (c *Coordinator) PrimaryEpoch() uint64 {
	f := c.fsmOrNil()
	if f == nil {
		return 0
	}
	return f.PrimaryEpoch()
}

func (c *Coordinator) CQCollecting() bool { return c.PolicyCollecting(raft.PolicyKindCQ) }
func (c *Coordinator) CQReadBarrier(ctx context.Context) error {
	if c == nil || c.raftNode == nil {
		return errors.New("continuous queries: raft not initialized")
	}
	return c.waitForManifestSync(ctx, c.raftNode, 10*time.Second)
}

func (c *Coordinator) CreateCQ(ctx context.Context, p raft.CreateCQPayload) error {
	p.Now = time.Now().UTC()
	return c.proposePolicy(ctx, raft.CommandCreateCQ, p)
}
func (c *Coordinator) UpdateCQ(ctx context.Context, p raft.UpdateCQPayload) error {
	p.Now = time.Now().UTC()
	return c.proposePolicy(ctx, raft.CommandUpdateCQ, p)
}
func (c *Coordinator) DeleteCQ(ctx context.Context, p raft.DeleteCQPayload) error {
	return c.proposePolicy(ctx, raft.CommandDeleteCQ, p)
}
func (c *Coordinator) CompleteCQ(ctx context.Context, p raft.CompleteCQPayload) error {
	return c.proposePolicy(ctx, raft.CommandCompleteCQ, p)
}
