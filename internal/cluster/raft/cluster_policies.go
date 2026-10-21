package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
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

// Policy kinds. Each is seeded once from the nodes' existing rows; until
// then that kind stays in each node's own SQLite exactly as before.
const (
	PolicyKindTiering = "tiering"
)

// maxPolicyDatabaseLen bounds a policy's database name.
const maxPolicyDatabaseLen = 256

// TieringPolicyEntry is one database's tiering policy. ID is the log index
// that created it.
type TieringPolicyEntry struct {
	ID            int64     `json:"id"`
	Database      string    `json:"database"`
	HotOnly       bool      `json:"hot_only"`
	HotMaxAgeDays *int      `json:"hot_max_age_days,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// SetTieringPolicyPayload creates or replaces a database's tiering policy.
type SetTieringPolicyPayload struct {
	Database      string    `json:"database"`
	HotOnly       bool      `json:"hot_only"`
	HotMaxAgeDays *int      `json:"hot_max_age_days,omitempty"`
	Now           time.Time `json:"now"`
}

// DeleteTieringPolicyPayload removes a database's tiering policy.
type DeleteTieringPolicyPayload struct {
	Database string `json:"database"`
}

// SeedPoliciesPayload installs a kind's adopted policies, once, for the
// collect at Epoch. Proposed by the leader only.
type SeedPoliciesPayload struct {
	Kind    string               `json:"kind"`
	Epoch   uint64               `json:"epoch"`
	Tiering []TieringPolicyEntry `json:"tiering,omitempty"`
	Now     time.Time            `json:"now"`
}

// CollectPoliciesPayload opens a collect of a kind: until it ends (a seed, or
// AwaitAdoption) no node takes a write of that kind into its own SQLite, so
// the rows the leader reads from every node are the rows that exist. The
// collect's epoch is its log index. Proposed by the leader only.
type CollectPoliciesPayload struct {
	Kind   string    `json:"kind"`
	Leader string    `json:"leader"`
	Now    time.Time `json:"now"`
}

// AwaitAdoptionPayload ends a collect that could not seed: Missing names the
// nodes that did not answer. Writes of the kind go to each node's SQLite
// again until the next collect. Proposed by the leader only.
type AwaitAdoptionPayload struct {
	Kind    string    `json:"kind"`
	Epoch   uint64    `json:"epoch"`
	Missing []string  `json:"missing,omitempty"`
	Now     time.Time `json:"now"`
}

// PolicyCollect is an open collect of a kind.
type PolicyCollect struct {
	Kind   string    `json:"kind"`
	Epoch  uint64    `json:"epoch"`
	Leader string    `json:"leader"`
	At     time.Time `json:"at"`
}

// PolicyAwaiting is a kind whose last collect could not seed.
type PolicyAwaiting struct {
	Kind    string    `json:"kind"`
	Missing []string  `json:"missing,omitempty"`
	At      time.Time `json:"at"`
}

// maxAwaitingMissing bounds the nodes an AwaitAdoption names.
const maxAwaitingMissing = 256

// knownPolicyKind reports a kind this build can seed.
func knownPolicyKind(kind string) bool { return kind == PolicyKindTiering }

// ErrPolicyNotFound: a delete of a policy that does not exist.
var ErrPolicyNotFound = errors.New("policy not found")

// ErrPoliciesNotSeeded: a policy command for a kind not yet seeded. Until a
// kind is seeded its policies live in each node's SQLite.
var ErrPoliciesNotSeeded = errors.New("cluster policies are not in effect for this kind yet")

// IsPolicyNotFound reports ErrPolicyNotFound, typed or as text (a refusal
// forwarded from the leader arrives as text).
func IsPolicyNotFound(err error) bool {
	return err != nil && (errors.Is(err, ErrPolicyNotFound) || strings.Contains(err.Error(), ErrPolicyNotFound.Error()))
}

// clusterPoliciesState is the snapshot form of the flag and the policies. nil
// in an older snapshot, or before the flag was applied.
type clusterPoliciesState struct {
	Enabled    bool                  `json:"enabled"`
	Seeded     []string              `json:"seeded,omitempty"`
	Tiering    []*TieringPolicyEntry `json:"tiering,omitempty"`
	Collecting []*PolicyCollect      `json:"collecting,omitempty"`
	Awaiting   []*PolicyAwaiting     `json:"awaiting,omitempty"`
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

// PoliciesSeeded reports whether kind has been seeded: its policies live here
// from then on.
func (f *ClusterFSM) PoliciesSeeded(kind string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.policiesSeeded[kind]
}

// TieringPolicy returns a copy of database's tiering policy.
func (f *ClusterFSM) TieringPolicy(database string) (TieringPolicyEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.tieringPolicies[database]
	if !ok {
		return TieringPolicyEntry{}, false
	}
	return copyTieringPolicy(e), true
}

// TieringPolicies returns a copy of every tiering policy, by database.
func (f *ClusterFSM) TieringPolicies() []TieringPolicyEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.tieringPoliciesLocked()
}

func copyTieringPolicy(e *TieringPolicyEntry) TieringPolicyEntry {
	c := *e
	if e.HotMaxAgeDays != nil {
		v := *e.HotMaxAgeDays
		c.HotMaxAgeDays = &v
	}
	return c
}

// policyKindReadyLocked refuses a policy command before the flag is on or
// before its kind is seeded.
func (f *ClusterFSM) policyKindReadyLocked(kind string) error {
	if !f.clusterPolicies {
		return errors.New("cluster policies are not enabled")
	}
	if !f.policiesSeeded[kind] {
		return fmt.Errorf("%w: %s", ErrPoliciesNotSeeded, kind)
	}
	return nil
}

func validPolicyDatabase(db string) error {
	if db == "" || len(db) > maxPolicyDatabaseLen {
		return fmt.Errorf("a database name of 1 to %d bytes is required", maxPolicyDatabaseLen)
	}
	return nil
}

func (f *ClusterFSM) applySetTieringPolicy(payload []byte, logIndex uint64) interface{} {
	var p SetTieringPolicyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal set tiering policy payload: %w", err)
	}
	if err := validPolicyDatabase(p.Database); err != nil {
		return fmt.Errorf("set tiering policy: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindTiering); err != nil {
		return fmt.Errorf("set tiering policy: %w", err)
	}
	e := &TieringPolicyEntry{
		ID: int64(logIndex), Database: p.Database, HotOnly: p.HotOnly, HotMaxAgeDays: p.HotMaxAgeDays,
		CreatedAt: p.Now, UpdatedAt: p.Now,
	}
	if prev, ok := f.tieringPolicies[p.Database]; ok {
		// A replace keeps the policy's identity.
		e.ID, e.CreatedAt = prev.ID, prev.CreatedAt
	}
	f.tieringPolicies[p.Database] = e
	return nil
}

func (f *ClusterFSM) applyDeleteTieringPolicy(payload []byte) interface{} {
	var p DeleteTieringPolicyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete tiering policy payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindTiering); err != nil {
		return fmt.Errorf("delete tiering policy: %w", err)
	}
	if _, ok := f.tieringPolicies[p.Database]; !ok {
		return fmt.Errorf("delete tiering policy for %q: %w", p.Database, ErrPolicyNotFound)
	}
	delete(f.tieringPolicies, p.Database)
	return nil
}

// PolicyCollecting returns kind's open collect.
func (f *ClusterFSM) PolicyCollecting(kind string) (PolicyCollect, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, ok := f.policyCollecting[kind]
	if !ok {
		return PolicyCollect{}, false
	}
	return *c, true
}

// PolicyAwaitingAdoption returns kind's state when its last collect could not
// seed.
func (f *ClusterFSM) PolicyAwaitingAdoption(kind string) (PolicyAwaiting, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	a, ok := f.policyAwaiting[kind]
	if !ok {
		return PolicyAwaiting{}, false
	}
	c := *a
	c.Missing = append([]string(nil), a.Missing...)
	return c, true
}

func (f *ClusterFSM) applyCollectPolicies(payload []byte, logIndex uint64) interface{} {
	var p CollectPoliciesPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal collect policies payload: %w", err)
	}
	if !knownPolicyKind(p.Kind) || p.Leader == "" {
		return fmt.Errorf("collect policies: a known kind and the leader are required (kind %q)", p.Kind)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.clusterPolicies {
		return errors.New("collect policies: cluster policies are not enabled")
	}
	if f.policiesSeeded[p.Kind] {
		return nil
	}
	// A newer collect replaces an open one (a leader that changed mid-collect
	// starts over): the older one can no longer seed.
	f.policyCollecting[p.Kind] = &PolicyCollect{Kind: p.Kind, Epoch: logIndex, Leader: p.Leader, At: p.Now}
	delete(f.policyAwaiting, p.Kind)
	return nil
}

func (f *ClusterFSM) applyAwaitAdoption(payload []byte) interface{} {
	var p AwaitAdoptionPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal await adoption payload: %w", err)
	}
	if len(p.Missing) > maxAwaitingMissing {
		p.Missing = p.Missing[:maxAwaitingMissing]
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.policyCollecting[p.Kind]
	if !ok || c.Epoch != p.Epoch {
		return fmt.Errorf("await adoption: no open collect of %q at epoch %d", p.Kind, p.Epoch)
	}
	delete(f.policyCollecting, p.Kind)
	missing := append([]string(nil), p.Missing...)
	sort.Strings(missing)
	f.policyAwaiting[p.Kind] = &PolicyAwaiting{Kind: p.Kind, Missing: missing, At: p.Now}
	return nil
}

func (f *ClusterFSM) applySeedPolicies(payload []byte, logIndex uint64) interface{} {
	var p SeedPoliciesPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal seed policies payload: %w", err)
	}
	if !knownPolicyKind(p.Kind) {
		return fmt.Errorf("seed policies: unknown kind %q", p.Kind)
	}
	seen := make(map[string]bool, len(p.Tiering))
	for _, e := range p.Tiering {
		if err := validPolicyDatabase(e.Database); err != nil {
			return fmt.Errorf("seed policies: %w", err)
		}
		if seen[e.Database] {
			return fmt.Errorf("seed policies: database %q listed twice", e.Database)
		}
		seen[e.Database] = true
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.clusterPolicies {
		return errors.New("seed policies: cluster policies are not enabled")
	}
	if f.policiesSeeded[p.Kind] {
		// Seeded once: a second seed (a leader change mid-seed) is a no-op.
		return nil
	}
	// Only the open collect's own rows: an older leader's seed, from rows
	// read before a newer collect, is refused.
	if c, ok := f.policyCollecting[p.Kind]; !ok || c.Epoch != p.Epoch {
		return fmt.Errorf("seed policies: no open collect of %q at epoch %d", p.Kind, p.Epoch)
	}
	for i := range p.Tiering {
		e := p.Tiering[i]
		c := copyTieringPolicy(&e)
		c.ID = int64(logIndex)
		if c.CreatedAt.IsZero() {
			c.CreatedAt = p.Now
		}
		if c.UpdatedAt.IsZero() {
			c.UpdatedAt = p.Now
		}
		f.tieringPolicies[c.Database] = &c
	}
	f.policiesSeeded[p.Kind] = true
	delete(f.policyCollecting, p.Kind)
	delete(f.policyAwaiting, p.Kind)
	f.logger.Info().Str("kind", p.Kind).Int("policies", len(p.Tiering)).Msg("Cluster policies seeded")
	return nil
}

func (f *ClusterFSM) snapshotClusterPoliciesLocked() *clusterPoliciesState {
	if !f.clusterPolicies {
		return nil
	}
	st := &clusterPoliciesState{Enabled: true}
	for kind, ok := range f.policiesSeeded {
		if ok {
			st.Seeded = append(st.Seeded, kind)
		}
	}
	sort.Strings(st.Seeded)
	for _, e := range f.tieringPoliciesLocked() {
		c := e
		st.Tiering = append(st.Tiering, &c)
	}
	for _, c := range f.policyCollecting {
		cc := *c
		st.Collecting = append(st.Collecting, &cc)
	}
	sort.Slice(st.Collecting, func(i, j int) bool { return st.Collecting[i].Kind < st.Collecting[j].Kind })
	for _, a := range f.policyAwaiting {
		ac := *a
		ac.Missing = append([]string(nil), a.Missing...)
		st.Awaiting = append(st.Awaiting, &ac)
	}
	sort.Slice(st.Awaiting, func(i, j int) bool { return st.Awaiting[i].Kind < st.Awaiting[j].Kind })
	return st
}

// tieringPoliciesLocked is TieringPolicies with f.mu already held.
func (f *ClusterFSM) tieringPoliciesLocked() []TieringPolicyEntry {
	out := make([]TieringPolicyEntry, 0, len(f.tieringPolicies))
	for _, e := range f.tieringPolicies {
		out = append(out, copyTieringPolicy(e))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Database < out[j].Database })
	return out
}

func (f *ClusterFSM) restoreClusterPoliciesLocked(st *clusterPoliciesState) {
	f.clusterPolicies = st != nil && st.Enabled
	f.policiesSeeded = map[string]bool{}
	f.tieringPolicies = map[string]*TieringPolicyEntry{}
	f.policyCollecting = map[string]*PolicyCollect{}
	f.policyAwaiting = map[string]*PolicyAwaiting{}
	if st == nil {
		return
	}
	for _, c := range st.Collecting {
		if c != nil && c.Kind != "" {
			cc := *c
			f.policyCollecting[c.Kind] = &cc
		}
	}
	for _, a := range st.Awaiting {
		if a != nil && a.Kind != "" {
			ac := *a
			ac.Missing = append([]string(nil), a.Missing...)
			f.policyAwaiting[a.Kind] = &ac
		}
	}
	for _, kind := range st.Seeded {
		f.policiesSeeded[kind] = true
	}
	for _, e := range st.Tiering {
		if e != nil && e.Database != "" {
			c := copyTieringPolicy(e)
			f.tieringPolicies[c.Database] = &c
		}
	}
}
