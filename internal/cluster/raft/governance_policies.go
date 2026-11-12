package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Query governance policies, cluster-wide (#1627).
//
// A governance policy limits one API token's queries: rate limits, quotas, a
// row cap and a scan-duration cap. Each node used to hold its own in SQLite,
// so a policy set on one node did not apply on another. With the governance
// flag on and the kind seeded, the policies live here, one per cluster token,
// keyed by the token's ID, which is its FSM ID (a log index): every entry's
// token exists in f.tokens, and a token delete drops its entry. Existence is
// all the FSM can check. That the row a node holds for token N was written
// for this cluster token N, and not for a pre-cluster local token that
// happens to share the ID, is decided before the seed, by the leader's merge
// (#1627 part 2), from each node's report of the token's materialisation and
// hash fingerprint.
//
// Governance has its own flag and capability, for the reason retention does:
// a build without them would refuse these commands and drop this state from
// its snapshots.

// CapabilityClusterGovernance is advertised at join by every build that
// applies the governance policy commands.
const CapabilityClusterGovernance = "cluster_policies_governance_v1"

// PolicyKindGovernance is the query governance kind.
const PolicyKindGovernance = "governance"

// MaxSeedGovernancePolicies bounds the governance policies one seed installs
// (and one node's answer to a collect): about 250 bytes each as JSON.
const MaxSeedGovernancePolicies = 2000

// GovernancePolicyEntry is one token's governance policy. ID is the policy's
// own number (never 0); TokenID, the cluster token it limits, is its key.
// Every limit is 0 for unlimited.
type GovernancePolicyEntry struct {
	ID                 int64     `json:"id"`
	TokenID            int64     `json:"token_id"`
	RateLimitPerMinute int       `json:"rate_limit_per_minute"`
	RateLimitPerHour   int       `json:"rate_limit_per_hour"`
	MaxQueriesPerHour  int       `json:"max_queries_per_hour"`
	MaxQueriesPerDay   int       `json:"max_queries_per_day"`
	MaxRowsPerQuery    int       `json:"max_rows_per_query"`
	MaxScanDurationSec int       `json:"max_scan_duration_sec"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// SetGovernancePolicyPayload creates (Create) or replaces a token's policy.
// A create of a token that has one is ErrGovernancePolicyExists; a replace of
// one that has none is ErrPolicyNotFound: the API's 409 and 404 (#1601).
// Decided here, against the state the command applies to, so two nodes'
// writes cannot both pass a check made before either applied.
type SetGovernancePolicyPayload struct {
	TokenID            int64     `json:"token_id"`
	Create             bool      `json:"create,omitempty"`
	RateLimitPerMinute int       `json:"rate_limit_per_minute"`
	RateLimitPerHour   int       `json:"rate_limit_per_hour"`
	MaxQueriesPerHour  int       `json:"max_queries_per_hour"`
	MaxQueriesPerDay   int       `json:"max_queries_per_day"`
	MaxRowsPerQuery    int       `json:"max_rows_per_query"`
	MaxScanDurationSec int       `json:"max_scan_duration_sec"`
	Now                time.Time `json:"now"`
}

// DeleteGovernancePolicyPayload removes a token's policy.
type DeleteGovernancePolicyPayload struct {
	TokenID int64 `json:"token_id"`
}

// EnableGovernancePoliciesPayload turns the governance flag on; Nodes as in
// EnableClusterPoliciesPayload.
type EnableGovernancePoliciesPayload = EnablePolicyKindPayload

// ErrGovernancePolicyExists: a create for a token that already has a policy.
var ErrGovernancePolicyExists = errors.New("a governance policy already exists for this token")

// ErrGovernanceTokenNotCluster: a policy for a token the cluster state does
// not hold. After adoption, governance policies apply to cluster tokens only.
var ErrGovernanceTokenNotCluster = errors.New("governance policies apply to cluster tokens only")

// ErrInvalidGovernancePolicy: a governance policy the cluster state refuses.
var ErrInvalidGovernancePolicy = errors.New("invalid governance policy")

// IsGovernancePolicyExists / IsGovernanceTokenNotCluster report those errors,
// typed or as text (a refusal forwarded from the leader arrives as text).
func IsGovernancePolicyExists(err error) bool {
	return err != nil && (errors.Is(err, ErrGovernancePolicyExists) || strings.Contains(err.Error(), ErrGovernancePolicyExists.Error()))
}

func IsGovernanceTokenNotCluster(err error) bool {
	return err != nil && (errors.Is(err, ErrGovernanceTokenNotCluster) || strings.Contains(err.Error(), ErrGovernanceTokenNotCluster.Error()))
}

// GovernancePoliciesEnabled reports whether the governance flag is applied.
func (f *ClusterFSM) GovernancePoliciesEnabled() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.clusterPolicies && f.governanceEnabled
}

// GovernancePolicy returns a copy of token tokenID's governance policy.
func (f *ClusterFSM) GovernancePolicy(tokenID int64) (GovernancePolicyEntry, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.governancePolicies[tokenID]
	if !ok {
		return GovernancePolicyEntry{}, false
	}
	return *e, true
}

// GovernancePolicies returns a copy of every governance policy, by token ID.
func (f *ClusterFSM) GovernancePolicies() []GovernancePolicyEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.governancePoliciesLocked()
}

func (f *ClusterFSM) governancePoliciesLocked() []GovernancePolicyEntry {
	out := make([]GovernancePolicyEntry, 0, len(f.governancePolicies))
	for _, e := range f.governancePolicies {
		out = append(out, *e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TokenID < out[j].TokenID })
	return out
}

// validGovernanceLimits applies the rules that never change between
// releases: a token, and no negative limit.
func validGovernanceLimits(tokenID int64, limits ...int) error {
	if tokenID <= 0 {
		return fmt.Errorf("%w: a positive token_id is required", ErrInvalidGovernancePolicy)
	}
	for _, v := range limits {
		if v < 0 {
			return fmt.Errorf("%w: limits must be non-negative (0 = unlimited)", ErrInvalidGovernancePolicy)
		}
	}
	return nil
}

func (e *GovernancePolicyEntry) limits() []int {
	return []int{e.RateLimitPerMinute, e.RateLimitPerHour, e.MaxQueriesPerHour, e.MaxQueriesPerDay, e.MaxRowsPerQuery, e.MaxScanDurationSec}
}

func (f *ClusterFSM) applySetGovernancePolicy(payload []byte) interface{} {
	var p SetGovernancePolicyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal set governance policy payload: %w", err)
	}
	next := GovernancePolicyEntry{
		TokenID: p.TokenID, RateLimitPerMinute: p.RateLimitPerMinute, RateLimitPerHour: p.RateLimitPerHour,
		MaxQueriesPerHour: p.MaxQueriesPerHour, MaxQueriesPerDay: p.MaxQueriesPerDay,
		MaxRowsPerQuery: p.MaxRowsPerQuery, MaxScanDurationSec: p.MaxScanDurationSec,
		CreatedAt: p.Now, UpdatedAt: p.Now,
	}
	if err := validGovernanceLimits(next.TokenID, next.limits()...); err != nil {
		return fmt.Errorf("set governance policy: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindGovernance); err != nil {
		return fmt.Errorf("set governance policy: %w", err)
	}
	if !f.governanceEnabled {
		return errors.New("set governance policy: governance policies are not enabled")
	}
	if _, ok := f.tokens[p.TokenID]; !ok {
		return fmt.Errorf("set governance policy for token %d: %w", p.TokenID, ErrGovernanceTokenNotCluster)
	}
	prev, exists := f.governancePolicies[p.TokenID]
	switch {
	case p.Create && exists:
		return fmt.Errorf("set governance policy for token %d: %w", p.TokenID, ErrGovernancePolicyExists)
	case !p.Create && !exists:
		return fmt.Errorf("set governance policy for token %d: %w", p.TokenID, ErrPolicyNotFound)
	case exists:
		// A replace keeps the policy's identity.
		next.ID, next.CreatedAt = prev.ID, prev.CreatedAt
	default:
		f.governanceSeq++
		next.ID = f.governanceSeq
	}
	f.governancePolicies[p.TokenID] = &next
	return nil
}

func (f *ClusterFSM) applyDeleteGovernancePolicy(payload []byte) interface{} {
	var p DeleteGovernancePolicyPayload
	if err := json.Unmarshal(payload, &p); err != nil {
		return fmt.Errorf("failed to unmarshal delete governance policy payload: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.policyKindReadyLocked(PolicyKindGovernance); err != nil {
		return fmt.Errorf("delete governance policy: %w", err)
	}
	if _, ok := f.governancePolicies[p.TokenID]; !ok {
		return fmt.Errorf("delete governance policy for token %d: %w", p.TokenID, ErrPolicyNotFound)
	}
	delete(f.governancePolicies, p.TokenID)
	return nil
}

// dropGovernancePolicyLocked removes token tokenID's policy, if any, when the
// token goes: a policy never outlives its token. Reports whether one went.
func (f *ClusterFSM) dropGovernancePolicyLocked(tokenID int64) bool {
	if _, ok := f.governancePolicies[tokenID]; !ok {
		return false
	}
	delete(f.governancePolicies, tokenID)
	return true
}

// dropOrphanGovernancePoliciesLocked removes the policies whose token the
// state does not hold. Restore runs it once the tokens are in place.
func (f *ClusterFSM) dropOrphanGovernancePoliciesLocked() {
	for tokenID := range f.governancePolicies {
		if _, ok := f.tokens[tokenID]; !ok {
			delete(f.governancePolicies, tokenID)
			f.logger.Warn().Int64("token_id", tokenID).Msg("Snapshot restore: dropped a governance policy whose token is not in the cluster")
		}
	}
}

// validGovernanceSeed checks a governance seed before it is applied: the
// bound, one row per token, valid limits, and with kept IDs, unique IDs below
// the floor.
func validGovernanceSeed(p *SeedPoliciesPayload) error {
	if len(p.Tiering) > 0 || len(p.Retention) > 0 || len(p.CQ) > 0 {
		return errors.New("tiering, retention or continuous query rows in a governance seed")
	}
	if len(p.Governance) > MaxSeedGovernancePolicies {
		return fmt.Errorf("%d governance policies exceed %d", len(p.Governance), MaxSeedGovernancePolicies)
	}
	if p.GovernanceFloor < 1 {
		return errors.New("a governance seed needs a floor of at least 1")
	}
	tokens := make(map[int64]bool, len(p.Governance))
	ids := make(map[int64]bool, len(p.Governance))
	for i := range p.Governance {
		e := &p.Governance[i]
		if err := validGovernanceLimits(e.TokenID, e.limits()...); err != nil {
			return fmt.Errorf("governance policy for token %d: %w", e.TokenID, err)
		}
		if tokens[e.TokenID] {
			return fmt.Errorf("governance policy for token %d listed twice", e.TokenID)
		}
		tokens[e.TokenID] = true
		if p.GovernanceKeepIDs {
			if e.ID <= 0 || e.ID >= p.GovernanceFloor || ids[e.ID] {
				return fmt.Errorf("governance policy for token %d: kept ID %d must be unique and in [1, %d)", e.TokenID, e.ID, p.GovernanceFloor)
			}
			ids[e.ID] = true
		}
	}
	return nil
}

// seedGovernanceLocked installs a validated governance seed. A row whose
// token is no longer in the cluster state (deleted after the leader read the
// nodes' rows) is dropped: it would limit no one, and no token delete is left
// to remove it.
func (f *ClusterFSM) seedGovernanceLocked(p *SeedPoliciesPayload) {
	f.governancePolicies = make(map[int64]*GovernancePolicyEntry, len(p.Governance))
	if f.governanceSeq < p.GovernanceFloor-1 {
		f.governanceSeq = p.GovernanceFloor - 1
	}
	var dropped []int64
	for i := range p.Governance {
		c := p.Governance[i]
		if _, ok := f.tokens[c.TokenID]; !ok {
			dropped = append(dropped, c.TokenID)
			continue
		}
		if !p.GovernanceKeepIDs {
			f.governanceSeq++
			c.ID = f.governanceSeq
		}
		if c.CreatedAt.IsZero() {
			c.CreatedAt = p.Now
		}
		if c.UpdatedAt.IsZero() {
			c.UpdatedAt = p.Now
		}
		f.governancePolicies[c.TokenID] = &c
	}
	if len(dropped) > 0 {
		f.logger.Warn().Interface("token_ids", dropped).
			Msg("Governance seed: dropped the policies of tokens no longer in the cluster")
	}
}

func (f *ClusterFSM) snapshotGovernanceLocked(st *clusterPoliciesState) {
	if !f.governanceEnabled {
		return
	}
	st.GovernanceEnabled = true
	st.GovernanceSeq = f.governanceSeq
	for _, e := range f.governancePoliciesLocked() {
		c := e
		st.Governance = append(st.Governance, &c)
	}
}

func (f *ClusterFSM) restoreGovernanceLocked(st *clusterPoliciesState) {
	f.governanceEnabled = false
	f.governancePolicies = map[int64]*GovernancePolicyEntry{}
	f.governanceSeq = 0
	if st == nil {
		return
	}
	f.governanceEnabled = st.GovernanceEnabled
	f.governanceSeq = st.GovernanceSeq
	for _, e := range st.Governance {
		if e != nil && e.TokenID > 0 && e.ID > 0 {
			c := *e
			f.governancePolicies[c.TokenID] = &c
		}
	}
}
