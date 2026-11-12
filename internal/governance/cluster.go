package governance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
)

// Governance policies in a cluster (#1627).
//
// Until the cluster adopts the nodes' governance policies, each node enforces
// its own, from its SQLite, as before. Once adopted, the cluster state is the
// only copy that is read: every check, every GET and every list is answered
// from it, and every write is proposed to it. This node's table is a backup
// copy kept equal to it (MirrorClusterPolicies), never read while the cluster
// owns the policies.
//
// The query path reads a view of the cluster state: an immutable map from
// token ID to a prebuilt *Policy, rebuilt by the FSM's policies-changed
// callback (SyncClusterView) and swapped in atomically. Finding a token's
// policy then costs one atomic load, one map lookup and one string compare:
// no lock beyond Admit's own admitMu, no allocation.
//
// A policy applies only to the token it was set for. Token IDs are the
// cluster's log indexes, and a node may keep a pre-cluster local token at the
// same ID, so a request's token gets the policy only when its stored hash
// (TokenInfo.Fingerprint) is the cluster token's; any other token gets this
// node's defaults.

// ClusterSource is the cluster state governance reads and writes once
// adopted. The cluster package implements it.
type ClusterSource interface {
	// Seeded reports whether the cluster holds the governance policies.
	Seeded() bool
	// Collecting reports a collect of the nodes' own policies: a local write
	// now would be missed, so it is refused.
	Collecting() bool
	// Get returns token tokenID's policy from the cluster state.
	Get(tokenID int64) (*Policy, bool)
	// Set creates (create) or replaces token p.TokenID's policy, for the
	// token whose stored hash has fingerprint tokenFP, and returns once this
	// node has applied it.
	Set(ctx context.Context, p *Policy, create bool, tokenFP string) error
	// Delete removes token tokenID's policy, for the token whose stored hash
	// has fingerprint tokenFP.
	Delete(ctx context.Context, tokenID int64, tokenFP string) error
}

// ClusterViewSource is the cluster state the view is built from:
// *raft.ClusterFSM.
type ClusterViewSource interface {
	GovernanceView() (seeded bool, entries []raft.GovernanceViewEntry)
}

// Errors of the cluster-wide policies.
var (
	// ErrPoliciesBeingAdopted: a local write during the cluster's collect.
	ErrPoliciesBeingAdopted = errors.New("governance policies are being adopted by the cluster; retry in a few seconds")
	// ErrNotCaughtUp: the cluster owns the policies and this node's cluster
	// state has not caught up since it started.
	ErrNotCaughtUp = errors.New("governance policies are owned by the cluster and this node has not caught up; retry shortly")
	// ErrTokenNotCluster: a cluster write for a token that is not a cluster
	// token (or a different token than the cluster's at that ID).
	ErrTokenNotCluster = errors.New("governance policies apply to cluster tokens only")
	// ErrInvalidPolicy: the cluster state refused the policy itself.
	ErrInvalidPolicy = errors.New("invalid governance policy")
	// ErrClusterUnavailable: the cluster could not take the write right now.
	ErrClusterUnavailable = errors.New("the cluster could not take the governance policy write right now; retry")
)

// clusterView is the cluster state as the query path reads it. nil (no view)
// is a node whose policies are its own.
type clusterView struct {
	// caughtUp: the cluster state holds the seeded kind. An owned view that
	// has not caught up enforces only this node's defaults.
	caughtUp bool
	byToken  map[int64]*clusterPolicy
}

// clusterPolicy is one policy of the view. Never mutated once published.
type clusterPolicy struct {
	policy  *Policy
	tokenFP string
}

// clusterState is Manager's cluster-mode state.
type clusterState struct {
	source atomic.Pointer[clusterSourceHolder]
	view   atomic.Pointer[clusterView]
	// owned: this node's table is the cluster's backup copy (the marker in
	// governance_cluster_state), so it is never read again.
	owned atomic.Bool
	// buildMu orders view builds, so an older build never replaces a newer
	// one. Held only while building from memory: no I/O, no other lock.
	buildMu sync.Mutex
	// afterViewBuildForTest runs at the end of each build.
	afterViewBuildForTest func()
}

type clusterSourceHolder struct{ src ClusterSource }

// SetClusterSource installs the cluster's copy of the policies (cluster mode).
func (m *Manager) SetClusterSource(src ClusterSource) {
	if src == nil {
		m.cluster.source.Store(nil)
		return
	}
	m.cluster.source.Store(&clusterSourceHolder{src: src})
}

func (m *Manager) clusterSource() ClusterSource {
	if h := m.cluster.source.Load(); h != nil {
		return h.src
	}
	return nil
}

// ClusterMode reports whether a cluster source is installed: the cluster may
// own the policies, and reads and writes then name a token by its ID and the
// fingerprint of its stored hash.
func (m *Manager) ClusterMode() bool {
	return m.clusterSource() != nil
}

// SyncClusterView rebuilds the query path's view from the cluster state. It
// runs on the FSM goroutine after every policy change (and once when wired),
// so it only reads memory and swaps a pointer.
func (m *Manager) SyncClusterView(src ClusterViewSource) {
	m.cluster.buildMu.Lock()
	defer m.cluster.buildMu.Unlock()
	if hook := m.cluster.afterViewBuildForTest; hook != nil {
		defer hook()
	}
	seeded, entries := src.GovernanceView()
	if !seeded {
		if m.cluster.owned.Load() {
			// Owned, not caught up: defaults only, never this table.
			m.cluster.view.Store(&clusterView{})
		} else {
			m.cluster.view.Store(nil)
		}
		return
	}
	byToken := make(map[int64]*clusterPolicy, len(entries))
	for i := range entries {
		e := &entries[i]
		if e.TokenHash == "" {
			continue // no cluster token: it limits no one
		}
		byToken[e.TokenID] = &clusterPolicy{policy: policyFromEntry(e.GovernancePolicyEntry), tokenFP: auth.TokenFingerprint(e.TokenHash)}
	}
	m.cluster.view.Store(&clusterView{caughtUp: true, byToken: byToken})
	metrics.Get().SetGovernancePoliciesActive(int64(len(byToken)))
}

func policyFromEntry(e raft.GovernancePolicyEntry) *Policy {
	return &Policy{
		ID: e.ID, TokenID: e.TokenID,
		RateLimitPerMinute: e.RateLimitPerMinute, RateLimitPerHour: e.RateLimitPerHour,
		MaxQueriesPerHour: e.MaxQueriesPerHour, MaxQueriesPerDay: e.MaxQueriesPerDay,
		MaxRowsPerQuery: e.MaxRowsPerQuery, MaxScanDurationSec: e.MaxScanDurationSec,
		CreatedAt: e.CreatedAt, UpdatedAt: e.UpdatedAt,
	}
}

// PolicyFromEntry is the API's form of a cluster state policy.
func PolicyFromEntry(e raft.GovernancePolicyEntry) *Policy { return policyFromEntry(e) }

// viewPolicy is token tokenID's policy in an owned view, for the token with
// fingerprint fp: nil when it has none (or the fingerprint is not the
// cluster token's).
func (v *clusterView) policyFor(tokenID int64, fp string) *Policy {
	if cp := v.byToken[tokenID]; cp != nil && fp != "" && cp.tokenFP == fp {
		return cp.policy
	}
	return nil
}

// copyPolicy returns a copy the caller may change.
func copyPolicy(p *Policy) *Policy {
	c := *p
	return &c
}

// localWriteLocked decides a local write, with policyWriteMu held: the
// cluster's copy when it holds the policies, a refusal while it collects
// them or while this node has not caught up with the cluster that owns them,
// or neither (write this node's table). Collecting is read before seeded: a
// seed ends the collect and marks the kind seeded in one apply.
func (m *Manager) localWriteLocked() (ClusterSource, error) {
	src := m.clusterSource()
	if src == nil {
		return nil, nil
	}
	if src.Collecting() {
		return nil, ErrPoliciesBeingAdopted
	}
	if src.Seeded() {
		return src, nil
	}
	if m.cluster.owned.Load() {
		return nil, ErrNotCaughtUp
	}
	return nil, nil
}

// clusterWrite takes policyWriteMu and decides the write. With a cluster
// source or an error the lock is already released: a proposal never runs
// under it. Otherwise the caller holds it and must release it.
func (m *Manager) clusterWrite() (ClusterSource, error) {
	m.policyWriteMu.Lock()
	src, err := m.localWriteLocked()
	if err != nil || src != nil {
		m.policyWriteMu.Unlock()
	}
	return src, err
}

func (m *Manager) setInCluster(ctx context.Context, src ClusterSource, p *Policy, create bool, tokenFP string) (*Policy, error) {
	if err := src.Set(ctx, p, create, tokenFP); err != nil {
		return nil, err
	}
	// Report the stored row: the cluster assigns the ID and keeps
	// created_at (the #1619 shape). Set returns once this node applied it.
	stored, ok := src.Get(p.TokenID)
	if !ok {
		return nil, fmt.Errorf("%w: token %d", ErrPolicyNotFound, p.TokenID)
	}
	op := "updated"
	if create {
		op = "created"
	}
	m.logger.Info().Int64("token_id", p.TokenID).Msg("Governance policy " + op + " cluster-wide")
	return stored, nil
}

// --- The backup copy ---

// governanceClusterStateTable marks this node's table as the cluster's
// backup copy once a mirror ran here.
const governanceClusterStateTable = `CREATE TABLE IF NOT EXISTS governance_cluster_state (
	name TEXT PRIMARY KEY,
	value TEXT NOT NULL
)`

// sqliteTimestamp is the layout SQLite's CURRENT_TIMESTAMP writes.
const sqliteTimestamp = "2006-01-02 15:04:05"

// MirrorClusterPolicies makes this node's table equal the cluster's
// governance policies once adopted (a no-op before), and marks it as the
// cluster's, so a restart never enforces it again. It writes nothing when the
// table already matches. Run on a worker, never on the FSM goroutine.
func (m *Manager) MirrorClusterPolicies(ctx context.Context, src ClusterViewSource) error {
	m.policyWriteMu.Lock()
	defer m.policyWriteMu.Unlock()
	owned, err := MirrorTable(ctx, m.db, src)
	if owned {
		m.cluster.owned.Store(true)
	}
	return err
}

// EnsureSchema creates the governance tables in db if they are missing.
func EnsureSchema(db *sql.DB) error {
	if _, err := db.Exec(governancePoliciesTable); err != nil {
		return err
	}
	_, err := db.Exec(governanceClusterStateTable)
	return err
}

// MirrorTable makes db's governance table equal the cluster's policies once
// adopted, and marks it as the cluster's backup copy; owned reports that it
// is. It writes nothing when the table already matches. Also run on a node
// where governance does not run (#1627), so every node's table is the
// cluster's: one turned back on later never enforces policies the cluster
// dropped.
func MirrorTable(ctx context.Context, db *sql.DB, src ClusterViewSource) (owned bool, err error) {
	seeded, entries := src.GovernanceView()
	if !seeded {
		return false, nil
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].TokenID < entries[j].TokenID })
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("mirror governance policies: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	same, err := mirrorMatches(ctx, tx, entries)
	if err != nil {
		return false, fmt.Errorf("mirror governance policies: %w", err)
	}
	if same {
		return true, nil
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM governance_policies`); err != nil {
		return false, fmt.Errorf("mirror governance policies: %w", err)
	}
	for _, e := range entries {
		if _, err := tx.ExecContext(ctx, `INSERT INTO governance_policies (id, token_id, rate_limit_per_minute, rate_limit_per_hour,
			max_queries_per_hour, max_queries_per_day, max_rows_per_query, max_scan_duration_sec, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			e.ID, e.TokenID, e.RateLimitPerMinute, e.RateLimitPerHour, e.MaxQueriesPerHour, e.MaxQueriesPerDay,
			e.MaxRowsPerQuery, e.MaxScanDurationSec,
			e.CreatedAt.UTC().Format(sqliteTimestamp), e.UpdatedAt.UTC().Format(sqliteTimestamp)); err != nil {
			return false, fmt.Errorf("mirror governance policies: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO governance_cluster_state (name, value) VALUES ('owned', '1')`); err != nil {
		return false, fmt.Errorf("mirror governance policies: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("mirror governance policies: %w", err)
	}
	return true, nil
}

// mirrorMatches reports whether the table already holds exactly entries
// (sorted by token) and carries the owned marker.
func mirrorMatches(ctx context.Context, tx *sql.Tx, entries []raft.GovernanceViewEntry) (bool, error) {
	var marked int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM governance_cluster_state WHERE name = 'owned'`).Scan(&marked); err != nil {
		return false, err
	}
	if marked == 0 {
		return false, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT id, token_id, rate_limit_per_minute, rate_limit_per_hour, max_queries_per_hour,
		max_queries_per_day, max_rows_per_query, max_scan_duration_sec, created_at, updated_at
		FROM governance_policies ORDER BY token_id`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	i := 0
	for rows.Next() {
		var p Policy
		var created, updated sql.NullTime
		if err := rows.Scan(&p.ID, &p.TokenID, &p.RateLimitPerMinute, &p.RateLimitPerHour, &p.MaxQueriesPerHour,
			&p.MaxQueriesPerDay, &p.MaxRowsPerQuery, &p.MaxScanDurationSec, &created, &updated); err != nil {
			return false, nil // a row that does not read is rewritten
		}
		if i >= len(entries) {
			return false, nil
		}
		e := entries[i].GovernancePolicyEntry
		i++
		if p.ID != e.ID || p.TokenID != e.TokenID || p.RateLimitPerMinute != e.RateLimitPerMinute ||
			p.RateLimitPerHour != e.RateLimitPerHour || p.MaxQueriesPerHour != e.MaxQueriesPerHour ||
			p.MaxQueriesPerDay != e.MaxQueriesPerDay || p.MaxRowsPerQuery != e.MaxRowsPerQuery ||
			p.MaxScanDurationSec != e.MaxScanDurationSec ||
			!created.Time.Equal(e.CreatedAt.UTC().Truncate(time.Second)) ||
			!updated.Time.Equal(e.UpdatedAt.UTC().Truncate(time.Second)) {
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return i == len(entries), nil
}

// readOwnedMarker reports whether this node's table is the cluster's backup
// copy.
func readOwnedMarker(db *sql.DB) (bool, error) {
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM governance_cluster_state WHERE name = 'owned'`).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// --- Adoption ---

// Token states a node reports for a policy's token.
const (
	TokenPresent = "present" // an api_tokens row with that ID exists here
	TokenAbsent  = "absent"  // this node's api_tokens has no such row
	TokenUnknown = "unknown" // this node cannot tell (no api_tokens table)
)

// LocalRow is one of a node's own governance policies, as it answers the
// cluster's adoption, with what the node knows of the policy's token.
type LocalRow struct {
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
	// Token is TokenPresent, TokenAbsent or TokenUnknown.
	Token string `json:"token"`
	// RaftMaterialised: the token row is the cluster's (written from the
	// cluster state). TokenFP: the fingerprint of its stored hash.
	RaftMaterialised bool   `json:"raft_materialised,omitempty"`
	TokenFP          string `json:"token_fp,omitempty"`
}

// Defaults are a node's governance.default_* settings.
type Defaults struct {
	RateLimitPerMin   int `json:"rate_limit_per_min"`
	RateLimitPerHour  int `json:"rate_limit_per_hour"`
	MaxQueriesPerHour int `json:"max_queries_per_hour"`
	MaxQueriesPerDay  int `json:"max_queries_per_day"`
	MaxRowsPerQuery   int `json:"max_rows_per_query"`
}

// LocalRows is one node's answer for the governance kind.
type LocalRows struct {
	Policies []LocalRow `json:"policies"`
	// MaxIssuedID is the highest policy ID the node ever issued.
	MaxIssuedID int64 `json:"max_issued_id,omitempty"`
	// Defaults: the node's defaults, when governance runs there.
	Defaults *Defaults `json:"defaults,omitempty"`
}

// LocalPolicyRows reads this node's own policies for the cluster's adoption,
// under policyWriteMu, so a local write is either in the answer or refused
// by the collect.
func (m *Manager) LocalPolicyRows(ctx context.Context) (LocalRows, error) {
	m.policyWriteMu.Lock()
	defer m.policyWriteMu.Unlock()
	out, err := ReadLocalRows(ctx, m.db)
	if err != nil {
		return out, err
	}
	cfg := m.config
	out.Defaults = &Defaults{
		RateLimitPerMin: cfg.DefaultRateLimitPerMin, RateLimitPerHour: cfg.DefaultRateLimitPerHour,
		MaxQueriesPerHour: cfg.DefaultMaxQueriesPerHour, MaxQueriesPerDay: cfg.DefaultMaxQueriesPerDay,
		MaxRowsPerQuery: cfg.DefaultMaxRowsPerQuery,
	}
	return out, nil
}

// ReadLocalRows reads the governance policies in db, with what db's
// api_tokens says of each policy's token. No governance table is no
// policies.
func ReadLocalRows(ctx context.Context, db *sql.DB) (LocalRows, error) {
	out := LocalRows{Policies: []LocalRow{}}
	tables := map[string]bool{}
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('governance_policies', 'api_tokens')`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return out, err
		}
		tables[n] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	if !tables["governance_policies"] {
		return out, nil
	}
	prows, err := db.QueryContext(ctx, `SELECT id, token_id, rate_limit_per_minute, rate_limit_per_hour, max_queries_per_hour,
		max_queries_per_day, max_rows_per_query, max_scan_duration_sec, created_at, updated_at
		FROM governance_policies ORDER BY id`)
	if err != nil {
		return out, err
	}
	for prows.Next() {
		var r LocalRow
		var created, updated sql.NullTime
		var a, b, c, d, e, f sql.NullInt64
		if err := prows.Scan(&r.ID, &r.TokenID, &a, &b, &c, &d, &e, &f, &created, &updated); err != nil {
			prows.Close()
			// Not skipped: a row missing from the answer would be dropped.
			return out, fmt.Errorf("read governance policy row: %w", err)
		}
		r.RateLimitPerMinute, r.RateLimitPerHour, r.MaxQueriesPerHour = int(a.Int64), int(b.Int64), int(c.Int64)
		r.MaxQueriesPerDay, r.MaxRowsPerQuery, r.MaxScanDurationSec = int(d.Int64), int(e.Int64), int(f.Int64)
		r.CreatedAt, r.UpdatedAt = created.Time.UTC(), updated.Time.UTC()
		out.Policies = append(out.Policies, r)
	}
	prows.Close()
	if err := prows.Err(); err != nil {
		return out, err
	}
	for i := range out.Policies {
		r := &out.Policies[i]
		if !tables["api_tokens"] {
			r.Token = TokenUnknown
			continue
		}
		var hash string
		var materialised sql.NullBool
		err := db.QueryRowContext(ctx, `SELECT token_hash, raft_materialised FROM api_tokens WHERE id = ?`, r.TokenID).Scan(&hash, &materialised)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			r.Token = TokenAbsent
		case err != nil:
			return out, fmt.Errorf("read the token of governance policy %d: %w", r.ID, err)
		default:
			r.Token = TokenPresent
			r.RaftMaterialised = materialised.Valid && materialised.Bool
			r.TokenFP = auth.TokenFingerprint(hash)
		}
	}
	var seq sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name = 'governance_policies'`).Scan(&seq); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	out.MaxIssuedID = seq.Int64
	if len(out.Policies) > raft.MaxSeedGovernancePolicies {
		return out, fmt.Errorf("%d governance policies exceed the %d the cluster adopts", len(out.Policies), raft.MaxSeedGovernancePolicies)
	}
	return out, nil
}
