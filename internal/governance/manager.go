package governance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// ErrPolicyExists is returned by CreatePolicy for a token that already has a
// policy: a token has at most one, changed with UpdatePolicy.
var ErrPolicyExists = errors.New("governance policy already exists for this token")

// ErrPolicyNotFound is returned by UpdatePolicy and DeletePolicy for a token
// with no stored policy.
var ErrPolicyNotFound = errors.New("no governance policy for this token")

// Policy represents a governance policy for a specific token.
type Policy struct {
	ID                 int64     `json:"id"`
	TokenID            int64     `json:"token_id"`
	TokenName          string    `json:"token_name,omitempty"`
	RateLimitPerMinute int       `json:"rate_limit_per_minute"`
	RateLimitPerHour   int       `json:"rate_limit_per_hour"`
	MaxQueriesPerHour  int       `json:"max_queries_per_hour"`
	MaxQueriesPerDay   int       `json:"max_queries_per_day"`
	MaxRowsPerQuery    int       `json:"max_rows_per_query"`
	MaxScanDurationSec int       `json:"max_scan_duration_sec"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// TokenUsage represents current usage stats for a token.
type TokenUsage struct {
	TokenID                   int64     `json:"token_id"`
	QueriesThisHour           int       `json:"queries_this_hour"`
	QueriesThisDay            int       `json:"queries_this_day"`
	HourResetAt               time.Time `json:"hour_reset_at"`
	DayResetAt                time.Time `json:"day_reset_at"`
	RateLimitRemainingPerMin  int       `json:"rate_limit_remaining_per_minute"`
	RateLimitRemainingPerHour int       `json:"rate_limit_remaining_per_hour"`
}

// EnforcementResult is returned when checking if a query is allowed.
type EnforcementResult struct {
	Allowed       bool          `json:"allowed"`
	Reason        string        `json:"reason,omitempty"`
	RetryAfterSec int           `json:"retry_after_sec,omitempty"`
	MaxRows       int           `json:"max_rows,omitempty"`
	MaxDuration   time.Duration `json:"-"`
	// Kind is KindRateLimit or KindQuota on a refusal: the limit with the
	// longest wait.
	Kind string `json:"-"`
}

// ManagerConfig holds configuration for creating a Manager.
type ManagerConfig struct {
	DB     *sql.DB
	Config *config.GovernanceConfig
	Logger zerolog.Logger
	// Cluster: this node is in a cluster, whose state may own the policies
	// (#1627). A table that the cluster marked as its backup copy is then
	// never loaded: until this node's cluster state has caught up, only the
	// defaults apply. A standalone node ignores the marker (a backup restored
	// there loads the table).
	Cluster bool
}

// Manager orchestrates governance policies and enforcement.
type Manager struct {
	db     *sql.DB
	config *config.GovernanceConfig
	logger zerolog.Logger

	// policyWriteMu serializes CreatePolicy, UpdatePolicy and DeletePolicy,
	// each held across its SQLite write and its cache update. Without it a
	// write and its publish could interleave with another write and leave the
	// cache enforcing a policy that is not stored, or not enforcing one that
	// is, until a restart reloads it. Only admin policy writes take it:
	// Admit, GetPolicy and ListPolicies never do, so it costs queries nothing.
	// Lock order: policyWriteMu, then admitMu.
	policyWriteMu sync.Mutex
	// afterPolicyWrite is a test seam: when set, each policy write calls it
	// between its SQLite write and its cache update.
	afterPolicyWrite func(op string)

	// In-memory rate limiters keyed by token ID
	minuteLimiters   map[int64]*slidingWindowCounter
	minuteLimitersMu sync.RWMutex
	hourLimiters     map[int64]*slidingWindowCounter
	hourLimitersMu   sync.RWMutex

	// In-memory quota trackers keyed by token ID
	quotaTrackers   map[int64]*quotaTracker
	quotaTrackersMu sync.RWMutex

	// Policy cache keyed by token ID
	policies   map[int64]*Policy
	policiesMu sync.RWMutex

	// admitMu serialises Admit's check-then-record, and policy updates of the
	// limiters (#1524). Taken before any other lock here; nothing that holds
	// another of these locks takes it.
	admitMu sync.Mutex
	// now is the clock new limiters and trackers read; time.Now outside tests.
	now func() time.Time
	// afterAdmitCheckForTest runs in Admit between checking every limit and
	// recording the query.
	afterAdmitCheckForTest func()

	// cluster: the cluster-wide policies (#1627), see cluster.go.
	cluster clusterState

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// NewManager creates a new governance Manager. It initializes the SQLite schema
// and loads existing policies into the in-memory cache.
func NewManager(cfg *ManagerConfig) (*Manager, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}

	m := &Manager{
		db:             cfg.DB,
		config:         cfg.Config,
		logger:         cfg.Logger.With().Str("component", "governance").Logger(),
		minuteLimiters: make(map[int64]*slidingWindowCounter),
		hourLimiters:   make(map[int64]*slidingWindowCounter),
		quotaTrackers:  make(map[int64]*quotaTracker),
		policies:       make(map[int64]*Policy),
		stopCh:         make(chan struct{}),
		now:            time.Now,
	}

	if err := m.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize governance schema: %w", err)
	}

	if cfg.Cluster {
		owned, err := readOwnedMarker(m.db)
		if err != nil {
			return nil, fmt.Errorf("failed to read the governance cluster marker: %w", err)
		}
		if owned {
			// The table is the cluster's backup copy, possibly stale (a
			// policy deleted while this node was down): never loaded.
			// Defaults only until the cluster state catches up.
			m.cluster.owned.Store(true)
			m.cluster.view.Store(&clusterView{})
			m.logger.Info().Msg("Governance policies are owned by the cluster: defaults only until this node catches up")
			return m, nil
		}
	}

	if err := m.loadPolicies(); err != nil {
		return nil, fmt.Errorf("failed to load governance policies: %w", err)
	}

	return m, nil
}

// Start begins background goroutines for periodic cleanup.
func (m *Manager) Start() {
	m.wg.Add(1)
	go m.cleanupLoop()
	m.logger.Info().
		Int("policies_loaded", len(m.policies)).
		Msg("Query governance started")
}

// Stop gracefully shuts down the manager.
func (m *Manager) Stop() error {
	close(m.stopCh)
	m.wg.Wait()
	m.logger.Info().Msg("Query governance stopped")
	return nil
}

// governancePoliciesTable is the policies table, with its index.
const governancePoliciesTable = `
		CREATE TABLE IF NOT EXISTS governance_policies (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			token_id INTEGER NOT NULL UNIQUE,
			rate_limit_per_minute INTEGER DEFAULT 0,
			rate_limit_per_hour INTEGER DEFAULT 0,
			max_queries_per_hour INTEGER DEFAULT 0,
			max_queries_per_day INTEGER DEFAULT 0,
			max_rows_per_query INTEGER DEFAULT 0,
			max_scan_duration_sec INTEGER DEFAULT 0,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_governance_token_id ON governance_policies(token_id);
	`

func (m *Manager) initSchema() error {
	return EnsureSchema(m.db)
}

func (m *Manager) loadPolicies() error {
	rows, err := m.db.Query(`SELECT id, token_id, rate_limit_per_minute, rate_limit_per_hour,
		max_queries_per_hour, max_queries_per_day, max_rows_per_query, max_scan_duration_sec,
		created_at, updated_at FROM governance_policies`)
	if err != nil {
		return err
	}
	defer rows.Close()

	m.policiesMu.Lock()
	defer m.policiesMu.Unlock()

	for rows.Next() {
		var p Policy
		if err := rows.Scan(&p.ID, &p.TokenID, &p.RateLimitPerMinute, &p.RateLimitPerHour,
			&p.MaxQueriesPerHour, &p.MaxQueriesPerDay, &p.MaxRowsPerQuery, &p.MaxScanDurationSec,
			&p.CreatedAt, &p.UpdatedAt); err != nil {
			return err
		}
		m.policies[p.TokenID] = &p
	}

	metrics.Get().SetGovernancePoliciesActive(int64(len(m.policies)))
	return rows.Err()
}

// cleanupLoop periodically removes stale in-memory entries for tokens that
// haven't been seen recently. Runs every 10 minutes.
func (m *Manager) cleanupLoop() {
	defer m.wg.Done()
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			// No-op for now; in-memory limiters are lazily evicted when policies are deleted.
			// Future: could evict limiters/trackers for tokens with no recent activity.
		}
	}
}

// --- Policy CRUD ---

// CreatePolicy creates a governance policy for a token.
func (m *Manager) CreatePolicy(ctx context.Context, p *Policy) (*Policy, error) {
	return m.CreatePolicyFor(ctx, p, "")
}

// CreatePolicyFor is CreatePolicy for the token whose stored hash has
// fingerprint tokenFP: once the cluster owns the policies, the policy is the
// cluster token's, and tokenFP must be that token's (ErrTokenNotCluster).
func (m *Manager) CreatePolicyFor(ctx context.Context, p *Policy, tokenFP string) (*Policy, error) {
	src, err := m.clusterWrite()
	if err != nil {
		return nil, err
	}
	if src != nil {
		return m.setInCluster(ctx, src, p, true, tokenFP)
	}
	defer m.policyWriteMu.Unlock()
	// token_id is UNIQUE. A second policy for the token inserts nothing
	// rather than failing, so it is told apart from a store failure without
	// matching driver error text.
	result, err := m.db.ExecContext(ctx, `
		INSERT INTO governance_policies (token_id, rate_limit_per_minute, rate_limit_per_hour,
			max_queries_per_hour, max_queries_per_day, max_rows_per_query, max_scan_duration_sec)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(token_id) DO NOTHING`,
		p.TokenID, p.RateLimitPerMinute, p.RateLimitPerHour,
		p.MaxQueriesPerHour, p.MaxQueriesPerDay, p.MaxRowsPerQuery, p.MaxScanDurationSec)
	if err != nil {
		return nil, fmt.Errorf("failed to create governance policy: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to create governance policy: %w", err)
	}
	if inserted == 0 {
		return nil, fmt.Errorf("%w: token %d", ErrPolicyExists, p.TokenID)
	}
	if m.afterPolicyWrite != nil {
		m.afterPolicyWrite("create")
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to get inserted policy ID: %w", err)
	}
	p.ID = id
	p.CreatedAt = time.Now()
	p.UpdatedAt = p.CreatedAt

	// Update cache and in-memory trackers
	m.publishPolicy(p)
	metrics.Get().SetGovernancePoliciesActive(int64(m.policyCount()))

	m.logger.Info().Int64("token_id", p.TokenID).Msg("Governance policy created")
	return p, nil
}

// GetPolicy returns the governance policy for a token, or nil if none exists.
func (m *Manager) GetPolicy(ctx context.Context, tokenID int64) (*Policy, error) {
	return m.GetPolicyFor(ctx, tokenID, "")
}

// GetPolicyFor is GetPolicy for the token with fingerprint tokenFP: once the
// cluster owns the policies, the cluster's policy when tokenFP is its token's,
// and none otherwise; ErrNotCaughtUp before this node caught up.
func (m *Manager) GetPolicyFor(ctx context.Context, tokenID int64, tokenFP string) (*Policy, error) {
	if v := m.cluster.view.Load(); v != nil {
		if !v.caughtUp {
			return nil, ErrNotCaughtUp
		}
		if p := v.policyFor(tokenID, tokenFP); p != nil {
			return copyPolicy(p), nil
		}
		return nil, nil
	}
	m.policiesMu.RLock()
	p, ok := m.policies[tokenID]
	m.policiesMu.RUnlock()
	if ok {
		return p, nil
	}
	return nil, nil
}

// UpdatePolicy updates an existing governance policy.
func (m *Manager) UpdatePolicy(ctx context.Context, p *Policy) (*Policy, error) {
	return m.UpdatePolicyFor(ctx, p, "")
}

// UpdatePolicyFor is UpdatePolicy for the token with fingerprint tokenFP (see
// CreatePolicyFor).
func (m *Manager) UpdatePolicyFor(ctx context.Context, p *Policy, tokenFP string) (*Policy, error) {
	src, err := m.clusterWrite()
	if err != nil {
		return nil, err
	}
	if src != nil {
		return m.setInCluster(ctx, src, p, false, tokenFP)
	}
	defer m.policyWriteMu.Unlock()
	result, err := m.db.ExecContext(ctx, `
		UPDATE governance_policies SET
			rate_limit_per_minute = ?, rate_limit_per_hour = ?,
			max_queries_per_hour = ?, max_queries_per_day = ?,
			max_rows_per_query = ?, max_scan_duration_sec = ?,
			updated_at = CURRENT_TIMESTAMP
		WHERE token_id = ?`,
		p.RateLimitPerMinute, p.RateLimitPerHour,
		p.MaxQueriesPerHour, p.MaxQueriesPerDay,
		p.MaxRowsPerQuery, p.MaxScanDurationSec,
		p.TokenID)
	if err != nil {
		return nil, fmt.Errorf("failed to update governance policy: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to update governance policy: %w", err)
	}
	// No row: there is no policy to update (a DELETE can land between the
	// caller's check and this write). Publishing p would enforce a policy
	// that is not stored, until a restart reloads the cache.
	if updated == 0 {
		return nil, fmt.Errorf("%w: token %d", ErrPolicyNotFound, p.TokenID)
	}
	if m.afterPolicyWrite != nil {
		m.afterPolicyWrite("update")
	}

	p.UpdatedAt = time.Now()

	m.publishPolicy(p)

	m.logger.Info().Int64("token_id", p.TokenID).Msg("Governance policy updated")
	return p, nil
}

// DeletePolicy removes the governance policy for a token.
func (m *Manager) DeletePolicy(ctx context.Context, tokenID int64) error {
	return m.DeletePolicyFor(ctx, tokenID, "")
}

// DeletePolicyFor is DeletePolicy for the token with fingerprint tokenFP (see
// CreatePolicyFor).
func (m *Manager) DeletePolicyFor(ctx context.Context, tokenID int64, tokenFP string) error {
	src, err := m.clusterWrite()
	if err != nil {
		return err
	}
	if src != nil {
		if err := src.Delete(ctx, tokenID, tokenFP); err != nil {
			return err
		}
		m.logger.Info().Int64("token_id", tokenID).Msg("Governance policy deleted cluster-wide")
		return nil
	}
	defer m.policyWriteMu.Unlock()
	result, err := m.db.ExecContext(ctx, `DELETE FROM governance_policies WHERE token_id = ?`, tokenID)
	if err != nil {
		return fmt.Errorf("failed to delete governance policy: %w", err)
	}
	deleted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to delete governance policy: %w", err)
	}

	if m.afterPolicyWrite != nil {
		m.afterPolicyWrite("delete")
	}

	// The in-memory state is cleared even when no row was deleted: whatever
	// it holds for the token is not stored (policyWriteMu keeps a concurrent
	// create from storing one meanwhile), so it must not stay enforced.
	//
	// The cache entry and the limiters go together under admitMu: an Admit
	// that read the policy before this must not recreate a limiter on its
	// limits after it (#1524).
	m.admitMu.Lock()
	defer m.admitMu.Unlock()
	m.policiesMu.Lock()
	_, wasCached := m.policies[tokenID]
	delete(m.policies, tokenID)
	m.policiesMu.Unlock()
	if deleted == 0 && wasCached {
		m.logger.Warn().Int64("token_id", tokenID).
			Msg("Governance policy was enforced from the cache with no stored row; cleared")
	}

	// Remove in-memory trackers
	m.minuteLimitersMu.Lock()
	delete(m.minuteLimiters, tokenID)
	m.minuteLimitersMu.Unlock()

	m.hourLimitersMu.Lock()
	delete(m.hourLimiters, tokenID)
	m.hourLimitersMu.Unlock()

	m.quotaTrackersMu.Lock()
	delete(m.quotaTrackers, tokenID)
	m.quotaTrackersMu.Unlock()

	metrics.Get().SetGovernancePoliciesActive(int64(m.policyCount()))

	if deleted == 0 {
		return fmt.Errorf("%w: token %d", ErrPolicyNotFound, tokenID)
	}
	m.logger.Info().Int64("token_id", tokenID).Msg("Governance policy deleted")
	return nil
}

// ListPolicies returns all governance policies.
func (m *Manager) ListPolicies(ctx context.Context) ([]*Policy, error) {
	if v := m.cluster.view.Load(); v != nil {
		if !v.caughtUp {
			return nil, ErrNotCaughtUp
		}
		result := make([]*Policy, 0, len(v.byToken))
		for _, cp := range v.byToken {
			result = append(result, copyPolicy(cp.policy))
		}
		sort.Slice(result, func(i, j int) bool { return result[i].TokenID < result[j].TokenID })
		return result, nil
	}
	m.policiesMu.RLock()
	defer m.policiesMu.RUnlock()

	result := make([]*Policy, 0, len(m.policies))
	for _, p := range m.policies {
		result = append(result, p)
	}
	return result, nil
}

// --- Enforcement ---

// getEffectivePolicy returns the policy for a token. If no per-token policy exists,
// returns a policy built from the config defaults. Returns nil only if all defaults are 0.
func (m *Manager) getEffectivePolicy(tokenID int64) *Policy {
	return m.effectivePolicy(tokenID, "")
}

// effectivePolicy is getEffectivePolicy for the token with fingerprint
// tokenFP. Once the cluster owns the policies it reads only the cluster's
// view: one atomic load, one map lookup, one compare, no lock.
func (m *Manager) effectivePolicy(tokenID int64, tokenFP string) *Policy {
	if v := m.cluster.view.Load(); v != nil {
		if v.caughtUp {
			if p := v.policyFor(tokenID, tokenFP); p != nil {
				return p
			}
		}
		return m.defaultPolicy(tokenID)
	}
	m.policiesMu.RLock()
	p, ok := m.policies[tokenID]
	m.policiesMu.RUnlock()
	if ok {
		return p
	}
	return m.defaultPolicy(tokenID)
}

// defaultPolicy is this node's governance.default_* as token tokenID's
// policy, or nil when every default is 0.
func (m *Manager) defaultPolicy(tokenID int64) *Policy {
	cfg := m.config
	if cfg.DefaultRateLimitPerMin == 0 && cfg.DefaultRateLimitPerHour == 0 &&
		cfg.DefaultMaxQueriesPerHour == 0 && cfg.DefaultMaxQueriesPerDay == 0 &&
		cfg.DefaultMaxRowsPerQuery == 0 {
		return nil // No restrictions
	}

	return &Policy{
		TokenID:            tokenID,
		RateLimitPerMinute: cfg.DefaultRateLimitPerMin,
		RateLimitPerHour:   cfg.DefaultRateLimitPerHour,
		MaxQueriesPerHour:  cfg.DefaultMaxQueriesPerHour,
		MaxQueriesPerDay:   cfg.DefaultMaxQueriesPerDay,
		MaxRowsPerQuery:    cfg.DefaultMaxRowsPerQuery,
	}
}

// GetCachedPolicy returns the effective policy for a token (from cache or defaults).
func (m *Manager) GetCachedPolicy(tokenID int64) *Policy {
	return m.getEffectivePolicy(tokenID)
}

// GetCachedPolicyFor is GetCachedPolicy for the token with fingerprint
// tokenFP: what AdmitToken applies to it.
func (m *Manager) GetCachedPolicyFor(tokenID int64, tokenFP string) *Policy {
	return m.effectivePolicy(tokenID, tokenFP)
}

// Kinds of limit an EnforcementResult can be refused by.
const (
	KindRateLimit = "rate_limit"
	KindQuota     = "quota"
)

// Admit decides whether the token may run a query now, against every limit
// its policy sets: the per-minute and per-hour rate limits and the hourly and
// daily quotas. A query is counted against all of them only when every one
// admits it (#1524): a refused query spends nothing, so a client that waits
// out the refusal's RetryAfterSec finds every limit as it was told.
//
// A refusal names the limit with the longest wait, which is the one that
// decides when a retry can succeed, and RetryAfterSec is that wait (at least
// 1). On a tie the limit checked first (minute, hour, quota) is named. Check and record run under admitMu, so two concurrent queries cannot
// both pass a limit with one slot left.
func (m *Manager) Admit(tokenID int64) *EnforcementResult {
	return m.AdmitToken(tokenID, "")
}

// AdmitToken is Admit for the token whose stored hash has fingerprint tokenFP
// (TokenInfo.Fingerprint). Once the cluster owns the policies, the token gets
// its cluster policy only when tokenFP is the cluster token's; otherwise this
// node's defaults.
func (m *Manager) AdmitToken(tokenID int64, tokenFP string) *EnforcementResult {
	m.admitMu.Lock()
	defer m.admitMu.Unlock()

	policy := m.effectivePolicy(tokenID, tokenFP)
	if policy == nil {
		return &EnforcementResult{Allowed: true}
	}

	// The gates live in an array on the stack: no allocation per query for
	// the limits a policy sets (#1627, rule 4).
	var buf [3]admitGate
	gates := buf[:0]
	if policy.RateLimitPerMinute > 0 {
		l := m.getOrCreateMinuteLimiter(tokenID, policy.RateLimitPerMinute)
		l.applyLimit(policy.RateLimitPerMinute)
		gates = append(gates, admitGate{window: l, limit: policy.RateLimitPerMinute, per: "minute"})
	}
	if policy.RateLimitPerHour > 0 {
		l := m.getOrCreateHourLimiter(tokenID, policy.RateLimitPerHour)
		l.applyLimit(policy.RateLimitPerHour)
		gates = append(gates, admitGate{window: l, limit: policy.RateLimitPerHour, per: "hour"})
	}
	if policy.MaxQueriesPerHour > 0 || policy.MaxQueriesPerDay > 0 {
		t := m.getOrCreateQuotaTracker(tokenID, policy.MaxQueriesPerHour, policy.MaxQueriesPerDay)
		t.applyLimits(policy.MaxQueriesPerHour, policy.MaxQueriesPerDay)
		gates = append(gates, admitGate{quota: t})
	}

	var refused *EnforcementResult
	longest := -1
	for i := range gates {
		g := &gates[i]
		if ok, reason := g.admits(); !ok {
			if w := g.wait(); w > longest {
				longest = w
				refused = &EnforcementResult{Allowed: false, Reason: reason, Kind: g.kind()}
			}
		}
	}
	if refused != nil {
		refused.RetryAfterSec = max(longest, 1)
		return refused
	}
	if m.afterAdmitCheckForTest != nil {
		m.afterAdmitCheckForTest()
	}
	for i := range gates {
		gates[i].record()
	}

	result := &EnforcementResult{Allowed: true}
	if policy.MaxRowsPerQuery > 0 {
		result.MaxRows = policy.MaxRowsPerQuery
	}
	if policy.MaxScanDurationSec > 0 {
		result.MaxDuration = time.Duration(policy.MaxScanDurationSec) * time.Second
	}
	return result
}

// admitGate is one limit Admit checks: a rate limit (window, with its limit
// and period for the refusal's reason) or the quotas.
type admitGate struct {
	window *slidingWindowCounter
	limit  int
	per    string
	quota  *quotaTracker
}

func (g *admitGate) admits() (bool, string) {
	if g.quota != nil {
		return g.quota.admits()
	}
	if g.window.admits() {
		return true, ""
	}
	return false, fmt.Sprintf("Rate limit exceeded: %d queries per %s", g.limit, g.per)
}

func (g *admitGate) record() {
	if g.quota != nil {
		g.quota.record()
		return
	}
	g.window.record()
}

func (g *admitGate) wait() int {
	if g.quota != nil {
		return g.quota.RetryAfterSec()
	}
	return g.window.RetryAfterSec()
}

func (g *admitGate) kind() string {
	if g.quota != nil {
		return KindQuota
	}
	return KindRateLimit
}

// RecordQueryCompletion records that a query completed for a token.
// Currently a no-op since Admit counts a query when it admits it.
func (m *Manager) RecordQueryCompletion(tokenID int64, rowCount int) {
	// Future: track row counts for analytics
}

// GetTokenUsage returns current usage statistics for a token.
func (m *Manager) GetTokenUsage(tokenID int64) *TokenUsage {
	return m.GetTokenUsageFor(tokenID, "")
}

// GetTokenUsageFor is GetTokenUsage for the token with fingerprint tokenFP.
// Once the cluster owns the policies, a changed limit reaches the token's
// counters at its next query on this node, and the remaining counts until
// then are against the limit last applied; a deleted policy's counters are
// kept (its token falls back to the defaults, counted on the same counters).
func (m *Manager) GetTokenUsageFor(tokenID int64, tokenFP string) *TokenUsage {
	usage := &TokenUsage{TokenID: tokenID}

	policy := m.effectivePolicy(tokenID, tokenFP)

	// Get quota usage
	m.quotaTrackersMu.RLock()
	tracker, ok := m.quotaTrackers[tokenID]
	m.quotaTrackersMu.RUnlock()
	if ok {
		usage.QueriesThisHour, usage.QueriesThisDay, usage.HourResetAt, usage.DayResetAt = tracker.GetUsage()
	} else {
		now := time.Now()
		usage.HourResetAt = now.Truncate(time.Hour).Add(time.Hour)
		usage.DayResetAt = now.Truncate(24 * time.Hour).Add(24 * time.Hour)
	}

	// Get rate limit remaining
	if policy != nil && policy.RateLimitPerMinute > 0 {
		m.minuteLimitersMu.RLock()
		limiter, ok := m.minuteLimiters[tokenID]
		m.minuteLimitersMu.RUnlock()
		if ok {
			usage.RateLimitRemainingPerMin = limiter.Remaining()
		} else {
			usage.RateLimitRemainingPerMin = policy.RateLimitPerMinute
		}
	}

	if policy != nil && policy.RateLimitPerHour > 0 {
		m.hourLimitersMu.RLock()
		limiter, ok := m.hourLimiters[tokenID]
		m.hourLimitersMu.RUnlock()
		if ok {
			usage.RateLimitRemainingPerHour = limiter.Remaining()
		} else {
			usage.RateLimitRemainingPerHour = policy.RateLimitPerHour
		}
	}

	return usage
}

// --- Internal helpers ---

func (m *Manager) getOrCreateMinuteLimiter(tokenID int64, limit int) *slidingWindowCounter {
	m.minuteLimitersMu.RLock()
	limiter, ok := m.minuteLimiters[tokenID]
	m.minuteLimitersMu.RUnlock()
	if ok {
		return limiter
	}

	m.minuteLimitersMu.Lock()
	defer m.minuteLimitersMu.Unlock()
	// Double-check after acquiring write lock
	if limiter, ok = m.minuteLimiters[tokenID]; ok {
		return limiter
	}
	limiter = newSlidingWindowCounterWithClock(time.Minute, 60, limit, m.now)
	limiter.applied = limit
	m.minuteLimiters[tokenID] = limiter
	return limiter
}

func (m *Manager) getOrCreateHourLimiter(tokenID int64, limit int) *slidingWindowCounter {
	m.hourLimitersMu.RLock()
	limiter, ok := m.hourLimiters[tokenID]
	m.hourLimitersMu.RUnlock()
	if ok {
		return limiter
	}

	m.hourLimitersMu.Lock()
	defer m.hourLimitersMu.Unlock()
	if limiter, ok = m.hourLimiters[tokenID]; ok {
		return limiter
	}
	limiter = newSlidingWindowCounterWithClock(time.Hour, 60, limit, m.now)
	limiter.applied = limit
	m.hourLimiters[tokenID] = limiter
	return limiter
}

func (m *Manager) getOrCreateQuotaTracker(tokenID int64, maxPerHour, maxPerDay int) *quotaTracker {
	m.quotaTrackersMu.RLock()
	tracker, ok := m.quotaTrackers[tokenID]
	m.quotaTrackersMu.RUnlock()
	if ok {
		return tracker
	}

	m.quotaTrackersMu.Lock()
	defer m.quotaTrackersMu.Unlock()
	if tracker, ok = m.quotaTrackers[tokenID]; ok {
		return tracker
	}
	tracker = newQuotaTrackerWithClock(maxPerHour, maxPerDay, m.now)
	tracker.appliedHour, tracker.appliedDay = maxPerHour, maxPerDay
	m.quotaTrackers[tokenID] = tracker
	return tracker
}

// publishPolicy puts p in the cache and applies its limits to the token's
// limiters in one step under admitMu, so an Admit sees the policy and the
// limits it enforces together, and two writes for one token cannot leave the
// cache on one and the limiters on the other (#1524).
func (m *Manager) publishPolicy(p *Policy) {
	m.admitMu.Lock()
	defer m.admitMu.Unlock()
	m.policiesMu.Lock()
	m.policies[p.TokenID] = p
	m.policiesMu.Unlock()
	m.updateTrackersForToken(p.TokenID, p)
}

// updateTrackersForToken applies p's limits to the token's limiters. Called
// with admitMu held.
func (m *Manager) updateTrackersForToken(tokenID int64, p *Policy) {
	// Update minute limiter
	m.minuteLimitersMu.Lock()
	if limiter, ok := m.minuteLimiters[tokenID]; ok {
		limiter.applyLimit(p.RateLimitPerMinute)
	}
	m.minuteLimitersMu.Unlock()

	// Update hour limiter
	m.hourLimitersMu.Lock()
	if limiter, ok := m.hourLimiters[tokenID]; ok {
		limiter.applyLimit(p.RateLimitPerHour)
	}
	m.hourLimitersMu.Unlock()

	// Update quota tracker
	m.quotaTrackersMu.Lock()
	if tracker, ok := m.quotaTrackers[tokenID]; ok {
		tracker.applyLimits(p.MaxQueriesPerHour, p.MaxQueriesPerDay)
	}
	m.quotaTrackersMu.Unlock()
}

func (m *Manager) policyCount() int {
	m.policiesMu.RLock()
	defer m.policiesMu.RUnlock()
	return len(m.policies)
}
