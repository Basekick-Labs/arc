package governance

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

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
}

// Manager orchestrates governance policies and enforcement.
type Manager struct {
	db     *sql.DB
	config *config.GovernanceConfig
	logger zerolog.Logger

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

func (m *Manager) initSchema() error {
	_, err := m.db.Exec(`
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
	`)
	return err
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
	result, err := m.db.ExecContext(ctx, `
		INSERT INTO governance_policies (token_id, rate_limit_per_minute, rate_limit_per_hour,
			max_queries_per_hour, max_queries_per_day, max_rows_per_query, max_scan_duration_sec)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		p.TokenID, p.RateLimitPerMinute, p.RateLimitPerHour,
		p.MaxQueriesPerHour, p.MaxQueriesPerDay, p.MaxRowsPerQuery, p.MaxScanDurationSec)
	if err != nil {
		return nil, fmt.Errorf("failed to create governance policy: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("failed to get inserted policy ID: %w", err)
	}
	p.ID = id
	p.CreatedAt = time.Now()
	p.UpdatedAt = p.CreatedAt

	// Update cache and in-memory trackers
	m.policiesMu.Lock()
	m.policies[p.TokenID] = p
	m.policiesMu.Unlock()

	m.updateTrackersForToken(p.TokenID, p)
	metrics.Get().SetGovernancePoliciesActive(int64(m.policyCount()))

	m.logger.Info().Int64("token_id", p.TokenID).Msg("Governance policy created")
	return p, nil
}

// GetPolicy returns the governance policy for a token, or nil if none exists.
func (m *Manager) GetPolicy(ctx context.Context, tokenID int64) (*Policy, error) {
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
	_, err := m.db.ExecContext(ctx, `
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

	p.UpdatedAt = time.Now()

	m.policiesMu.Lock()
	m.policies[p.TokenID] = p
	m.policiesMu.Unlock()

	m.updateTrackersForToken(p.TokenID, p)

	m.logger.Info().Int64("token_id", p.TokenID).Msg("Governance policy updated")
	return p, nil
}

// DeletePolicy removes the governance policy for a token.
func (m *Manager) DeletePolicy(ctx context.Context, tokenID int64) error {
	_, err := m.db.ExecContext(ctx, `DELETE FROM governance_policies WHERE token_id = ?`, tokenID)
	if err != nil {
		return fmt.Errorf("failed to delete governance policy: %w", err)
	}

	m.policiesMu.Lock()
	delete(m.policies, tokenID)
	m.policiesMu.Unlock()

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

	m.logger.Info().Int64("token_id", tokenID).Msg("Governance policy deleted")
	return nil
}

// ListPolicies returns all governance policies.
func (m *Manager) ListPolicies(ctx context.Context) ([]*Policy, error) {
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
	m.policiesMu.RLock()
	p, ok := m.policies[tokenID]
	m.policiesMu.RUnlock()
	if ok {
		return p
	}

	// Use config defaults
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
// 1). Check and record run under admitMu, so two concurrent queries cannot
// both pass a limit with one slot left.
func (m *Manager) Admit(tokenID int64) *EnforcementResult {
	m.admitMu.Lock()
	defer m.admitMu.Unlock()

	policy := m.getEffectivePolicy(tokenID)
	if policy == nil {
		return &EnforcementResult{Allowed: true}
	}

	type gate struct {
		admits func() (bool, string)
		record func()
		wait   func() int
		kind   string
	}
	var gates []gate
	if policy.RateLimitPerMinute > 0 {
		l := m.getOrCreateMinuteLimiter(tokenID, policy.RateLimitPerMinute)
		reason := fmt.Sprintf("Rate limit exceeded: %d queries per minute", policy.RateLimitPerMinute)
		gates = append(gates, gate{func() (bool, string) { return l.admits(), reason }, l.record, l.RetryAfterSec, KindRateLimit})
	}
	if policy.RateLimitPerHour > 0 {
		l := m.getOrCreateHourLimiter(tokenID, policy.RateLimitPerHour)
		reason := fmt.Sprintf("Rate limit exceeded: %d queries per hour", policy.RateLimitPerHour)
		gates = append(gates, gate{func() (bool, string) { return l.admits(), reason }, l.record, l.RetryAfterSec, KindRateLimit})
	}
	if policy.MaxQueriesPerHour > 0 || policy.MaxQueriesPerDay > 0 {
		t := m.getOrCreateQuotaTracker(tokenID, policy.MaxQueriesPerHour, policy.MaxQueriesPerDay)
		gates = append(gates, gate{t.admits, t.record, t.RetryAfterSec, KindQuota})
	}

	var refused *EnforcementResult
	longest := -1
	for _, g := range gates {
		if ok, reason := g.admits(); !ok {
			if w := g.wait(); w > longest {
				longest = w
				refused = &EnforcementResult{Allowed: false, Reason: reason, Kind: g.kind}
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
	for _, g := range gates {
		g.record()
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

// RecordQueryCompletion records that a query completed for a token.
// Currently a no-op since Admit counts a query when it admits it.
func (m *Manager) RecordQueryCompletion(tokenID int64, rowCount int) {
	// Future: track row counts for analytics
}

// GetTokenUsage returns current usage statistics for a token.
func (m *Manager) GetTokenUsage(tokenID int64) *TokenUsage {
	usage := &TokenUsage{TokenID: tokenID}

	policy := m.getEffectivePolicy(tokenID)

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
	m.quotaTrackers[tokenID] = tracker
	return tracker
}

func (m *Manager) updateTrackersForToken(tokenID int64, p *Policy) {
	// Under admitMu: an Admit in progress finishes on the old limits, and the
	// next one sees the new limits (#1524).
	m.admitMu.Lock()
	defer m.admitMu.Unlock()
	// Update minute limiter
	m.minuteLimitersMu.Lock()
	if limiter, ok := m.minuteLimiters[tokenID]; ok {
		limiter.UpdateLimit(p.RateLimitPerMinute)
	}
	m.minuteLimitersMu.Unlock()

	// Update hour limiter
	m.hourLimitersMu.Lock()
	if limiter, ok := m.hourLimiters[tokenID]; ok {
		limiter.UpdateLimit(p.RateLimitPerHour)
	}
	m.hourLimitersMu.Unlock()

	// Update quota tracker
	m.quotaTrackersMu.Lock()
	if tracker, ok := m.quotaTrackers[tokenID]; ok {
		tracker.UpdateLimits(p.MaxQueriesPerHour, p.MaxQueriesPerDay)
	}
	m.quotaTrackersMu.Unlock()
}

func (m *Manager) policyCount() int {
	m.policiesMu.RLock()
	defer m.policiesMu.RUnlock()
	return len(m.policies)
}
