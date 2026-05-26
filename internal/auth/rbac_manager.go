package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/license"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
)

// Pattern validation regex - allows alphanumeric, underscore, hyphen, and wildcards
var patternValidationRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+\*?$|^\*[a-zA-Z0-9_-]*$|^\*$`)

// Name validation regex - must start with letter, alphanumeric + underscore/hyphen, max 64 chars
var nameValidationRegex = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,63}$`)

// validatePattern validates database or measurement patterns
func validatePattern(pattern string) error {
	if pattern == "" {
		return errors.New("pattern cannot be empty")
	}
	if !patternValidationRegex.MatchString(pattern) {
		return errors.New("invalid pattern: use alphanumeric characters, underscores, hyphens, with optional trailing or leading wildcard (*)")
	}
	return nil
}

// validateName validates organization and team names
func validateName(name string) error {
	if name == "" {
		return errors.New("name cannot be empty")
	}
	if !nameValidationRegex.MatchString(name) {
		return errors.New("name must start with a letter, contain only alphanumeric characters, underscores, or hyphens, and be at most 64 characters")
	}
	return nil
}

// =============================================================================
// RBAC Cache Types
// =============================================================================

// tokenRBACData holds all preloaded RBAC data for a token
type tokenRBACData struct {
	teams     []Team
	roles     map[int64][]Role                  // teamID -> roles
	measPerms map[int64][]MeasurementPermission // roleID -> measurement permissions
	loadedAt  time.Time
}

// permissionCacheKey uniquely identifies a permission check
type permissionCacheKey struct {
	tokenID     int64
	database    string
	measurement string
	permission  string
}

// permissionCacheEntry caches a permission check result
type permissionCacheEntry struct {
	result    *PermissionCheckResult
	expiresAt time.Time
}

// RBACManager handles role-based access control operations
type RBACManager struct {
	db            *sql.DB
	licenseClient *license.Client
	logger        zerolog.Logger

	// Token RBAC data cache (teams/roles/permissions preloaded)
	tokenCache    map[int64]*tokenRBACData
	tokenCacheMu  sync.RWMutex
	tokenCacheTTL time.Duration

	// Permission result cache
	permCache    map[permissionCacheKey]*permissionCacheEntry
	permCacheMu  sync.RWMutex
	permCacheTTL time.Duration

	// Maximum cache sizes — oldest entries evicted when exceeded
	maxCacheSize int

	// Cache stats
	cacheHits   atomic.Int64
	cacheMisses atomic.Int64

	// Phase A.1: Cluster Auth Convergence (RBAC). Same shape as
	// AuthManager.proposer: nil in OSS / standalone (direct-SQLite
	// path), non-nil in cluster mode (every write proposes via Raft).
	// Guarded by proposerMu so SetRaftProposer can be called
	// concurrently with in-flight writes.
	proposer   RaftProposer
	proposerMu sync.RWMutex

	// Phase A.2 Item 2: cascade-on-delete soft cap. DeleteOrganization
	// and DeleteTeam in cluster mode pre-check the descendant count
	// against this cap before proposing. 0 = disabled (no cap, used in
	// OSS and as an explicit operator escape hatch). Read-only after
	// construction.
	maxCascadeDescendants int

	// proposerNowUnixNano holds the last UnixNano returned by
	// nextProposerTimestamp(). It is updated via a CAS loop so two
	// concurrent callers are guaranteed to receive strictly-increasing
	// timestamps. Background: the round-9 G28 fix matches the
	// CreateRole / CreateMeasurementPermission read-back by
	// `created_at = proposer's now` to disambiguate concurrent identical
	// creates. time.Now() alone can collide at the same nanosecond
	// under heavy concurrency on fast machines, producing duplicate
	// created_at values and the same race the fix was supposed to
	// eliminate. The CAS loop returns max(prev+1, time.Now().UnixNano())
	// so concurrent callers get distinct, monotonic stamps. The
	// trade-off is a sub-microsecond drift from wall clock under heavy
	// concurrency, which is fine — `created_at` is "when the proposer
	// issued this command," not "exact wall clock." Phase A.2 follow-up
	// to PR #458 round 9 G28.
	proposerNowUnixNano atomic.Int64

	// Shutdown
	done chan struct{}
}

// RBACManagerConfig holds configuration for the RBAC manager
type RBACManagerConfig struct {
	DB            *sql.DB
	LicenseClient *license.Client
	Logger        zerolog.Logger
	CacheTTL      time.Duration // TTL for permission cache (default: 30s)
	MaxCacheSize  int           // Max entries per cache (default: 10000)
	// MaxCascadeDescendants caps DeleteOrganization / DeleteTeam in
	// cluster mode. 0 = disabled (no cap; recommended for OSS / tests
	// that don't care about the cap). Production wiring in
	// cmd/arc/main.go reads cluster.rbac.max_cascade_descendants
	// (default 50000) and passes it here. Phase A.2 Item 2.
	MaxCascadeDescendants int
}

// NewRBACManager creates a new RBAC manager
func NewRBACManager(cfg *RBACManagerConfig) *RBACManager {
	cacheTTL := cfg.CacheTTL
	if cacheTTL == 0 {
		cacheTTL = 30 * time.Second // Default 30s cache
	}

	maxCacheSize := cfg.MaxCacheSize
	if maxCacheSize == 0 {
		maxCacheSize = 10000 // Default 10k entries per cache
	}

	rm := &RBACManager{
		db:            cfg.DB,
		licenseClient: cfg.LicenseClient,
		logger:        cfg.Logger.With().Str("component", "rbac").Logger(),
		tokenCache:    make(map[int64]*tokenRBACData),
		tokenCacheTTL: cacheTTL,
		permCache:     make(map[permissionCacheKey]*permissionCacheEntry),
		permCacheTTL:  cacheTTL,
		maxCacheSize:  maxCacheSize,
		// Negative values normalise to 0 (disabled). The cap check is
		// already gated on `> 0`, but normalising here keeps the field
		// canonical so a future metric export or comparison can't
		// surprise on "-1 means disabled" vs "0 means disabled."
		// Gemini PR #459 round 3.
		maxCascadeDescendants: max(0, cfg.MaxCascadeDescendants),
		done:                  make(chan struct{}),
	}

	// Start background cache cleanup
	go rm.cacheCleanupLoop()

	return rm
}

// Close stops the background cleanup goroutine.
func (rm *RBACManager) Close() error {
	close(rm.done)
	return nil
}

// cacheCleanupLoop periodically cleans expired cache entries
func (rm *RBACManager) cacheCleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rm.cleanupExpiredCache()
		case <-rm.done:
			return
		}
	}
}

// evictPermCacheIfFull removes a random entry from the permission cache if it
// has reached maxCacheSize. Caller must hold permCacheMu write lock.
func (rm *RBACManager) evictPermCacheIfFull() {
	if len(rm.permCache) >= rm.maxCacheSize {
		for k := range rm.permCache {
			delete(rm.permCache, k)
			break
		}
	}
}

// evictTokenCacheIfFull removes a random entry from the token cache if it
// has reached maxCacheSize. Caller must hold tokenCacheMu write lock.
func (rm *RBACManager) evictTokenCacheIfFull() {
	if len(rm.tokenCache) >= rm.maxCacheSize {
		for k := range rm.tokenCache {
			delete(rm.tokenCache, k)
			break
		}
	}
}

// cleanupExpiredCache removes expired entries from both caches
func (rm *RBACManager) cleanupExpiredCache() {
	now := time.Now()

	// Clean token cache
	rm.tokenCacheMu.Lock()
	for tokenID, data := range rm.tokenCache {
		if now.Sub(data.loadedAt) > rm.tokenCacheTTL {
			delete(rm.tokenCache, tokenID)
		}
	}
	rm.tokenCacheMu.Unlock()

	// Clean permission cache
	rm.permCacheMu.Lock()
	for key, entry := range rm.permCache {
		if now.After(entry.expiresAt) {
			delete(rm.permCache, key)
		}
	}
	rm.permCacheMu.Unlock()
}

// InvalidateTokenCache clears cached RBAC data for a specific token
func (rm *RBACManager) InvalidateTokenCache(tokenID int64) {
	rm.tokenCacheMu.Lock()
	delete(rm.tokenCache, tokenID)
	rm.tokenCacheMu.Unlock()

	// Also clear permission cache entries for this token
	rm.permCacheMu.Lock()
	for key := range rm.permCache {
		if key.tokenID == tokenID {
			delete(rm.permCache, key)
		}
	}
	rm.permCacheMu.Unlock()
}

// InvalidateAllCache clears all RBAC caches (call after role/permission changes)
func (rm *RBACManager) InvalidateAllCache() {
	rm.tokenCacheMu.Lock()
	rm.tokenCache = make(map[int64]*tokenRBACData)
	rm.tokenCacheMu.Unlock()

	rm.permCacheMu.Lock()
	rm.permCache = make(map[permissionCacheKey]*permissionCacheEntry)
	rm.permCacheMu.Unlock()
}

// GetCacheStats returns cache hit/miss statistics
func (rm *RBACManager) GetCacheStats() map[string]int64 {
	return map[string]int64{
		"hits":   rm.cacheHits.Load(),
		"misses": rm.cacheMisses.Load(),
	}
}

// IsRBACEnabled returns true if RBAC feature is available
func (rm *RBACManager) IsRBACEnabled() bool {
	if rm.licenseClient == nil {
		return false
	}
	lic := rm.licenseClient.GetLicense()
	if lic == nil || !lic.IsValid() {
		return false
	}
	return lic.HasFeature(license.FeatureRBAC)
}

// =============================================================================
// Organizations CRUD
// =============================================================================

// CreateOrganization creates a new organization. In cluster mode the
// write is proposed via Raft and materialised on every node via
// ApplyCreateOrganization; in OSS / standalone mode it writes directly
// to local SQLite (unchanged from pre-Phase-A.1 behaviour).
func (rm *RBACManager) CreateOrganization(ctx context.Context, req *CreateOrganizationRequest) (*Organization, error) {
	if req.Name == "" {
		return nil, errors.New("organization name is required")
	}
	if err := validateName(req.Name); err != nil {
		return nil, fmt.Errorf("invalid organization name: %w", err)
	}

	if rm.getProposer() != nil {
		now := time.Now()
		payload := createOrganizationPayloadWire{
			Organization: clusterOrganizationEntryWire{
				Name:              req.Name,
				Description:       req.Description,
				CreatedAtUnixNano: now.UnixNano(),
				UpdatedAtUnixNano: now.UnixNano(),
				Enabled:           true,
			},
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandCreateOrganization, payload); err != nil {
			// Translate FSM-side rejection of duplicate-name into the same
			// error string the OSS path returns, so handlers can keep their
			// existing UX.
			if strings.Contains(err.Error(), "already exists") {
				return nil, fmt.Errorf("organization with name '%s' already exists", req.Name)
			}
			return nil, fmt.Errorf("failed to create organization: %w", err)
		}
		// On the leader, the apply callback has already fired by the time
		// Propose returned — read-back is immediate. On a follower, the
		// entry replicates asynchronously via the runFSM goroutine and
		// may not have hit local SQLite yet, so we retry briefly on
		// sql.ErrNoRows. Gemini PR #458 round 2.
		rm.logger.Info().Str("name", req.Name).Msg("Created organization via Raft")
		var org Organization
		if err := readBackAfterPropose(ctx, func() error {
			return rm.db.QueryRowContext(ctx, `
				SELECT id, name, description, created_at, updated_at, enabled
				FROM rbac_organizations WHERE name = ?
			`, req.Name).Scan(&org.ID, &org.Name, &org.Description, &org.CreatedAt, &org.UpdatedAt, &org.Enabled)
		}); err != nil {
			return nil, fmt.Errorf("failed to read back organization after Raft apply: %w", err)
		}
		return &org, nil
	}

	// OSS path: direct SQLite. ExecContext so cancellation is honoured
	// even on the standalone deployment. Gemini PR #458 round 3.
	// .UTC() for consistency with the cluster path (which goes through
	// nextProposerTimestamp → UTC) and the rest of the auth surface.
	// Issue #460.
	now := time.Now().UTC()
	result, err := rm.db.ExecContext(ctx, `
		INSERT INTO rbac_organizations (name, description, created_at, updated_at, enabled)
		VALUES (?, ?, ?, ?, 1)
	`, req.Name, req.Description, now, now)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, fmt.Errorf("organization with name '%s' already exists", req.Name)
		}
		return nil, fmt.Errorf("failed to create organization: %w", err)
	}

	id, _ := result.LastInsertId()
	rm.logger.Info().Int64("id", id).Str("name", req.Name).Msg("Created organization")

	return &Organization{
		ID:          id,
		Name:        req.Name,
		Description: req.Description,
		CreatedAt:   now,
		UpdatedAt:   now,
		Enabled:     true,
	}, nil
}

// GetOrganization retrieves an organization by ID
func (rm *RBACManager) GetOrganization(id int64) (*Organization, error) {
	var org Organization
	err := rm.db.QueryRow(`
		SELECT id, name, description, created_at, updated_at, enabled
		FROM rbac_organizations WHERE id = ?
	`, id).Scan(&org.ID, &org.Name, &org.Description, &org.CreatedAt, &org.UpdatedAt, &org.Enabled)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get organization: %w", err)
	}
	return &org, nil
}

// ListOrganizations returns all organizations
func (rm *RBACManager) ListOrganizations() ([]Organization, error) {
	rows, err := rm.db.Query(`
		SELECT id, name, description, created_at, updated_at, enabled
		FROM rbac_organizations ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to list organizations: %w", err)
	}
	defer rows.Close()

	var orgs []Organization
	for rows.Next() {
		var org Organization
		if err := rows.Scan(&org.ID, &org.Name, &org.Description, &org.CreatedAt, &org.UpdatedAt, &org.Enabled); err != nil {
			return nil, fmt.Errorf("failed to scan organization: %w", err)
		}
		orgs = append(orgs, org)
	}
	return orgs, nil
}

// UpdateOrganization updates an organization. Same dual-path shape as
// CreateOrganization.
func (rm *RBACManager) UpdateOrganization(ctx context.Context, id int64, req *UpdateOrganizationRequest) error {
	if rm.getProposer() != nil {
		payload := updateOrganizationPayloadWire{
			ID:                id,
			UpdatedAtUnixNano: time.Now().UnixNano(),
		}
		if req.Name != nil {
			payload.Name = *req.Name
			payload.ChangedFields = append(payload.ChangedFields, "name")
		}
		if req.Description != nil {
			payload.Description = *req.Description
			payload.ChangedFields = append(payload.ChangedFields, "description")
		}
		if req.Enabled != nil {
			payload.Enabled = *req.Enabled
			payload.ChangedFields = append(payload.ChangedFields, "enabled")
		}
		if len(payload.ChangedFields) == 0 {
			return nil
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandUpdateOrganization, payload); err != nil {
			if strings.Contains(err.Error(), "not found") {
				return errors.New("organization not found")
			}
			if strings.Contains(err.Error(), "already exists") {
				return errors.New("organization with that name already exists")
			}
			return fmt.Errorf("failed to update organization: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Updated organization via Raft")
		return nil
	}

	// OSS path: direct SQLite. ExecContext honours ctx cancellation.
	// Gemini PR #458 round 3.
	var updates []string
	var args []interface{}

	if req.Name != nil {
		updates = append(updates, "name = ?")
		args = append(args, *req.Name)
	}
	if req.Description != nil {
		updates = append(updates, "description = ?")
		args = append(args, *req.Description)
	}
	if req.Enabled != nil {
		updates = append(updates, "enabled = ?")
		if *req.Enabled {
			args = append(args, 1)
		} else {
			args = append(args, 0)
		}
	}

	if len(updates) == 0 {
		return nil
	}

	updates = append(updates, "updated_at = ?")
	args = append(args, time.Now().UTC()) // OSS path; UTC per issue #460.
	args = append(args, id)

	query := fmt.Sprintf("UPDATE rbac_organizations SET %s WHERE id = ?", strings.Join(updates, ", "))
	result, err := rm.db.ExecContext(ctx, query, args...)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return errors.New("organization with that name already exists")
		}
		return fmt.Errorf("failed to update organization: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("organization not found")
	}

	rm.logger.Info().Int64("id", id).Msg("Updated organization")
	return nil
}

// DeleteOrganization deletes an organization (cascades to teams, roles, etc.).
// Cluster path: cascade-in-apply across all 4 descendant levels under one
// Raft log entry; SQLite ON DELETE CASCADE handles the local mirror. OSS
// path: unchanged.
func (rm *RBACManager) DeleteOrganization(ctx context.Context, id int64) error {
	if rm.getProposer() != nil {
		// Local existence pre-check so cluster-mode Delete returns the
		// same "not found" error as the OSS path (avoids the OSS-vs-
		// cluster behavioural divergence where the FSM's
		// applyDeleteOrganization treats not-found as an idempotent
		// no-op and the proposer returns nil → API 200 OK for a
		// missing org). On a follower with a stale local cache the
		// pre-check might 404 for an org just-created elsewhere not
		// yet replicated to us — that race window is sub-50ms and
		// returning 404 honestly conveys "doesn't exist to me yet".
		// Gemini PR #458 round 8 G27.
		org, err := rm.GetOrganization(id)
		if err != nil {
			return fmt.Errorf("failed to look up organization: %w", err)
		}
		if org == nil {
			return errors.New("organization not found")
		}
		// Phase A.2 Item 2: cascade-on-delete soft cap. Count the
		// descendants in local SQLite; if the sum exceeds the cap,
		// refuse with a clear operator error instead of letting the
		// FSM apply block past the Raft heartbeat margin. cap == 0
		// disables the check.
		if rm.maxCascadeDescendants > 0 {
			n, countErr := rm.countOrgCascadeDescendants(ctx, id)
			if countErr != nil {
				return fmt.Errorf("failed to count organization descendants: %w", countErr)
			}
			if n > rm.maxCascadeDescendants {
				metrics.Get().IncClusterRBACCascadeRejected()
				rm.logger.Warn().
					Int64("organization_id", id).
					Int("descendants", n).
					Int("max_cascade_descendants", rm.maxCascadeDescendants).
					Msg("DeleteOrganization refused: cascade exceeds configured limit")
				return fmt.Errorf("%w: %d descendants under organization %d (max %d); delete child entities (teams, roles, measurement_permissions, token_memberships) first",
					ErrCascadeCapExceeded, n, id, rm.maxCascadeDescendants)
			}
		}
		payload := deleteOrganizationPayloadWire{ID: id}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandDeleteOrganization, payload); err != nil {
			return fmt.Errorf("failed to delete organization: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Deleted organization via Raft")
		return nil
	}

	// OSS path. Gemini PR #458 round 3: ExecContext honours ctx.
	result, err := rm.db.ExecContext(ctx, "DELETE FROM rbac_organizations WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete organization: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("organization not found")
	}

	rm.logger.Info().Int64("id", id).Msg("Deleted organization")
	return nil
}

// =============================================================================
// Teams CRUD
// =============================================================================

// CreateTeam creates a new team in an organization. Same dual-path shape
// as CreateOrganization.
func (rm *RBACManager) CreateTeam(ctx context.Context, orgID int64, req *CreateTeamRequest) (*Team, error) {
	if req.Name == "" {
		return nil, errors.New("team name is required")
	}
	if err := validateName(req.Name); err != nil {
		return nil, fmt.Errorf("invalid team name: %w", err)
	}

	if rm.getProposer() != nil {
		now := time.Now()
		payload := createTeamPayloadWire{
			Team: clusterTeamEntryWire{
				OrganizationID:    orgID,
				Name:              req.Name,
				Description:       req.Description,
				CreatedAtUnixNano: now.UnixNano(),
				UpdatedAtUnixNano: now.UnixNano(),
				Enabled:           true,
			},
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandCreateTeam, payload); err != nil {
			if strings.Contains(err.Error(), "organization") && strings.Contains(err.Error(), "not found") {
				return nil, errors.New("organization not found")
			}
			if strings.Contains(err.Error(), "already exists") {
				return nil, fmt.Errorf("team with name '%s' already exists in this organization", req.Name)
			}
			return nil, fmt.Errorf("failed to create team: %w", err)
		}
		rm.logger.Info().Int64("org_id", orgID).Str("name", req.Name).Msg("Created team via Raft")
		var team Team
		if err := readBackAfterPropose(ctx, func() error {
			return rm.db.QueryRowContext(ctx, `
				SELECT id, organization_id, name, description, created_at, updated_at, enabled
				FROM rbac_teams WHERE organization_id = ? AND name = ?
			`, orgID, req.Name).Scan(&team.ID, &team.OrganizationID, &team.Name, &team.Description, &team.CreatedAt, &team.UpdatedAt, &team.Enabled)
		}); err != nil {
			return nil, fmt.Errorf("failed to read back team after Raft apply: %w", err)
		}
		return &team, nil
	}

	// OSS path. GetOrganization is a Get helper that does not currently
	// thread ctx; the INSERT uses ExecContext so cancellation is at
	// least honoured at the mutation site. Gemini PR #458 round 3.
	// Verify organization exists
	org, err := rm.GetOrganization(orgID)
	if err != nil {
		return nil, err
	}
	if org == nil {
		return nil, errors.New("organization not found")
	}

	// OSS path; UTC per issue #460 (matches cluster path's
	// nextProposerTimestamp → UTC convention).
	now := time.Now().UTC()
	result, err := rm.db.ExecContext(ctx, `
		INSERT INTO rbac_teams (organization_id, name, description, created_at, updated_at, enabled)
		VALUES (?, ?, ?, ?, ?, 1)
	`, orgID, req.Name, req.Description, now, now)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, fmt.Errorf("team with name '%s' already exists in this organization", req.Name)
		}
		return nil, fmt.Errorf("failed to create team: %w", err)
	}

	id, _ := result.LastInsertId()
	rm.logger.Info().Int64("id", id).Int64("org_id", orgID).Str("name", req.Name).Msg("Created team")

	return &Team{
		ID:             id,
		OrganizationID: orgID,
		Name:           req.Name,
		Description:    req.Description,
		CreatedAt:      now,
		UpdatedAt:      now,
		Enabled:        true,
	}, nil
}

// GetTeam retrieves a team by ID
func (rm *RBACManager) GetTeam(id int64) (*Team, error) {
	var team Team
	err := rm.db.QueryRow(`
		SELECT id, organization_id, name, description, created_at, updated_at, enabled
		FROM rbac_teams WHERE id = ?
	`, id).Scan(&team.ID, &team.OrganizationID, &team.Name, &team.Description, &team.CreatedAt, &team.UpdatedAt, &team.Enabled)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get team: %w", err)
	}
	return &team, nil
}

// ListTeamsByOrganization returns all teams in an organization
func (rm *RBACManager) ListTeamsByOrganization(orgID int64) ([]Team, error) {
	rows, err := rm.db.Query(`
		SELECT id, organization_id, name, description, created_at, updated_at, enabled
		FROM rbac_teams WHERE organization_id = ? ORDER BY name
	`, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to list teams: %w", err)
	}
	defer rows.Close()

	var teams []Team
	for rows.Next() {
		var team Team
		if err := rows.Scan(&team.ID, &team.OrganizationID, &team.Name, &team.Description, &team.CreatedAt, &team.UpdatedAt, &team.Enabled); err != nil {
			return nil, fmt.Errorf("failed to scan team: %w", err)
		}
		teams = append(teams, team)
	}
	return teams, nil
}

// UpdateTeam updates a team.
func (rm *RBACManager) UpdateTeam(ctx context.Context, id int64, req *UpdateTeamRequest) error {
	if rm.getProposer() != nil {
		payload := updateTeamPayloadWire{
			ID:                id,
			UpdatedAtUnixNano: time.Now().UnixNano(),
		}
		if req.Name != nil {
			payload.Name = *req.Name
			payload.ChangedFields = append(payload.ChangedFields, "name")
		}
		if req.Description != nil {
			payload.Description = *req.Description
			payload.ChangedFields = append(payload.ChangedFields, "description")
		}
		if req.Enabled != nil {
			payload.Enabled = *req.Enabled
			payload.ChangedFields = append(payload.ChangedFields, "enabled")
		}
		if len(payload.ChangedFields) == 0 {
			return nil
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandUpdateTeam, payload); err != nil {
			if strings.Contains(err.Error(), "team") && strings.Contains(err.Error(), "not found") {
				return errors.New("team not found")
			}
			if strings.Contains(err.Error(), "already exists") {
				return errors.New("team with that name already exists in this organization")
			}
			return fmt.Errorf("failed to update team: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Updated team via Raft")
		return nil
	}

	// OSS path. Gemini PR #458 round 3.
	var updates []string
	var args []interface{}

	if req.Name != nil {
		updates = append(updates, "name = ?")
		args = append(args, *req.Name)
	}
	if req.Description != nil {
		updates = append(updates, "description = ?")
		args = append(args, *req.Description)
	}
	if req.Enabled != nil {
		updates = append(updates, "enabled = ?")
		if *req.Enabled {
			args = append(args, 1)
		} else {
			args = append(args, 0)
		}
	}

	if len(updates) == 0 {
		return nil
	}

	updates = append(updates, "updated_at = ?")
	args = append(args, time.Now().UTC()) // OSS path; UTC per issue #460.
	args = append(args, id)

	query := fmt.Sprintf("UPDATE rbac_teams SET %s WHERE id = ?", strings.Join(updates, ", "))
	result, err := rm.db.ExecContext(ctx, query, args...)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return errors.New("team with that name already exists in this organization")
		}
		return fmt.Errorf("failed to update team: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("team not found")
	}

	rm.logger.Info().Int64("id", id).Msg("Updated team")

	// Invalidate all caches - team changes affect all tokens in this team
	rm.InvalidateAllCache()

	return nil
}

// DeleteTeam deletes a team (cascades to roles and memberships).
func (rm *RBACManager) DeleteTeam(ctx context.Context, id int64) error {
	if rm.getProposer() != nil {
		// Pre-check for OSS-vs-cluster parity. See DeleteOrganization
		// for rationale. Gemini PR #458 round 8 G27.
		team, err := rm.GetTeam(id)
		if err != nil {
			return fmt.Errorf("failed to look up team: %w", err)
		}
		if team == nil {
			return errors.New("team not found")
		}
		// Phase A.2 Item 2: cascade-on-delete soft cap. Same shape as
		// DeleteOrganization. Team cascade is 3-level (roles +
		// measurement_permissions + memberships).
		if rm.maxCascadeDescendants > 0 {
			n, countErr := rm.countTeamCascadeDescendants(ctx, id)
			if countErr != nil {
				return fmt.Errorf("failed to count team descendants: %w", countErr)
			}
			if n > rm.maxCascadeDescendants {
				metrics.Get().IncClusterRBACCascadeRejected()
				rm.logger.Warn().
					Int64("team_id", id).
					Int("descendants", n).
					Int("max_cascade_descendants", rm.maxCascadeDescendants).
					Msg("DeleteTeam refused: cascade exceeds configured limit")
				return fmt.Errorf("%w: %d descendants under team %d (max %d); delete child entities (roles, measurement_permissions, token_memberships) first",
					ErrCascadeCapExceeded, n, id, rm.maxCascadeDescendants)
			}
		}
		payload := deleteTeamPayloadWire{ID: id}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandDeleteTeam, payload); err != nil {
			return fmt.Errorf("failed to delete team: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Deleted team via Raft")
		return nil
	}

	// OSS path. Gemini PR #458 round 3.
	result, err := rm.db.ExecContext(ctx, "DELETE FROM rbac_teams WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete team: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("team not found")
	}

	rm.logger.Info().Int64("id", id).Msg("Deleted team")

	// Invalidate all caches - team deletion affects all tokens
	rm.InvalidateAllCache()

	return nil
}

// =============================================================================
// Roles CRUD
// =============================================================================

// CreateRole creates a new role for a team.
func (rm *RBACManager) CreateRole(ctx context.Context, teamID int64, req *CreateRoleRequest) (*Role, error) {
	if req.DatabasePattern == "" {
		return nil, errors.New("database pattern is required")
	}
	if len(req.Permissions) == 0 {
		return nil, errors.New("at least one permission is required")
	}
	if err := validatePattern(req.DatabasePattern); err != nil {
		return nil, fmt.Errorf("invalid database pattern: %w", err)
	}
	for _, p := range req.Permissions {
		if !IsValidPermission(p) {
			return nil, fmt.Errorf("invalid permission: %s", p)
		}
	}

	if rm.getProposer() != nil {
		// nextProposerTimestamp instead of time.Now(): the round-9
		// G28 read-back disambiguator matches on `created_at = now`,
		// and two concurrent callers can collide at the same
		// nanosecond on fast machines (race-detector-confirmed under
		// 8-goroutine load). nextProposerTimestamp adds a monotonic
		// counter offset so even simultaneous calls get distinct
		// timestamps. Phase A.2 follow-up.
		now := rm.nextProposerTimestamp()
		perms := strings.Join(req.Permissions, ",")
		payload := createRolePayloadWire{
			Role: clusterRoleEntryWire{
				TeamID:            teamID,
				DatabasePattern:   req.DatabasePattern,
				Permissions:       perms,
				CreatedAtUnixNano: now.UnixNano(),
			},
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandCreateRole, payload); err != nil {
			if strings.Contains(err.Error(), "team") && strings.Contains(err.Error(), "not found") {
				return nil, errors.New("team not found")
			}
			return nil, fmt.Errorf("failed to create role: %w", err)
		}
		rm.logger.Info().
			Int64("team_id", teamID).
			Str("database_pattern", req.DatabasePattern).
			Strs("permissions", req.Permissions).
			Msg("Created role via Raft")
		// Read back. Roles aren't UNIQUE on (team_id, database_pattern,
		// permissions) — two concurrent identical Create calls would
		// produce two rows with the same business key. Match on
		// created_at = this proposer's now (nanosecond Go monotonic
		// clock + the proposer holding the value locally before
		// propose makes the timestamp unique per call across the whole
		// process). This unambiguously identifies THIS caller's row
		// even when an identical row was created concurrently. The FSM
		// stamps the proposer-supplied CreatedAtUnixNano verbatim into
		// SQLite. Gemini PR #458 round 9 G28.
		var role Role
		var dbPerms string
		if err := readBackAfterPropose(ctx, func() error {
			return rm.db.QueryRowContext(ctx, `
				SELECT id, team_id, database_pattern, permissions, created_at
				FROM rbac_roles
				WHERE team_id = ? AND database_pattern = ? AND permissions = ? AND created_at = ?
			`, teamID, req.DatabasePattern, perms, now).Scan(&role.ID, &role.TeamID, &role.DatabasePattern, &dbPerms, &role.CreatedAt)
		}); err != nil {
			return nil, fmt.Errorf("failed to read back role after Raft apply: %w", err)
		}
		role.Permissions = strings.Split(dbPerms, ",")
		return &role, nil
	}

	// OSS path. Gemini PR #458 round 3.
	team, err := rm.GetTeam(teamID)
	if err != nil {
		return nil, err
	}
	if team == nil {
		return nil, errors.New("team not found")
	}

	// OSS path; UTC per issue #460 (the cluster path uses
	// nextProposerTimestamp → UTC and has a created_at = ? readback
	// downstream, so OSS matches the convention for symmetry).
	now := time.Now().UTC()
	perms := strings.Join(req.Permissions, ",")
	result, err := rm.db.ExecContext(ctx, `
		INSERT INTO rbac_roles (team_id, database_pattern, permissions, created_at)
		VALUES (?, ?, ?, ?)
	`, teamID, req.DatabasePattern, perms, now)
	if err != nil {
		return nil, fmt.Errorf("failed to create role: %w", err)
	}

	id, _ := result.LastInsertId()
	rm.logger.Info().
		Int64("id", id).
		Int64("team_id", teamID).
		Str("database_pattern", req.DatabasePattern).
		Strs("permissions", req.Permissions).
		Msg("Created role")

	rm.InvalidateAllCache()

	return &Role{
		ID:              id,
		TeamID:          teamID,
		DatabasePattern: req.DatabasePattern,
		Permissions:     req.Permissions,
		CreatedAt:       now,
	}, nil
}

// GetRole retrieves a role by ID
func (rm *RBACManager) GetRole(id int64) (*Role, error) {
	var role Role
	var perms string
	err := rm.db.QueryRow(`
		SELECT id, team_id, database_pattern, permissions, created_at
		FROM rbac_roles WHERE id = ?
	`, id).Scan(&role.ID, &role.TeamID, &role.DatabasePattern, &perms, &role.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get role: %w", err)
	}
	role.Permissions = strings.Split(perms, ",")
	return &role, nil
}

// ListRolesByTeam returns all roles for a team
func (rm *RBACManager) ListRolesByTeam(teamID int64) ([]Role, error) {
	rows, err := rm.db.Query(`
		SELECT id, team_id, database_pattern, permissions, created_at
		FROM rbac_roles WHERE team_id = ? ORDER BY database_pattern
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("failed to list roles: %w", err)
	}
	defer rows.Close()

	var roles []Role
	for rows.Next() {
		var role Role
		var perms string
		if err := rows.Scan(&role.ID, &role.TeamID, &role.DatabasePattern, &perms, &role.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan role: %w", err)
		}
		role.Permissions = strings.Split(perms, ",")
		roles = append(roles, role)
	}
	return roles, nil
}

// UpdateRole updates a role.
func (rm *RBACManager) UpdateRole(ctx context.Context, id int64, req *UpdateRoleRequest) error {
	if len(req.Permissions) > 0 {
		for _, p := range req.Permissions {
			if !IsValidPermission(p) {
				return fmt.Errorf("invalid permission: %s", p)
			}
		}
	}
	if req.DatabasePattern != nil {
		if err := validatePattern(*req.DatabasePattern); err != nil {
			return fmt.Errorf("invalid database pattern: %w", err)
		}
	}

	if rm.getProposer() != nil {
		payload := updateRolePayloadWire{ID: id}
		if req.DatabasePattern != nil {
			payload.DatabasePattern = *req.DatabasePattern
			payload.ChangedFields = append(payload.ChangedFields, "database_pattern")
		}
		if len(req.Permissions) > 0 {
			payload.Permissions = strings.Join(req.Permissions, ",")
			payload.ChangedFields = append(payload.ChangedFields, "permissions")
		}
		if len(payload.ChangedFields) == 0 {
			return nil
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandUpdateRole, payload); err != nil {
			if strings.Contains(err.Error(), "role") && strings.Contains(err.Error(), "not found") {
				return errors.New("role not found")
			}
			return fmt.Errorf("failed to update role: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Updated role via Raft")
		return nil
	}

	// OSS path. Gemini PR #458 round 3.
	var updates []string
	var args []interface{}

	if req.DatabasePattern != nil {
		updates = append(updates, "database_pattern = ?")
		args = append(args, *req.DatabasePattern)
	}
	if len(req.Permissions) > 0 {
		updates = append(updates, "permissions = ?")
		args = append(args, strings.Join(req.Permissions, ","))
	}

	if len(updates) == 0 {
		return nil
	}

	args = append(args, id)
	query := fmt.Sprintf("UPDATE rbac_roles SET %s WHERE id = ?", strings.Join(updates, ", "))
	result, err := rm.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to update role: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("role not found")
	}

	rm.logger.Info().Int64("id", id).Msg("Updated role")

	rm.InvalidateAllCache()

	return nil
}

// DeleteRole deletes a role (cascades to measurement permissions).
func (rm *RBACManager) DeleteRole(ctx context.Context, id int64) error {
	if rm.getProposer() != nil {
		// Pre-check for OSS-vs-cluster parity. See DeleteOrganization
		// for rationale. Gemini PR #458 round 8 G27.
		role, err := rm.GetRole(id)
		if err != nil {
			return fmt.Errorf("failed to look up role: %w", err)
		}
		if role == nil {
			return errors.New("role not found")
		}
		payload := deleteRolePayloadWire{ID: id}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandDeleteRole, payload); err != nil {
			return fmt.Errorf("failed to delete role: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Deleted role via Raft")
		return nil
	}

	// OSS path. Gemini PR #458 round 3.
	result, err := rm.db.ExecContext(ctx, "DELETE FROM rbac_roles WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete role: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("role not found")
	}

	rm.logger.Info().Int64("id", id).Msg("Deleted role")

	rm.InvalidateAllCache()

	return nil
}

// =============================================================================
// Measurement Permissions CRUD
// =============================================================================

// CreateMeasurementPermission creates measurement-level permissions for a role.
func (rm *RBACManager) CreateMeasurementPermission(ctx context.Context, roleID int64, req *CreateMeasurementPermissionRequest) (*MeasurementPermission, error) {
	if req.MeasurementPattern == "" {
		return nil, errors.New("measurement pattern is required")
	}
	if len(req.Permissions) == 0 {
		return nil, errors.New("at least one permission is required")
	}
	if err := validatePattern(req.MeasurementPattern); err != nil {
		return nil, fmt.Errorf("invalid measurement pattern: %w", err)
	}
	for _, p := range req.Permissions {
		if !IsValidPermission(p) {
			return nil, fmt.Errorf("invalid permission: %s", p)
		}
	}

	if rm.getProposer() != nil {
		// nextProposerTimestamp instead of time.Now() — same race fix
		// as CreateRole; see comment there. Phase A.2 follow-up to PR
		// #458 round 9 G29.
		now := rm.nextProposerTimestamp()
		perms := strings.Join(req.Permissions, ",")
		payload := createMeasurementPermissionPayloadWire{
			MeasurementPermission: clusterMeasurementPermissionEntryWire{
				RoleID:             roleID,
				MeasurementPattern: req.MeasurementPattern,
				Permissions:        perms,
				CreatedAtUnixNano:  now.UnixNano(),
			},
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandCreateMeasurementPermission, payload); err != nil {
			if strings.Contains(err.Error(), "role") && strings.Contains(err.Error(), "not found") {
				return nil, errors.New("role not found")
			}
			return nil, fmt.Errorf("failed to create measurement permission: %w", err)
		}
		rm.logger.Info().
			Int64("role_id", roleID).
			Str("measurement_pattern", req.MeasurementPattern).
			Msg("Created measurement permission via Raft")
		// Same race fix as CreateRole — match on created_at = this
		// proposer's now to disambiguate concurrent identical creates
		// (no UNIQUE(role_id, measurement_pattern, permissions) in the
		// schema). Gemini PR #458 round 9 G29.
		var mp MeasurementPermission
		var dbPerms string
		if err := readBackAfterPropose(ctx, func() error {
			return rm.db.QueryRowContext(ctx, `
				SELECT id, role_id, measurement_pattern, permissions, created_at
				FROM rbac_measurement_permissions
				WHERE role_id = ? AND measurement_pattern = ? AND permissions = ? AND created_at = ?
			`, roleID, req.MeasurementPattern, perms, now).Scan(&mp.ID, &mp.RoleID, &mp.MeasurementPattern, &dbPerms, &mp.CreatedAt)
		}); err != nil {
			return nil, fmt.Errorf("failed to read back measurement permission after Raft apply: %w", err)
		}
		mp.Permissions = strings.Split(dbPerms, ",")
		return &mp, nil
	}

	// OSS path. Gemini PR #458 round 3.
	role, err := rm.GetRole(roleID)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return nil, errors.New("role not found")
	}

	// OSS path; UTC per issue #460 (same reasoning as CreateRole above).
	now := time.Now().UTC()
	perms := strings.Join(req.Permissions, ",")
	result, err := rm.db.ExecContext(ctx, `
		INSERT INTO rbac_measurement_permissions (role_id, measurement_pattern, permissions, created_at)
		VALUES (?, ?, ?, ?)
	`, roleID, req.MeasurementPattern, perms, now)
	if err != nil {
		return nil, fmt.Errorf("failed to create measurement permission: %w", err)
	}

	id, _ := result.LastInsertId()
	rm.logger.Info().
		Int64("id", id).
		Int64("role_id", roleID).
		Str("measurement_pattern", req.MeasurementPattern).
		Msg("Created measurement permission")

	rm.InvalidateAllCache()

	return &MeasurementPermission{
		ID:                 id,
		RoleID:             roleID,
		MeasurementPattern: req.MeasurementPattern,
		Permissions:        req.Permissions,
		CreatedAt:          now,
	}, nil
}

// ListMeasurementPermissionsByRole returns measurement permissions for a role
func (rm *RBACManager) ListMeasurementPermissionsByRole(roleID int64) ([]MeasurementPermission, error) {
	rows, err := rm.db.Query(`
		SELECT id, role_id, measurement_pattern, permissions, created_at
		FROM rbac_measurement_permissions WHERE role_id = ? ORDER BY measurement_pattern
	`, roleID)
	if err != nil {
		return nil, fmt.Errorf("failed to list measurement permissions: %w", err)
	}
	defer rows.Close()

	var perms []MeasurementPermission
	for rows.Next() {
		var mp MeasurementPermission
		var permStr string
		if err := rows.Scan(&mp.ID, &mp.RoleID, &mp.MeasurementPattern, &permStr, &mp.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to scan measurement permission: %w", err)
		}
		mp.Permissions = strings.Split(permStr, ",")
		perms = append(perms, mp)
	}
	return perms, nil
}

// DeleteMeasurementPermission deletes a measurement permission.
func (rm *RBACManager) DeleteMeasurementPermission(ctx context.Context, id int64) error {
	if rm.getProposer() != nil {
		// Pre-check for OSS-vs-cluster parity. No GetMeasurementPermission
		// helper today, so inline the existence query. See
		// DeleteOrganization for rationale. Gemini PR #458 round 8 G27.
		var exists int
		err := rm.db.QueryRowContext(ctx,
			`SELECT 1 FROM rbac_measurement_permissions WHERE id = ?`, id,
		).Scan(&exists)
		if err == sql.ErrNoRows {
			return errors.New("measurement permission not found")
		}
		if err != nil {
			return fmt.Errorf("failed to look up measurement permission: %w", err)
		}
		payload := deleteMeasurementPermissionPayloadWire{ID: id}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandDeleteMeasurementPermission, payload); err != nil {
			return fmt.Errorf("failed to delete measurement permission: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Deleted measurement permission via Raft")
		return nil
	}

	// OSS path. Gemini PR #458 round 3.
	result, err := rm.db.ExecContext(ctx, "DELETE FROM rbac_measurement_permissions WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("failed to delete measurement permission: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("measurement permission not found")
	}

	rm.logger.Info().Int64("id", id).Msg("Deleted measurement permission")

	rm.InvalidateAllCache()

	return nil
}

// =============================================================================
// Token Memberships
// =============================================================================

// AddTokenToTeam adds a token to a team.
func (rm *RBACManager) AddTokenToTeam(ctx context.Context, tokenID, teamID int64) (*TokenMembership, error) {
	if rm.getProposer() != nil {
		now := time.Now()
		payload := addTokenToTeamPayloadWire{
			Membership: clusterTokenMembershipEntryWire{
				TokenID:           tokenID,
				TeamID:            teamID,
				CreatedAtUnixNano: now.UnixNano(),
			},
		}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandAddTokenToTeam, payload); err != nil {
			if strings.Contains(err.Error(), "team") && strings.Contains(err.Error(), "not found") {
				return nil, errors.New("team not found")
			}
			if strings.Contains(err.Error(), "token") && strings.Contains(err.Error(), "not found") {
				return nil, errors.New("token not found")
			}
			if strings.Contains(err.Error(), "already") {
				return nil, errors.New("token is already a member of this team")
			}
			return nil, fmt.Errorf("failed to add token to team: %w", err)
		}
		rm.logger.Info().Int64("token_id", tokenID).Int64("team_id", teamID).Msg("Added token to team via Raft")
		var mem TokenMembership
		if err := readBackAfterPropose(ctx, func() error {
			return rm.db.QueryRowContext(ctx, `
				SELECT id, token_id, team_id, created_at
				FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?
			`, tokenID, teamID).Scan(&mem.ID, &mem.TokenID, &mem.TeamID, &mem.CreatedAt)
		}); err != nil {
			return nil, fmt.Errorf("failed to read back token membership after Raft apply: %w", err)
		}
		return &mem, nil
	}

	// OSS path. Gemini PR #458 round 3.
	team, err := rm.GetTeam(teamID)
	if err != nil {
		return nil, err
	}
	if team == nil {
		return nil, errors.New("team not found")
	}

	// OSS path; UTC per issue #460.
	now := time.Now().UTC()
	result, err := rm.db.ExecContext(ctx, `
		INSERT INTO rbac_token_memberships (token_id, team_id, created_at)
		VALUES (?, ?, ?)
	`, tokenID, teamID, now)
	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return nil, errors.New("token is already a member of this team")
		}
		return nil, fmt.Errorf("failed to add token to team: %w", err)
	}

	id, _ := result.LastInsertId()
	rm.logger.Info().Int64("token_id", tokenID).Int64("team_id", teamID).Msg("Added token to team")

	rm.InvalidateTokenCache(tokenID)

	return &TokenMembership{
		ID:        id,
		TokenID:   tokenID,
		TeamID:    teamID,
		CreatedAt: now,
	}, nil
}

// RemoveTokenFromTeam removes a token from a team.
func (rm *RBACManager) RemoveTokenFromTeam(ctx context.Context, tokenID, teamID int64) error {
	if rm.getProposer() != nil {
		// Pre-check for OSS-vs-cluster parity. See DeleteOrganization
		// for rationale. Gemini PR #458 round 8 G27.
		var exists int
		err := rm.db.QueryRowContext(ctx,
			`SELECT 1 FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?`,
			tokenID, teamID,
		).Scan(&exists)
		if err == sql.ErrNoRows {
			return errors.New("token membership not found")
		}
		if err != nil {
			return fmt.Errorf("failed to look up token membership: %w", err)
		}
		payload := removeTokenFromTeamPayloadWire{TokenID: tokenID, TeamID: teamID}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandRemoveTokenFromTeam, payload); err != nil {
			return fmt.Errorf("failed to remove token from team: %w", err)
		}
		rm.logger.Info().Int64("token_id", tokenID).Int64("team_id", teamID).Msg("Removed token from team via Raft")
		return nil
	}

	// OSS path. Gemini PR #458 round 3.
	result, err := rm.db.ExecContext(ctx, `
		DELETE FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?
	`, tokenID, teamID)
	if err != nil {
		return fmt.Errorf("failed to remove token from team: %w", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("token membership not found")
	}

	rm.logger.Info().Int64("token_id", tokenID).Int64("team_id", teamID).Msg("Removed token from team")

	rm.InvalidateTokenCache(tokenID)

	return nil
}

// GetTokenTeams returns all teams a token belongs to
func (rm *RBACManager) GetTokenTeams(tokenID int64) ([]Team, error) {
	rows, err := rm.db.Query(`
		SELECT t.id, t.organization_id, t.name, t.description, t.created_at, t.updated_at, t.enabled
		FROM rbac_teams t
		INNER JOIN rbac_token_memberships m ON t.id = m.team_id
		WHERE m.token_id = ?
		ORDER BY t.name
	`, tokenID)
	if err != nil {
		return nil, fmt.Errorf("failed to get token teams: %w", err)
	}
	defer rows.Close()

	var teams []Team
	for rows.Next() {
		var team Team
		if err := rows.Scan(&team.ID, &team.OrganizationID, &team.Name, &team.Description, &team.CreatedAt, &team.UpdatedAt, &team.Enabled); err != nil {
			return nil, fmt.Errorf("failed to scan team: %w", err)
		}
		teams = append(teams, team)
	}
	return teams, nil
}

// =============================================================================
// Permission Checking (with caching)
// =============================================================================

// CheckPermission checks if a token has a specific permission for a resource
// Uses two-level caching: permission result cache + token RBAC data cache
func (rm *RBACManager) CheckPermission(req *PermissionCheckRequest) *PermissionCheckResult {
	if req.TokenInfo == nil {
		return &PermissionCheckResult{
			Allowed: false,
			Source:  "denied",
			Reason:  "no token provided",
		}
	}

	// If RBAC is not enabled, use OSS token permissions only (no caching needed - it's fast)
	if !rm.IsRBACEnabled() {
		return rm.checkOSSPermission(req)
	}

	// Check permission result cache first
	cacheKey := permissionCacheKey{
		tokenID:     req.TokenInfo.ID,
		database:    req.Database,
		measurement: req.Measurement,
		permission:  req.Permission,
	}

	rm.permCacheMu.RLock()
	if entry, ok := rm.permCache[cacheKey]; ok && time.Now().Before(entry.expiresAt) {
		rm.permCacheMu.RUnlock()
		rm.cacheHits.Add(1)
		return entry.result
	}
	rm.permCacheMu.RUnlock()
	rm.cacheMisses.Add(1)

	// Cache miss - compute permission
	result := rm.checkPermissionUncached(req)

	// Cache the result
	rm.permCacheMu.Lock()
	rm.evictPermCacheIfFull()
	rm.permCache[cacheKey] = &permissionCacheEntry{
		result:    result,
		expiresAt: time.Now().Add(rm.permCacheTTL),
	}
	rm.permCacheMu.Unlock()

	return result
}

// CheckPermissionsBatch checks permissions for multiple resources in a single call.
// This is more efficient than multiple CheckPermission calls when checking permissions
// for the same token across multiple tables (e.g., multi-table queries).
// It loads the token's RBAC data once and checks all permissions against it.
func (rm *RBACManager) CheckPermissionsBatch(reqs []*PermissionCheckRequest) []*PermissionCheckResult {
	if len(reqs) == 0 {
		return nil
	}

	results := make([]*PermissionCheckResult, len(reqs))

	// If RBAC is not enabled, use OSS token permissions only (fast path)
	if !rm.IsRBACEnabled() {
		for i, req := range reqs {
			if req.TokenInfo == nil {
				results[i] = &PermissionCheckResult{
					Allowed: false,
					Source:  "denied",
					Reason:  "no token provided",
				}
			} else {
				results[i] = rm.checkOSSPermission(req)
			}
		}
		return results
	}

	// Group requests by token ID to batch-load RBAC data
	type indexedReq struct {
		index int
		req   *PermissionCheckRequest
	}
	tokenGroups := make(map[int64][]indexedReq)

	for i, req := range reqs {
		if req.TokenInfo == nil {
			results[i] = &PermissionCheckResult{
				Allowed: false,
				Source:  "denied",
				Reason:  "no token provided",
			}
			continue
		}
		tokenGroups[req.TokenInfo.ID] = append(tokenGroups[req.TokenInfo.ID], indexedReq{index: i, req: req})
	}

	// Process each token's requests with single RBAC data load
	for tokenID, indexedReqs := range tokenGroups {
		// Load RBAC data once for all requests with this token
		rbacData, err := rm.getTokenRBACData(tokenID)
		if err != nil {
			rm.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to get token RBAC data for batch")
			// Fall back to OSS permissions for all requests with this token
			for _, ir := range indexedReqs {
				results[ir.index] = rm.checkOSSPermission(ir.req)
			}
			continue
		}

		// Check each permission using the loaded RBAC data
		for _, ir := range indexedReqs {
			req := ir.req

			// Check permission result cache first
			cacheKey := permissionCacheKey{
				tokenID:     req.TokenInfo.ID,
				database:    req.Database,
				measurement: req.Measurement,
				permission:  req.Permission,
			}

			rm.permCacheMu.RLock()
			entry, cacheHit := rm.permCache[cacheKey]
			if cacheHit && time.Now().Before(entry.expiresAt) {
				rm.permCacheMu.RUnlock()
				rm.cacheHits.Add(1)
				results[ir.index] = entry.result
				continue
			}
			rm.permCacheMu.RUnlock()
			rm.cacheMisses.Add(1)

			// Compute permission using cached RBAC data
			var result *PermissionCheckResult

			// No team memberships - use OSS permissions (backward compat)
			if len(rbacData.teams) == 0 {
				result = rm.checkOSSPermission(req)
			} else if rm.checkRBACPermissionCached(req, rbacData) {
				result = &PermissionCheckResult{
					Allowed: true,
					Source:  "rbac",
				}
			} else {
				// RBAC denied, fall back to OSS permissions
				ossResult := rm.checkOSSPermission(req)
				if ossResult.Allowed {
					result = ossResult
				} else {
					result = &PermissionCheckResult{
						Allowed: false,
						Source:  "denied",
						Reason:  fmt.Sprintf("no permission for %s on database '%s'", req.Permission, req.Database),
					}
				}
			}

			// Cache the result
			rm.permCacheMu.Lock()
			rm.evictPermCacheIfFull()
			rm.permCache[cacheKey] = &permissionCacheEntry{
				result:    result,
				expiresAt: time.Now().Add(rm.permCacheTTL),
			}
			rm.permCacheMu.Unlock()

			results[ir.index] = result
		}
	}

	return results
}

// checkPermissionUncached performs the actual permission check without caching
func (rm *RBACManager) checkPermissionUncached(req *PermissionCheckRequest) *PermissionCheckResult {
	// Get or load token RBAC data (cached)
	rbacData, err := rm.getTokenRBACData(req.TokenInfo.ID)
	if err != nil {
		rm.logger.Error().Err(err).Msg("Failed to get token RBAC data")
		return rm.checkOSSPermission(req)
	}

	// No team memberships - use OSS permissions (backward compat)
	if len(rbacData.teams) == 0 {
		return rm.checkOSSPermission(req)
	}

	// Check RBAC permissions using cached data
	if rm.checkRBACPermissionCached(req, rbacData) {
		return &PermissionCheckResult{
			Allowed: true,
			Source:  "rbac",
		}
	}

	// RBAC denied, fall back to OSS permissions
	ossResult := rm.checkOSSPermission(req)
	if ossResult.Allowed {
		return ossResult
	}

	return &PermissionCheckResult{
		Allowed: false,
		Source:  "denied",
		Reason:  fmt.Sprintf("no permission for %s on database '%s'", req.Permission, req.Database),
	}
}

// getTokenRBACData gets or loads all RBAC data for a token (cached)
// Uses double-checked locking to prevent TOCTOU race conditions
func (rm *RBACManager) getTokenRBACData(tokenID int64) (*tokenRBACData, error) {
	now := time.Now()

	// Fast path: check with read lock
	rm.tokenCacheMu.RLock()
	if data, ok := rm.tokenCache[tokenID]; ok && now.Sub(data.loadedAt) < rm.tokenCacheTTL {
		rm.tokenCacheMu.RUnlock()
		return data, nil
	}
	rm.tokenCacheMu.RUnlock()

	// Slow path: acquire write lock and double-check
	rm.tokenCacheMu.Lock()
	defer rm.tokenCacheMu.Unlock()

	// Double-check after acquiring write lock - another goroutine may have loaded it
	if data, ok := rm.tokenCache[tokenID]; ok && now.Sub(data.loadedAt) < rm.tokenCacheTTL {
		return data, nil
	}

	// Cache miss - load all RBAC data for this token in optimized queries
	data, err := rm.loadTokenRBACData(tokenID)
	if err != nil {
		return nil, err
	}

	// Cache the result (already holding write lock)
	rm.evictTokenCacheIfFull()
	rm.tokenCache[tokenID] = data

	return data, nil
}

// loadTokenRBACData loads all RBAC data for a token in minimal queries
func (rm *RBACManager) loadTokenRBACData(tokenID int64) (*tokenRBACData, error) {
	data := &tokenRBACData{
		roles:     make(map[int64][]Role),
		measPerms: make(map[int64][]MeasurementPermission),
		loadedAt:  time.Now(),
	}

	// Query 1: Get all teams for this token
	teams, err := rm.GetTokenTeams(tokenID)
	if err != nil {
		return nil, err
	}
	data.teams = teams

	if len(teams) == 0 {
		return data, nil
	}

	// Build team IDs for IN clause
	teamIDs := make([]interface{}, len(teams))
	placeholders := make([]string, len(teams))
	for i, t := range teams {
		teamIDs[i] = t.ID
		placeholders[i] = "?"
	}

	// Query 2: Get all roles for all teams in one query
	roleQuery := fmt.Sprintf(`
		SELECT id, team_id, database_pattern, permissions, created_at
		FROM rbac_roles WHERE team_id IN (%s) ORDER BY team_id, database_pattern
	`, strings.Join(placeholders, ","))

	rows, err := rm.db.Query(roleQuery, teamIDs...)
	if err != nil {
		return nil, fmt.Errorf("failed to load roles: %w", err)
	}

	var roleIDs []interface{}
	var rolePlaceholders []string
	for rows.Next() {
		var role Role
		var permsJSON string
		if err := rows.Scan(&role.ID, &role.TeamID, &role.DatabasePattern, &permsJSON, &role.CreatedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan role: %w", err)
		}
		role.Permissions = splitPermissions(permsJSON)
		data.roles[role.TeamID] = append(data.roles[role.TeamID], role)
		roleIDs = append(roleIDs, role.ID)
		rolePlaceholders = append(rolePlaceholders, "?")
	}
	rows.Close()

	// Query 3: Get all measurement permissions for all roles in one query
	if len(roleIDs) > 0 {
		measQuery := fmt.Sprintf(`
			SELECT id, role_id, measurement_pattern, permissions, created_at
			FROM rbac_measurement_permissions WHERE role_id IN (%s) ORDER BY role_id, measurement_pattern
		`, strings.Join(rolePlaceholders, ","))

		rows, err := rm.db.Query(measQuery, roleIDs...)
		if err != nil {
			return nil, fmt.Errorf("failed to load measurement permissions: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var mp MeasurementPermission
			var permsJSON string
			if err := rows.Scan(&mp.ID, &mp.RoleID, &mp.MeasurementPattern, &permsJSON, &mp.CreatedAt); err != nil {
				return nil, fmt.Errorf("failed to scan measurement permission: %w", err)
			}
			mp.Permissions = splitPermissions(permsJSON)
			data.measPerms[mp.RoleID] = append(data.measPerms[mp.RoleID], mp)
		}
	}

	return data, nil
}

// checkOSSPermission checks permissions using OSS token model
func (rm *RBACManager) checkOSSPermission(req *PermissionCheckRequest) *PermissionCheckResult {
	for _, p := range req.TokenInfo.Permissions {
		if p == "admin" || p == req.Permission {
			return &PermissionCheckResult{
				Allowed: true,
				Source:  "token",
			}
		}
	}
	return &PermissionCheckResult{
		Allowed: false,
		Source:  "denied",
		Reason:  fmt.Sprintf("token does not have '%s' permission", req.Permission),
	}
}

// checkRBACPermissionCached checks permissions using cached RBAC data (no DB queries)
func (rm *RBACManager) checkRBACPermissionCached(req *PermissionCheckRequest, data *tokenRBACData) bool {
	// Check if token is disabled - deny immediately
	if !req.TokenInfo.Enabled {
		return false
	}

	for _, team := range data.teams {
		if !team.Enabled {
			continue
		}

		roles := data.roles[team.ID]
		for _, role := range roles {
			// Check if database pattern matches
			if !matchPattern(role.DatabasePattern, req.Database) {
				continue
			}

			// If measurement is specified, check measurement permissions
			if req.Measurement != "" {
				measPerms := data.measPerms[role.ID]

				// If there are measurement permissions, check them
				if len(measPerms) > 0 {
					for _, mp := range measPerms {
						if matchPattern(mp.MeasurementPattern, req.Measurement) {
							if containsPermission(mp.Permissions, req.Permission) {
								return true
							}
						}
					}
					// Has measurement permissions but none matched - deny for this role
					continue
				}
			}

			// Check role-level permissions (applies if no measurement filter or no measurement permissions defined)
			if containsPermission(role.Permissions, req.Permission) {
				return true
			}
		}
	}
	return false
}

// checkRBACPermission checks if any team/role grants the permission (uncached version for backwards compat)
func (rm *RBACManager) checkRBACPermission(req *PermissionCheckRequest, teams []Team) bool {
	for _, team := range teams {
		if !team.Enabled {
			continue
		}

		roles, err := rm.ListRolesByTeam(team.ID)
		if err != nil {
			rm.logger.Error().Err(err).Int64("team_id", team.ID).Msg("Failed to get team roles")
			continue
		}

		for _, role := range roles {
			// Check if database pattern matches
			if !matchPattern(role.DatabasePattern, req.Database) {
				continue
			}

			// If measurement is specified, check measurement permissions
			if req.Measurement != "" {
				// Get measurement-level permissions for this role
				measPerms, err := rm.ListMeasurementPermissionsByRole(role.ID)
				if err != nil {
					rm.logger.Error().Err(err).Int64("role_id", role.ID).Msg("Failed to get measurement permissions")
					continue
				}

				// If there are measurement permissions, check them
				if len(measPerms) > 0 {
					for _, mp := range measPerms {
						if matchPattern(mp.MeasurementPattern, req.Measurement) {
							if containsPermission(mp.Permissions, req.Permission) {
								return true
							}
						}
					}
					// Has measurement permissions but none matched - deny for this role
					continue
				}
			}

			// Check role-level permissions (applies if no measurement filter or no measurement permissions defined)
			if containsPermission(role.Permissions, req.Permission) {
				return true
			}
		}
	}
	return false
}

// GetEffectivePermissions returns all effective permissions for a token
func (rm *RBACManager) GetEffectivePermissions(tokenID int64, tokenInfo *TokenInfo) ([]EffectivePermission, error) {
	var perms []EffectivePermission

	// Add OSS token permissions
	if tokenInfo != nil && len(tokenInfo.Permissions) > 0 {
		perms = append(perms, EffectivePermission{
			Database:    "*",
			Permissions: tokenInfo.Permissions,
			Source:      "token",
		})
	}

	// If RBAC is not enabled, return only OSS permissions
	if !rm.IsRBACEnabled() {
		return perms, nil
	}

	// Get RBAC permissions from team memberships
	teams, err := rm.GetTokenTeams(tokenID)
	if err != nil {
		return nil, fmt.Errorf("failed to get token teams: %w", err)
	}

	for _, team := range teams {
		if !team.Enabled {
			continue
		}

		roles, err := rm.ListRolesByTeam(team.ID)
		if err != nil {
			rm.logger.Error().Err(err).Int64("team_id", team.ID).Msg("Failed to get team roles")
			continue
		}

		for _, role := range roles {
			// Add role-level permissions
			perms = append(perms, EffectivePermission{
				Database:    role.DatabasePattern,
				Permissions: role.Permissions,
				Source:      "rbac",
			})

			// Add measurement-level permissions
			measPerms, err := rm.ListMeasurementPermissionsByRole(role.ID)
			if err != nil {
				rm.logger.Error().Err(err).Int64("role_id", role.ID).Msg("Failed to get measurement permissions")
				continue
			}

			for _, mp := range measPerms {
				perms = append(perms, EffectivePermission{
					Database:    role.DatabasePattern,
					Measurement: mp.MeasurementPattern,
					Permissions: mp.Permissions,
					Source:      "rbac",
				})
			}
		}
	}

	return perms, nil
}

// =============================================================================
// Helper Functions
// =============================================================================

// IsValidPermission checks if a permission string is valid
func IsValidPermission(p string) bool {
	switch p {
	case "read", "write", "delete", "admin":
		return true
	default:
		return false
	}
}

// containsPermission checks if a permission list contains a specific permission
func containsPermission(perms []string, target string) bool {
	for _, p := range perms {
		if p == "admin" || p == target {
			return true
		}
	}
	return false
}

// splitPermissions splits a comma-separated permission string into a slice
func splitPermissions(perms string) []string {
	if perms == "" {
		return nil
	}
	return strings.Split(perms, ",")
}

// matchPattern checks if a value matches a pattern (supports * and prefix_* wildcards)
func matchPattern(pattern, value string) bool {
	// Exact wildcard
	if pattern == "*" {
		return true
	}

	// Prefix wildcard (e.g., "prod_*" matches "prod_us", "prod_eu")
	if strings.HasSuffix(pattern, "_*") {
		prefix := strings.TrimSuffix(pattern, "_*")
		return strings.HasPrefix(value, prefix+"_")
	}

	// Suffix wildcard (e.g., "*_metrics" matches "cpu_metrics", "memory_metrics")
	if strings.HasPrefix(pattern, "*_") {
		suffix := strings.TrimPrefix(pattern, "*_")
		return strings.HasSuffix(value, "_"+suffix)
	}

	// General wildcard at end (e.g., "prod*" matches "production", "prod_us")
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(value, prefix)
	}

	// Exact match
	return pattern == value
}

// readBackAfterPropose retries a read-back Scan that may race the local
// FSM apply on a follower. When a Create<X> RBAC method proposes via
// Raft, Propose blocks until the leader commits the entry. On the
// leader itself the local apply callback has already fired by the time
// Propose returns — but on a follower the entry replicates
// asynchronously via the runFSM goroutine and may not have hit local
// SQLite yet. Without retry, the immediate read-back can return
// sql.ErrNoRows and the API caller sees a 500.
//
// Retry shape: exponential backoff capped at ~785ms total. The
// proposeTimeout elsewhere is 5s so we stay well under that. Returns
// the last error (typically sql.ErrNoRows) if all attempts fail.
//
// ctx is honoured between attempts via select on time.After + ctx.Done
// so a cancelled / timed-out request doesn't keep the goroutine
// blocked in time.Sleep. Gemini PR #458 round 2 (introduction) +
// round 4 (ctx propagation).
func readBackAfterPropose(ctx context.Context, scan func() error) error {
	delays := []time.Duration{
		0, // immediate
		10 * time.Millisecond,
		25 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
	}
	var lastErr error
	for _, d := range delays {
		// Honour ctx at the top of every iteration in addition to the
		// select-during-wait arm below. Covers two cases the select
		// alone misses: (a) first iteration has d==0 and skips the
		// select, so a pre-cancelled ctx would still run one Scan; and
		// (b) ctx cancelled while the previous iteration's Scan was
		// in flight. Gemini PR #458 round 6.
		if err := ctx.Err(); err != nil {
			return err
		}
		if d > 0 {
			// time.NewTimer + Stop on the cancel arm avoids the
			// time.After leak (the underlying Timer can't be GC'd
			// until it fires, even after ctx cancels). Under hot
			// churn — many cancelled creates — the leaked timers
			// pile up briefly. Gemini PR #458 round 8.
			timer := time.NewTimer(d)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		err := scan()
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			// Non-row-missing error: don't retry, return immediately.
			return err
		}
		lastErr = err
	}
	return lastErr
}

// countOrgCascadeDescendants returns the total number of descendants
// (teams + roles + measurement_permissions + token_memberships) under
// the given organization id. Used by DeleteOrganization in cluster mode
// to pre-check against maxCascadeDescendants before proposing — turning
// a "FSM apply blocks past the Raft heartbeat margin" failure into a
// clean 409 Conflict at the API. Phase A.2 Item 2.
//
// All four COUNTs run against local SQLite under the same ctx. The
// nested IN subqueries lean on the existing per-table indexes:
//   - rbac_teams(organization_id)
//   - rbac_roles(team_id)
//   - rbac_measurement_permissions(role_id)
//   - rbac_token_memberships(team_id)
//
// At realistic cap values (default 50000) the four COUNTs together
// finish in a few ms on local SQLite — well under the proposeTimeout.
func (rm *RBACManager) countOrgCascadeDescendants(ctx context.Context, orgID int64) (int, error) {
	var teams, roles, mperms, memberships int
	if err := rm.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rbac_teams WHERE organization_id = ?`,
		orgID,
	).Scan(&teams); err != nil {
		return 0, fmt.Errorf("count teams: %w", err)
	}
	// JOIN shape (Gemini round 1 PR #459) measurably outperforms
	// nested-IN at every fixture size we benched: 1.38x at small
	// (~250 descendants), 1.27x at medium (~3.5k), 1.23x at large
	// (~24k). Absolute cost stays sub-ms either way; readability is
	// the other win.
	if err := rm.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM rbac_roles r
		JOIN rbac_teams t ON r.team_id = t.id
		WHERE t.organization_id = ?
	`, orgID).Scan(&roles); err != nil {
		return 0, fmt.Errorf("count roles: %w", err)
	}
	if err := rm.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM rbac_measurement_permissions mp
		JOIN rbac_roles r ON mp.role_id = r.id
		JOIN rbac_teams t ON r.team_id = t.id
		WHERE t.organization_id = ?
	`, orgID).Scan(&mperms); err != nil {
		return 0, fmt.Errorf("count measurement_permissions: %w", err)
	}
	if err := rm.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM rbac_token_memberships tm
		JOIN rbac_teams t ON tm.team_id = t.id
		WHERE t.organization_id = ?
	`, orgID).Scan(&memberships); err != nil {
		return 0, fmt.Errorf("count token_memberships: %w", err)
	}
	return teams + roles + mperms + memberships, nil
}

// countTeamCascadeDescendants returns the total descendants under the
// given team id (roles + measurement_permissions + memberships). The
// DeleteTeam cascade is 3-level (no parent org rebase). Phase A.2 Item 2.
func (rm *RBACManager) countTeamCascadeDescendants(ctx context.Context, teamID int64) (int, error) {
	var roles, mperms, memberships int
	if err := rm.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rbac_roles WHERE team_id = ?`,
		teamID,
	).Scan(&roles); err != nil {
		return 0, fmt.Errorf("count roles: %w", err)
	}
	if err := rm.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM rbac_measurement_permissions mp
		JOIN rbac_roles r ON mp.role_id = r.id
		WHERE r.team_id = ?
	`, teamID).Scan(&mperms); err != nil {
		return 0, fmt.Errorf("count measurement_permissions: %w", err)
	}
	if err := rm.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rbac_token_memberships WHERE team_id = ?`,
		teamID,
	).Scan(&memberships); err != nil {
		return 0, fmt.Errorf("count token_memberships: %w", err)
	}
	return roles + mperms + memberships, nil
}

// ErrCascadeCapExceeded is the sentinel returned by DeleteOrganization
// and DeleteTeam (cluster mode) when the local descendant count exceeds
// rm.maxCascadeDescendants. HTTP handlers detect this via errors.Is and
// map it to 409 Conflict. Phase A.2 Item 2.
var ErrCascadeCapExceeded = errors.New("cascade exceeds configured limit")

// nextProposerTimestamp returns a time.Time that is guaranteed to be
// strictly increasing across concurrent callers within a single
// RBACManager process — even when time.Now() collides at the same
// nanosecond under heavy concurrency. Used by Create<X> RBAC paths
// that lack a schema-level UNIQUE constraint on (parent_id, key,
// value) and rely on `created_at = proposer's now` to disambiguate
// the read-back across concurrent identical creates.
//
// Mechanism: CAS loop on proposerNowUnixNano. Each call computes
// max(prev+1, time.Now().UnixNano()) and tries to CAS it into the
// shared counter; on contention it retries with the freshly-read
// prev. This is strictly monotonic by construction: every successful
// CAS writes a value greater than the previous load, and the loaded
// value seen by any later caller is at least that. Under low
// concurrency the returned value equals wall-clock time.Now(); under
// heavy concurrency it can drift forward by 1ns per colliding caller,
// which is fine — `created_at` is "when the proposer issued this
// command," not "exact wall clock."
//
// The returned time is always in UTC. SQLite serialises time.Time
// values via the driver's TimeZone-aware text encoder; a proposer
// node and an applier-side read-back running with different local
// timezones would produce different text representations of the same
// instant, and the `created_at = ?` match would fall through. UTC
// removes the variable so cross-node and cross-process matches are
// byte-identical. (Gemini round 1 PR #459.)
//
// Phase A.2 follow-up to PR #458 round 9 G28: the original fix
// (matching readback on `created_at = time.Now()`) raced under
// concurrent identical CreateRole calls on fast machines.
func (rm *RBACManager) nextProposerTimestamp() time.Time {
	for {
		prev := rm.proposerNowUnixNano.Load()
		now := time.Now().UnixNano()
		next := now
		if prev >= next {
			next = prev + 1
		}
		if rm.proposerNowUnixNano.CompareAndSwap(prev, next) {
			return time.Unix(0, next).UTC()
		}
	}
}
