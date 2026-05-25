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
		done:          make(chan struct{}),
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
		// Apply callback fired on this node before Propose returned — look
		// up the materialised row to surface the Raft-stamped ID and the
		// stored timestamps.
		rm.logger.Info().Str("name", req.Name).Msg("Created organization via Raft")
		var org Organization
		if err := rm.db.QueryRow(`
			SELECT id, name, description, created_at, updated_at, enabled
			FROM rbac_organizations WHERE name = ?
		`, req.Name).Scan(&org.ID, &org.Name, &org.Description, &org.CreatedAt, &org.UpdatedAt, &org.Enabled); err != nil {
			return nil, fmt.Errorf("failed to read back organization after Raft apply: %w", err)
		}
		return &org, nil
	}

	_ = ctx
	now := time.Now()
	result, err := rm.db.Exec(`
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

	_ = ctx
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
	args = append(args, time.Now())
	args = append(args, id)

	query := fmt.Sprintf("UPDATE rbac_organizations SET %s WHERE id = ?", strings.Join(updates, ", "))
	result, err := rm.db.Exec(query, args...)
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
		payload := deleteOrganizationPayloadWire{ID: id}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandDeleteOrganization, payload); err != nil {
			return fmt.Errorf("failed to delete organization: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Deleted organization via Raft")
		return nil
	}

	_ = ctx
	result, err := rm.db.Exec("DELETE FROM rbac_organizations WHERE id = ?", id)
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
		if err := rm.db.QueryRow(`
			SELECT id, organization_id, name, description, created_at, updated_at, enabled
			FROM rbac_teams WHERE organization_id = ? AND name = ?
		`, orgID, req.Name).Scan(&team.ID, &team.OrganizationID, &team.Name, &team.Description, &team.CreatedAt, &team.UpdatedAt, &team.Enabled); err != nil {
			return nil, fmt.Errorf("failed to read back team after Raft apply: %w", err)
		}
		return &team, nil
	}

	_ = ctx
	// Verify organization exists
	org, err := rm.GetOrganization(orgID)
	if err != nil {
		return nil, err
	}
	if org == nil {
		return nil, errors.New("organization not found")
	}

	now := time.Now()
	result, err := rm.db.Exec(`
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

	_ = ctx
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
	args = append(args, time.Now())
	args = append(args, id)

	query := fmt.Sprintf("UPDATE rbac_teams SET %s WHERE id = ?", strings.Join(updates, ", "))
	result, err := rm.db.Exec(query, args...)
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
		payload := deleteTeamPayloadWire{ID: id}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandDeleteTeam, payload); err != nil {
			return fmt.Errorf("failed to delete team: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Deleted team via Raft")
		return nil
	}

	_ = ctx
	result, err := rm.db.Exec("DELETE FROM rbac_teams WHERE id = ?", id)
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
		now := time.Now()
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
		// Read back. Roles aren't UNIQUE on (team_id, database_pattern) —
		// pick the newest matching row by created_at to avoid a stale
		// read if the same handler call retries.
		var role Role
		var dbPerms string
		if err := rm.db.QueryRow(`
			SELECT id, team_id, database_pattern, permissions, created_at
			FROM rbac_roles
			WHERE team_id = ? AND database_pattern = ? AND permissions = ?
			ORDER BY created_at DESC, id DESC
			LIMIT 1
		`, teamID, req.DatabasePattern, perms).Scan(&role.ID, &role.TeamID, &role.DatabasePattern, &dbPerms, &role.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to read back role after Raft apply: %w", err)
		}
		role.Permissions = strings.Split(dbPerms, ",")
		return &role, nil
	}

	_ = ctx
	team, err := rm.GetTeam(teamID)
	if err != nil {
		return nil, err
	}
	if team == nil {
		return nil, errors.New("team not found")
	}

	now := time.Now()
	perms := strings.Join(req.Permissions, ",")
	result, err := rm.db.Exec(`
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

	_ = ctx
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
	result, err := rm.db.Exec(query, args...)
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
		payload := deleteRolePayloadWire{ID: id}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandDeleteRole, payload); err != nil {
			return fmt.Errorf("failed to delete role: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Deleted role via Raft")
		return nil
	}

	_ = ctx
	result, err := rm.db.Exec("DELETE FROM rbac_roles WHERE id = ?", id)
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
		now := time.Now()
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
		var mp MeasurementPermission
		var dbPerms string
		if err := rm.db.QueryRow(`
			SELECT id, role_id, measurement_pattern, permissions, created_at
			FROM rbac_measurement_permissions
			WHERE role_id = ? AND measurement_pattern = ? AND permissions = ?
			ORDER BY created_at DESC, id DESC
			LIMIT 1
		`, roleID, req.MeasurementPattern, perms).Scan(&mp.ID, &mp.RoleID, &mp.MeasurementPattern, &dbPerms, &mp.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to read back measurement permission after Raft apply: %w", err)
		}
		mp.Permissions = strings.Split(dbPerms, ",")
		return &mp, nil
	}

	_ = ctx
	role, err := rm.GetRole(roleID)
	if err != nil {
		return nil, err
	}
	if role == nil {
		return nil, errors.New("role not found")
	}

	now := time.Now()
	perms := strings.Join(req.Permissions, ",")
	result, err := rm.db.Exec(`
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
		payload := deleteMeasurementPermissionPayloadWire{ID: id}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandDeleteMeasurementPermission, payload); err != nil {
			return fmt.Errorf("failed to delete measurement permission: %w", err)
		}
		rm.logger.Info().Int64("id", id).Msg("Deleted measurement permission via Raft")
		return nil
	}

	_ = ctx
	result, err := rm.db.Exec("DELETE FROM rbac_measurement_permissions WHERE id = ?", id)
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
		if err := rm.db.QueryRow(`
			SELECT id, token_id, team_id, created_at
			FROM rbac_token_memberships WHERE token_id = ? AND team_id = ?
		`, tokenID, teamID).Scan(&mem.ID, &mem.TokenID, &mem.TeamID, &mem.CreatedAt); err != nil {
			return nil, fmt.Errorf("failed to read back token membership after Raft apply: %w", err)
		}
		return &mem, nil
	}

	_ = ctx
	team, err := rm.GetTeam(teamID)
	if err != nil {
		return nil, err
	}
	if team == nil {
		return nil, errors.New("team not found")
	}

	now := time.Now()
	result, err := rm.db.Exec(`
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
		payload := removeTokenFromTeamPayloadWire{TokenID: tokenID, TeamID: teamID}
		if err := rm.proposeRBACCommand(ctx, ProposalCommandRemoveTokenFromTeam, payload); err != nil {
			return fmt.Errorf("failed to remove token from team: %w", err)
		}
		rm.logger.Info().Int64("token_id", tokenID).Int64("team_id", teamID).Msg("Removed token from team via Raft")
		return nil
	}

	_ = ctx
	result, err := rm.db.Exec(`
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
