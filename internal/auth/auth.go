package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/sqlitex"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"

	_ "github.com/mattn/go-sqlite3"
)

// PermissionsNone is the explicit sentinel a caller passes to CreateToken /
// CreateTokenWithValue to request a token with NO OSS-level permissions (an
// RBAC-only token, whose access comes solely from team/role grants).
//
// It exists because the permissions parameter is overloaded: an empty string
// ("") means "use the default read,write" — a contract relied on by bootstrap
// and existing callers (see TestCreateTokenWithValue "default permissions are
// read,write when empty"). Without a distinct sentinel there is no way to
// express "deliberately empty", so requests for a least-privilege RBAC-only
// token were silently upgraded to read,write — a privilege-escalation-shaped
// bug (a token meant to be scoped to one database via RBAC instead got
// wildcard read,write). The sentinel is normalised to a stored empty string
// by storePermissions; the verify path already maps stored-empty to an empty
// permission set (RBAC-only).
const PermissionsNone = "\x00none"

// storePermissions resolves the overloaded permissions argument into the exact
// string persisted in api_tokens.permissions:
//   - ""              => "read,write" (default contract, unchanged)
//   - PermissionsNone => ""           (deliberate RBAC-only, no OSS perms)
//   - anything else   => as-is
func storePermissions(permissions string) string {
	switch permissions {
	case "":
		return "read,write"
	case PermissionsNone:
		return ""
	default:
		return permissions
	}
}

// TokenInfo represents token metadata returned by verify
type TokenInfo struct {
	ID          int64      `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description,omitempty"`
	Permissions []string   `json:"permissions"`
	CreatedAt   time.Time  `json:"created_at"`
	LastUsedAt  time.Time  `json:"last_used_at,omitempty"`
	Enabled     bool       `json:"enabled"`
	ExpiresAt   *time.Time `json:"expires_at"`
}

// cacheEntry represents a cached token
type cacheEntry struct {
	info      *TokenInfo
	expiresAt time.Time
}

// AuthManager handles API token authentication with SQLite storage
type AuthManager struct {
	db           *sql.DB
	dbPath       string
	cacheTTL     time.Duration
	maxCacheSize int

	cache          map[string]cacheEntry // cache key (sha256 of token) -> cached info
	cacheMu        sync.RWMutex
	cacheHits      atomic.Int64
	cacheMisses    atomic.Int64
	cacheEvictions atomic.Int64

	cleanupDone chan struct{}
	logger      zerolog.Logger

	// proposer is the Raft seam for cluster-wide auth state replication.
	// When non-nil (Enterprise cluster mode), every write method routes
	// through here so the change applies on every node via the FSM. When
	// nil (OSS / standalone), the write methods fall through to the
	// existing direct-SQLite path — runtime behavior unchanged for OSS.
	// Set via SetRaftProposer, typically once at startup from
	// cmd/arc/main.go after both AuthManager and the cluster coordinator
	// are constructed. Phase A: Cluster Auth Convergence.
	//
	// proposerMu guards reads of `proposer` against the rare reconfigure
	// case (Stop() unsetting it during shutdown). All hot-path writes
	// take it for read.
	proposer   RaftProposer
	proposerMu sync.RWMutex
}

// Logger returns the auth component logger.
func (am *AuthManager) Logger() zerolog.Logger {
	return am.logger
}

// NewAuthManager creates a new authentication manager
func NewAuthManager(dbPath string, cacheTTL time.Duration, maxCacheSize int, logger zerolog.Logger) (*AuthManager, error) {
	// Open with owner-only (0600) permissions. dbPath is a bare filesystem path
	// by contract (e.g. "./data/arc.db" — see the auth.db_path default in
	// internal/config); sqlitex.Open appends the query params below, so dbPath
	// must NOT already be a file: URI or carry "?". The auth DB holds token
	// hashes and must never be world-readable. See internal/sqlitex (the shared
	// hardening helper extracted from this function per security finding M4).
	db, err := sqlitex.Open(dbPath, "_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=ON")
	if err != nil {
		return nil, fmt.Errorf("failed to open auth database: %w", err)
	}

	am := &AuthManager{
		db:           db,
		dbPath:       dbPath,
		cacheTTL:     cacheTTL,
		maxCacheSize: maxCacheSize,
		cache:        make(map[string]cacheEntry),
		cleanupDone:  make(chan struct{}),
		logger:       logger.With().Str("component", "auth").Logger(),
	}

	if err := am.initDB(); err != nil {
		db.Close()
		return nil, err
	}

	// Lock the -wal/-shm sidecars, which SQLite creates on first write (initDB
	// above). They can contain recently-committed token hashes, so they must
	// not inherit the process umask.
	if err := sqlitex.HardenWALSHM(dbPath, am.logger); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to harden auth DB WAL/SHM permissions: %w", err)
	}

	// Start background cleanup
	go am.cleanupLoop()

	am.logger.Debug().
		Str("db_path", dbPath).
		Dur("cache_ttl", cacheTTL).
		Int("max_cache_size", maxCacheSize).
		Msg("AuthManager initialized")

	return am, nil
}

// initDB creates the tokens table if it doesn't exist
func (am *AuthManager) initDB() error {
	_, err := am.db.Exec(`
		CREATE TABLE IF NOT EXISTS api_tokens (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			token_hash TEXT NOT NULL,
			description TEXT,
			permissions TEXT DEFAULT 'read,write',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			last_used_at TIMESTAMP,
			expires_at TIMESTAMP,
			enabled INTEGER DEFAULT 1
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create api_tokens table: %w", err)
	}

	// Create index for faster lookups
	_, err = am.db.Exec(`CREATE INDEX IF NOT EXISTS idx_api_tokens_enabled ON api_tokens(enabled)`)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// Migration: add expires_at column if it doesn't exist (for databases created by Python)
	_, err = am.db.Exec(`ALTER TABLE api_tokens ADD COLUMN expires_at TIMESTAMP`)
	if err != nil {
		// Ignore "duplicate column" error - column already exists
		if !strings.Contains(err.Error(), "duplicate column") {
			am.logger.Debug().Err(err).Msg("expires_at column already exists or migration failed")
		}
	}

	// Migration: add token_prefix column for O(1) token lookup optimization
	// token_prefix stores first 16 chars of SHA256(token) for fast filtering
	_, err = am.db.Exec(`ALTER TABLE api_tokens ADD COLUMN token_prefix TEXT`)
	if err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			am.logger.Debug().Err(err).Msg("token_prefix column already exists or migration failed")
		}
	}

	// Create index on token_prefix for fast lookups
	_, err = am.db.Exec(`CREATE INDEX IF NOT EXISTS idx_api_tokens_prefix ON api_tokens(token_prefix)`)
	if err != nil {
		am.logger.Debug().Err(err).Msg("Failed to create token_prefix index")
	}

	// Backfill token_prefix for existing tokens that don't have it
	// This is a one-time migration that will be skipped if all tokens have prefixes
	am.backfillTokenPrefixes()

	// Initialize RBAC tables (Enterprise feature)
	if err := am.initRBACTables(); err != nil {
		return fmt.Errorf("failed to initialize RBAC tables: %w", err)
	}

	return nil
}

// initRBACTables creates the RBAC tables for enterprise features
func (am *AuthManager) initRBACTables() error {
	// Organizations table
	_, err := am.db.Exec(`
		CREATE TABLE IF NOT EXISTS rbac_organizations (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL UNIQUE,
			description TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			enabled INTEGER DEFAULT 1
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create rbac_organizations table: %w", err)
	}

	// Teams table
	_, err = am.db.Exec(`
		CREATE TABLE IF NOT EXISTS rbac_teams (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			organization_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			description TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			enabled INTEGER DEFAULT 1,
			FOREIGN KEY (organization_id) REFERENCES rbac_organizations(id) ON DELETE CASCADE,
			UNIQUE(organization_id, name)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create rbac_teams table: %w", err)
	}

	// Roles table
	_, err = am.db.Exec(`
		CREATE TABLE IF NOT EXISTS rbac_roles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			team_id INTEGER NOT NULL,
			database_pattern TEXT NOT NULL,
			permissions TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (team_id) REFERENCES rbac_teams(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create rbac_roles table: %w", err)
	}

	// Measurement permissions table
	_, err = am.db.Exec(`
		CREATE TABLE IF NOT EXISTS rbac_measurement_permissions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			role_id INTEGER NOT NULL,
			measurement_pattern TEXT NOT NULL,
			permissions TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (role_id) REFERENCES rbac_roles(id) ON DELETE CASCADE
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create rbac_measurement_permissions table: %w", err)
	}

	// Token memberships table
	_, err = am.db.Exec(`
		CREATE TABLE IF NOT EXISTS rbac_token_memberships (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			token_id INTEGER NOT NULL,
			team_id INTEGER NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (token_id) REFERENCES api_tokens(id) ON DELETE CASCADE,
			FOREIGN KEY (team_id) REFERENCES rbac_teams(id) ON DELETE CASCADE,
			UNIQUE(token_id, team_id)
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create rbac_token_memberships table: %w", err)
	}

	// Create indexes for performance
	indexes := []string{
		`CREATE INDEX IF NOT EXISTS idx_rbac_teams_org ON rbac_teams(organization_id)`,
		`CREATE INDEX IF NOT EXISTS idx_rbac_roles_team ON rbac_roles(team_id)`,
		`CREATE INDEX IF NOT EXISTS idx_rbac_measurement_perms_role ON rbac_measurement_permissions(role_id)`,
		`CREATE INDEX IF NOT EXISTS idx_rbac_token_memberships_token ON rbac_token_memberships(token_id)`,
		`CREATE INDEX IF NOT EXISTS idx_rbac_token_memberships_team ON rbac_token_memberships(team_id)`,
	}

	for _, idx := range indexes {
		if _, err := am.db.Exec(idx); err != nil {
			am.logger.Debug().Err(err).Str("index", idx).Msg("Failed to create RBAC index")
		}
	}

	am.logger.Debug().Msg("RBAC tables initialized")
	return nil
}

// backfillTokenPrefixes populates token_prefix for tokens that don't have it
// This handles migration from older versions where token_prefix didn't exist
func (am *AuthManager) backfillTokenPrefixes() {
	// Check if there are any tokens without prefix
	var count int
	err := am.db.QueryRow(`SELECT COUNT(*) FROM api_tokens WHERE token_prefix IS NULL OR token_prefix = ''`).Scan(&count)
	if err != nil || count == 0 {
		return // No migration needed
	}

	am.logger.Info().Int("count", count).Msg("Backfilling token_prefix for existing tokens")

	// For existing tokens, we can't compute the prefix from the bcrypt hash
	// We'll need to mark them with a special value that forces full scan
	// When tokens are rotated or new tokens created, they'll get proper prefixes
	_, err = am.db.Exec(`UPDATE api_tokens SET token_prefix = '__legacy__' WHERE token_prefix IS NULL OR token_prefix = ''`)
	if err != nil {
		am.logger.Error().Err(err).Msg("Failed to backfill token_prefix")
	}
}

// cleanupLoop runs periodic cache cleanup
func (am *AuthManager) cleanupLoop() {
	interval := am.cacheTTL
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			am.cleanupExpiredCache()
		case <-am.cleanupDone:
			return
		}
	}
}

// cleanupExpiredCache removes expired entries from cache
func (am *AuthManager) cleanupExpiredCache() {
	now := time.Now()
	var expiredCount int

	am.cacheMu.Lock()
	for key, entry := range am.cache {
		if now.After(entry.expiresAt) {
			delete(am.cache, key)
			expiredCount++
		}
	}
	am.cacheMu.Unlock()

	if expiredCount > 0 {
		am.logger.Debug().Int("expired_count", expiredCount).Msg("Cleaned up expired cache entries")
	}
}

// hashToken generates a bcrypt hash of the token for storage
func (am *AuthManager) hashToken(token string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(token), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(hash), nil
}

// verifyTokenHash checks if a token matches a stored hash
func (am *AuthManager) verifyTokenHash(token, hash string) bool {
	// Bcrypt hash (secure, used for all new tokens since v26)
	if strings.HasPrefix(hash, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte(token)) == nil
	}
	// SHA256 hash (legacy compatibility for pre-v26 tokens)
	// New tokens always use bcrypt. This path only verifies existing old tokens.
	// #nosec G401 -- Legacy compatibility only, not used for new token storage
	h := sha256.Sum256([]byte(token))
	return hash == hex.EncodeToString(h[:])
}

// cacheKey generates a cache key for in-memory token lookup.
// This is NOT password storage - it's a fast cache index for O(1) lookups.
// The actual token is stored securely with bcrypt in the database.
// #nosec G401 -- Not password hashing, just cache key derivation for lookups
func cacheKey(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// tokenPrefix generates a prefix for fast database token lookup.
// This enables O(1) database lookup via index instead of O(n) full table scan.
// The actual token is stored securely with bcrypt - this is just a lookup index.
// #nosec G401 -- Not password hashing, just lookup index derivation
func tokenPrefix(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])[:16]
}

// generateToken creates a secure random token
func generateToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return base64.URLEncoding.EncodeToString(bytes), nil
}

// insertToken inserts a pre-hashed token into the database.
// It is the shared implementation used by CreateToken and CreateTokenWithValue.
// The permissions argument MUST already be resolved (via storePermissions) by
// the caller — insertToken persists it verbatim and does not re-default, so the
// sentinel is never double-resolved.
func (am *AuthManager) insertToken(hash, prefix, name, description, permissions string, expiresAt *time.Time) error {

	var expiresAtVal interface{}
	if expiresAt != nil {
		expiresAtVal = *expiresAt
	}

	_, err := am.db.Exec(`
		INSERT INTO api_tokens (name, token_hash, token_prefix, description, permissions, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, name, hash, prefix, description, permissions, expiresAtVal)

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return fmt.Errorf("token with name '%s' already exists", name)
		}
		return fmt.Errorf("failed to create token: %w", err)
	}

	return nil
}

// CreateToken creates a new API token.
//
// In cluster mode, this proposes a CreateToken command through Raft and
// waits up to proposeTimeout for the apply to land. The context bounds
// that wait so a client disconnect or higher-level timeout cancels the
// proposal cleanly. In OSS / standalone mode the context is currently
// unused (the SQLite write is a single statement) but the signature is
// kept consistent across both paths for callers (Gemini #451 round-3
// review).
func (am *AuthManager) CreateToken(ctx context.Context, name, description, permissions string, expiresAt *time.Time) (string, error) {
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %w", err)
	}

	hash, err := am.hashToken(token)
	if err != nil {
		return "", fmt.Errorf("failed to hash token: %w", err)
	}
	prefix := tokenPrefix(token)

	// Resolve permissions to the exact stored value once, here, so both the
	// cluster-propose path and the direct-SQLite insertToken path persist the
	// same string (insertToken does NOT re-resolve). "" => read,write default;
	// PermissionsNone => "" (deliberate RBAC-only).
	permissions = storePermissions(permissions)

	// Cluster mode: propose via Raft, which runs ApplyCreateToken on every
	// node (including this one) and writes to local SQLite via the apply
	// callback. The plaintext token value never lands in the Raft log —
	// only the bcrypt hash and prefix. Returned to the caller out of band.
	if am.getProposer() != nil {
		var expiresAtNano int64
		if expiresAt != nil {
			expiresAtNano = expiresAt.UnixNano()
		}
		// Use clusterTokenEntryWire mirroring fsm.go's TokenEntry. The
		// proposer wraps this payload struct in a raft.Command envelope.
		payload := struct {
			Token clusterTokenEntryWire `json:"token"`
		}{
			Token: clusterTokenEntryWire{
				Name:              name,
				Description:       description,
				Permissions:       permissions,
				TokenHash:         hash,
				TokenPrefix:       prefix,
				CreatedAtUnixNano: time.Now().UnixNano(),
				ExpiresAtUnixNano: expiresAtNano,
				Enabled:           true,
			},
		}
		if err := am.proposeCommand(ctx, ProposalCommandCreateToken, payload); err != nil {
			return "", fmt.Errorf("create token: %w", err)
		}
		am.logger.Info().
			Str("name", name).
			Str("permissions", permissions).
			Msg("Created API token via Raft")
		return token, nil
	}

	// Standalone / OSS: direct SQLite write, unchanged from pre-Phase-A
	// behaviour. ctx is accepted for signature consistency but unused on
	// this path — a single SQLite INSERT can't observe cancellation
	// without re-plumbing the entire database/sql layer.
	_ = ctx
	if err := am.insertToken(hash, prefix, name, description, permissions, expiresAt); err != nil {
		return "", err
	}

	am.logger.Info().
		Str("name", name).
		Str("permissions", permissions).
		Msg("Created API token")

	return token, nil
}

// VerifyToken verifies a token and returns token info if valid
func (am *AuthManager) VerifyToken(token string) *TokenInfo {
	if token == "" {
		return nil
	}

	key := cacheKey(token)
	now := time.Now()

	// Check cache first
	am.cacheMu.RLock()
	if entry, ok := am.cache[key]; ok && now.Before(entry.expiresAt) {
		am.cacheMu.RUnlock()
		am.cacheHits.Add(1)
		metrics.Get().IncAuthCacheHit()
		return entry.info
	}
	am.cacheMu.RUnlock()

	am.cacheMisses.Add(1)
	metrics.Get().IncAuthCacheMiss()

	// Generate prefix for optimized lookup
	prefix := tokenPrefix(token)

	// Query database using token_prefix index for O(1) lookup
	// Fallback to full scan for legacy tokens (prefix = '__legacy__')
	rows, err := am.db.Query(`
		SELECT id, name, token_hash, description, permissions, created_at, last_used_at, expires_at, enabled
		FROM api_tokens
		WHERE enabled = 1 AND (token_prefix = ? OR token_prefix = '__legacy__')
	`, prefix)
	if err != nil {
		am.logger.Error().Err(err).Msg("Failed to query tokens")
		return nil
	}
	defer rows.Close()

	// Find matching token by verifying hash
	// With prefix index, this typically checks only 1-2 tokens instead of all
	for rows.Next() {
		var (
			id          int64
			name        string
			tokenHash   string
			description sql.NullString
			permissions sql.NullString
			createdAt   time.Time
			lastUsedAt  sql.NullTime
			expiresAt   sql.NullTime
			enabled     bool
		)

		if err := rows.Scan(&id, &name, &tokenHash, &description, &permissions, &createdAt, &lastUsedAt, &expiresAt, &enabled); err != nil {
			am.logger.Error().Err(err).Msg("Failed to scan token row")
			continue
		}

		if !am.verifyTokenHash(token, tokenHash) {
			continue
		}

		// Check expiration
		if expiresAt.Valid && now.After(expiresAt.Time) {
			am.logger.Warn().Str("name", name).Msg("Token has expired")
			return nil
		}

		// Update last used timestamp (fire and forget)
		go func(tokenID int64) {
			_, err := am.db.Exec("UPDATE api_tokens SET last_used_at = ? WHERE id = ?", now, tokenID)
			if err != nil {
				am.logger.Error().Err(err).Int64("token_id", tokenID).Msg("Failed to update last_used_at")
			}
		}(id)

		// Build token info
		info := &TokenInfo{
			ID:          id,
			Name:        name,
			Description: description.String,
			Permissions: []string{}, // empty by default for RBAC-only tokens
			CreatedAt:   createdAt,
			Enabled:     enabled,
		}
		if permissions.Valid && permissions.String != "" {
			info.Permissions = strings.Split(permissions.String, ",")
		}
		if lastUsedAt.Valid {
			info.LastUsedAt = lastUsedAt.Time
		}
		if expiresAt.Valid {
			info.ExpiresAt = &expiresAt.Time
		}

		// Add to cache
		am.cacheMu.Lock()
		// Evict oldest if cache is full
		if len(am.cache) >= am.maxCacheSize {
			var oldestKey string
			var oldestTime time.Time
			for k, v := range am.cache {
				if oldestKey == "" || v.expiresAt.Before(oldestTime) {
					oldestKey = k
					oldestTime = v.expiresAt
				}
			}
			if oldestKey != "" {
				delete(am.cache, oldestKey)
				am.cacheEvictions.Add(1)
			}
		}
		am.cache[key] = cacheEntry{
			info:      info,
			expiresAt: now.Add(am.cacheTTL),
		}
		am.cacheMu.Unlock()

		return info
	}

	am.logger.Debug().Msg("Authentication failed: invalid token")
	return nil
}

// HasPermission checks if a token has a specific permission
func (am *AuthManager) HasPermission(info *TokenInfo, permission string) bool {
	if info == nil {
		return false
	}
	for _, p := range info.Permissions {
		if p == "admin" || p == permission {
			return true
		}
	}
	return false
}

// ListTokens returns all tokens (without revealing hashes)
func (am *AuthManager) ListTokens() ([]TokenInfo, error) {
	rows, err := am.db.Query(`
		SELECT id, name, description, permissions, created_at, last_used_at, expires_at, enabled
		FROM api_tokens
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokens []TokenInfo
	for rows.Next() {
		var (
			id          int64
			name        string
			description sql.NullString
			permissions sql.NullString
			createdAt   time.Time
			lastUsedAt  sql.NullTime
			expiresAt   sql.NullTime
			enabled     bool
		)

		if err := rows.Scan(&id, &name, &description, &permissions, &createdAt, &lastUsedAt, &expiresAt, &enabled); err != nil {
			return nil, err
		}

		info := TokenInfo{
			ID:          id,
			Name:        name,
			Description: description.String,
			Permissions: []string{}, // empty by default for RBAC-only tokens
			CreatedAt:   createdAt,
			Enabled:     enabled,
		}
		if permissions.Valid && permissions.String != "" {
			info.Permissions = strings.Split(permissions.String, ",")
		}
		if lastUsedAt.Valid {
			info.LastUsedAt = lastUsedAt.Time
		}
		if expiresAt.Valid {
			info.ExpiresAt = &expiresAt.Time
		}

		tokens = append(tokens, info)
	}

	return tokens, nil
}

// GetTokenByID returns token info by ID
func (am *AuthManager) GetTokenByID(id int64) (*TokenInfo, error) {
	var (
		name        string
		description sql.NullString
		permissions sql.NullString
		createdAt   time.Time
		lastUsedAt  sql.NullTime
		expiresAt   sql.NullTime
		enabled     bool
	)

	err := am.db.QueryRow(`
		SELECT name, description, permissions, created_at, last_used_at, expires_at, enabled
		FROM api_tokens WHERE id = ?
	`, id).Scan(&name, &description, &permissions, &createdAt, &lastUsedAt, &expiresAt, &enabled)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	info := &TokenInfo{
		ID:          id,
		Name:        name,
		Description: description.String,
		Permissions: []string{}, // empty by default for RBAC-only tokens
		CreatedAt:   createdAt,
		Enabled:     enabled,
	}
	if permissions.Valid && permissions.String != "" {
		info.Permissions = strings.Split(permissions.String, ",")
	}
	if lastUsedAt.Valid {
		info.LastUsedAt = lastUsedAt.Time
	}
	if expiresAt.Valid {
		info.ExpiresAt = &expiresAt.Time
	}

	return info, nil
}

// UpdateToken updates token metadata.
//
// The context bounds the cluster-mode Raft proposal wait. In OSS mode
// it's currently unused (single SQLite UPDATE statement). Gemini #451
// round-3 review.
func (am *AuthManager) UpdateToken(ctx context.Context, id int64, name, description, permissions *string, expiresAt *time.Time) error {
	// Cluster mode: propose via Raft with a ChangedFields list so the FSM
	// applier knows which fields are "leave alone" vs "set to empty".
	if am.getProposer() != nil {
		var changed []string
		payload := struct {
			ID                int64    `json:"id"`
			Name              string   `json:"name,omitempty"`
			Description       string   `json:"description,omitempty"`
			Permissions       string   `json:"permissions,omitempty"`
			ExpiresAtUnixNano int64    `json:"expires_at_unix_nano,omitempty"`
			ChangedFields     []string `json:"changed_fields"`
		}{ID: id}
		if name != nil {
			payload.Name = *name
			changed = append(changed, "name")
		}
		if description != nil {
			payload.Description = *description
			changed = append(changed, "description")
		}
		if permissions != nil {
			payload.Permissions = *permissions
			changed = append(changed, "permissions")
		}
		if expiresAt != nil {
			payload.ExpiresAtUnixNano = expiresAt.UnixNano()
			changed = append(changed, "expires_at")
		}
		if len(changed) == 0 {
			return nil
		}
		payload.ChangedFields = changed
		if err := am.proposeCommand(ctx, ProposalCommandUpdateToken, payload); err != nil {
			return fmt.Errorf("update token: %w", err)
		}
		return nil
	}
	_ = ctx

	// Standalone / OSS: direct SQLite.
	var updates []string
	var args []interface{}

	if name != nil {
		updates = append(updates, "name = ?")
		args = append(args, *name)
	}
	if description != nil {
		updates = append(updates, "description = ?")
		args = append(args, *description)
	}
	if permissions != nil {
		updates = append(updates, "permissions = ?")
		args = append(args, *permissions)
	}
	if expiresAt != nil {
		updates = append(updates, "expires_at = ?")
		args = append(args, *expiresAt)
	}

	if len(updates) == 0 {
		return nil
	}

	args = append(args, id)
	query := fmt.Sprintf("UPDATE api_tokens SET %s WHERE id = ?", strings.Join(updates, ", "))

	result, err := am.db.Exec(query, args...)
	if err != nil {
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("token not found")
	}

	am.InvalidateCache()
	return nil
}

// DeleteToken deletes a token by ID. The context bounds the cluster-mode
// Raft proposal wait. Gemini #451 round-3 review.
func (am *AuthManager) DeleteToken(ctx context.Context, id int64) error {
	if am.getProposer() != nil {
		payload := struct {
			ID int64 `json:"id"`
		}{ID: id}
		if err := am.proposeCommand(ctx, ProposalCommandDeleteToken, payload); err != nil {
			return fmt.Errorf("delete token: %w", err)
		}
		am.logger.Info().Int64("token_id", id).Msg("Deleted API token via Raft")
		return nil
	}
	_ = ctx

	result, err := am.db.Exec("DELETE FROM api_tokens WHERE id = ?", id)
	if err != nil {
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("token not found")
	}

	am.InvalidateCache()
	am.logger.Info().Int64("token_id", id).Msg("Deleted API token")
	return nil
}

// RevokeToken disables a token. The context bounds the cluster-mode
// Raft proposal wait. Gemini #451 round-3 review.
func (am *AuthManager) RevokeToken(ctx context.Context, id int64) error {
	if am.getProposer() != nil {
		payload := struct {
			ID int64 `json:"id"`
		}{ID: id}
		if err := am.proposeCommand(ctx, ProposalCommandRevokeToken, payload); err != nil {
			return fmt.Errorf("revoke token: %w", err)
		}
		am.logger.Info().Int64("token_id", id).Msg("Revoked API token via Raft")
		return nil
	}
	_ = ctx

	result, err := am.db.Exec("UPDATE api_tokens SET enabled = 0 WHERE id = ?", id)
	if err != nil {
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return errors.New("token not found")
	}

	am.InvalidateCache()
	am.logger.Info().Int64("token_id", id).Msg("Revoked API token")
	return nil
}

// RotateToken generates a new token value while keeping metadata. The
// context bounds the cluster-mode Raft proposal wait. Gemini #451
// round-3 review.
func (am *AuthManager) RotateToken(ctx context.Context, id int64) (string, error) {
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %w", err)
	}

	hash, err := am.hashToken(token)
	if err != nil {
		return "", fmt.Errorf("failed to hash token: %w", err)
	}

	// Generate new prefix for O(1) lookup optimization
	prefix := tokenPrefix(token)

	// Cluster mode: propose via Raft. The plaintext token is returned to
	// the caller out of band — only hash + prefix go through the log.
	if am.getProposer() != nil {
		payload := struct {
			ID        int64  `json:"id"`
			NewHash   string `json:"new_hash"`
			NewPrefix string `json:"new_prefix"`
		}{ID: id, NewHash: hash, NewPrefix: prefix}
		if err := am.proposeCommand(ctx, ProposalCommandRotateToken, payload); err != nil {
			return "", fmt.Errorf("rotate token: %w", err)
		}
		am.logger.Info().Int64("token_id", id).Msg("Rotated API token via Raft")
		return token, nil
	}
	_ = ctx

	result, err := am.db.Exec("UPDATE api_tokens SET token_hash = ?, token_prefix = ? WHERE id = ?", hash, prefix, id)
	if err != nil {
		return "", err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return "", errors.New("token not found")
	}

	am.InvalidateCache()
	am.logger.Info().Int64("token_id", id).Msg("Rotated API token")
	return token, nil
}

// ensureFirstToken is the shared implementation for EnsureInitialToken and EnsureInitialTokenWithValue.
// It uses INSERT OR IGNORE to atomically create a token only when none exist, avoiding a TOCTOU race.
// Returns the token value if a new token was created, or empty string if one already existed.
//
// In cluster mode this routes through Raft: every node calls ensureFirstToken
// with its own (random) tokenValue on boot, but only one Raft proposal can
// succeed cluster-wide. The FSM applier rejects subsequent CreateToken
// commands whose Name = "admin" with a "token name already exists" error;
// non-winning nodes get that back from proposeCommand and treat it as
// "already initialised" (empty string return — same shape as the SQLite
// path's "rows == 0" check). The plaintext is only known to the proposer
// and only the winning leader prints the banner.
func (am *AuthManager) ensureFirstToken(ctx context.Context, tokenValue, description string) (string, error) {
	hash, err := am.hashToken(tokenValue)
	if err != nil {
		return "", fmt.Errorf("failed to hash token: %w", err)
	}
	prefix := tokenPrefix(tokenValue)

	if am.getProposer() != nil {
		payload := struct {
			Token clusterTokenEntryWire `json:"token"`
		}{
			Token: clusterTokenEntryWire{
				Name:              "admin",
				Description:       description,
				Permissions:       "read,write,delete,admin",
				TokenHash:         hash,
				TokenPrefix:       prefix,
				CreatedAtUnixNano: time.Now().UnixNano(),
				Enabled:           true,
			},
		}
		// Cluster bootstrap can race a Raft leader-flap window: WaitForLeader
		// in main.go observed an election, but by the time we ship the
		// proposal the leader may have stepped down. A bounded retry with
		// exponential backoff for ErrLeaderUnknown lets us ride out a
		// re-election without surfacing a scary error to the operator.
		// "Already exists" is still an immediate empty-return (the leader
		// applied a peer's proposal before ours).
		//
		// The backoff respects ctx so a shutdown signal during a
		// pathological no-leader window doesn't block process exit (Gemini
		// #451 round-2 review).
		const maxAttempts = 4
		backoff := 250 * time.Millisecond
		var err error
		for attempt := 0; attempt < maxAttempts; attempt++ {
			err = am.proposeCommand(ctx, ProposalCommandCreateToken, payload)
			if err == nil {
				return tokenValue, nil
			}
			if strings.Contains(err.Error(), "already exists") {
				return "", nil
			}
			if !errors.Is(err, ErrLeaderUnknown) {
				break
			}
			if attempt == maxAttempts-1 {
				break // don't sleep after the last attempt
			}
			select {
			case <-time.After(backoff):
				backoff *= 2 // exponential: 250ms, 500ms, 1s
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return "", err
	}

	// Standalone / OSS: direct SQLite path.
	// Use a single atomic statement: INSERT only if the table is empty.
	// This eliminates the COUNT→INSERT race when multiple nodes start simultaneously.
	result, err := am.db.Exec(`
		INSERT OR IGNORE INTO api_tokens (name, token_hash, token_prefix, description, permissions)
		SELECT 'admin', ?, ?, ?, 'read,write,delete,admin'
		WHERE NOT EXISTS (SELECT 1 FROM api_tokens)
	`, hash, prefix, description)
	if err != nil {
		return "", fmt.Errorf("failed to create initial token: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return "", nil // Tokens already existed — no-op
	}

	return tokenValue, nil
}

// EnsureInitialToken creates an admin token if no tokens exist.
//
// The context bounds the cluster-mode retry loop (in OSS / no-proposer
// mode it's ignored, since the underlying SQLite INSERT is a single
// statement). Pass a shutdown-aware context if you want bootstrap to
// abort cleanly when the process is being torn down — relevant when
// the Raft cluster never achieves leader election (Gemini #451 review).
func (am *AuthManager) EnsureInitialToken(ctx context.Context) (string, error) {
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %w", err)
	}

	result, err := am.ensureFirstToken(ctx, token, "Initial admin token (auto-generated on first run)")
	if err != nil {
		return "", err
	}
	if result != "" {
		am.logger.Info().Msg("First run detected - created initial admin token")
	}
	return result, nil
}

// CreateTokenWithValue creates a new API token using a caller-provided token value instead of generating one.
// The value must be at least 32 characters long to ensure adequate entropy.
// The context bounds the cluster-mode Raft proposal wait. Gemini #451
// round-3 review.
func (am *AuthManager) CreateTokenWithValue(ctx context.Context, tokenValue, name, description, permissions string, expiresAt *time.Time) (string, error) {
	if len(tokenValue) < 32 {
		return "", fmt.Errorf("bootstrap token must be at least 32 characters long")
	}

	hash, err := am.hashToken(tokenValue)
	if err != nil {
		return "", fmt.Errorf("failed to hash token: %w", err)
	}
	prefix := tokenPrefix(tokenValue)
	// Resolve once (see CreateToken): "" => read,write; PermissionsNone => "".
	permissions = storePermissions(permissions)

	// Cluster mode: propose via Raft — same shape as CreateToken.
	if am.getProposer() != nil {
		var expiresAtNano int64
		if expiresAt != nil {
			expiresAtNano = expiresAt.UnixNano()
		}
		payload := struct {
			Token clusterTokenEntryWire `json:"token"`
		}{
			Token: clusterTokenEntryWire{
				Name:              name,
				Description:       description,
				Permissions:       permissions,
				TokenHash:         hash,
				TokenPrefix:       prefix,
				CreatedAtUnixNano: time.Now().UnixNano(),
				ExpiresAtUnixNano: expiresAtNano,
				Enabled:           true,
			},
		}
		if err := am.proposeCommand(ctx, ProposalCommandCreateToken, payload); err != nil {
			return "", fmt.Errorf("create token with value: %w", err)
		}
		am.logger.Info().
			Str("name", name).
			Str("permissions", permissions).
			Msg("Created API token with provided value via Raft")
		return tokenValue, nil
	}
	_ = ctx

	if err := am.insertToken(hash, prefix, name, description, permissions, expiresAt); err != nil {
		return "", err
	}

	am.logger.Info().
		Str("name", name).
		Str("permissions", permissions).
		Msg("Created API token with provided value")

	return tokenValue, nil
}

// EnsureInitialTokenWithValue creates the initial admin token using a caller-provided value.
// If tokens already exist, this is a no-op (returns empty string).
//
// The context bounds the cluster-mode retry loop. See EnsureInitialToken
// for the contract.
func (am *AuthManager) EnsureInitialTokenWithValue(ctx context.Context, tokenValue string) (string, error) {
	if len(tokenValue) < 32 {
		return "", fmt.Errorf("bootstrap token must be at least 32 characters long")
	}

	result, err := am.ensureFirstToken(ctx, tokenValue, "Initial admin token (set via ARC_AUTH_BOOTSTRAP_TOKEN)")
	if err != nil {
		return "", err
	}
	if result == "" {
		am.logger.Debug().Msg("ARC_AUTH_BOOTSTRAP_TOKEN set but tokens already exist — skipping (no-op)")
	} else {
		am.logger.Info().Msg("First run detected - created initial admin token from ARC_AUTH_BOOTSTRAP_TOKEN")
	}
	return result, nil
}

// ForceAddRecoveryToken adds a new admin token with the provided value without removing existing tokens.
// This is a recovery path for when the admin token has been lost. Requires ARC_AUTH_FORCE_BOOTSTRAP=true.
// Existing tokens are preserved so that legitimate admins can still revoke the recovery token if it was
// injected by a bad actor.
func (am *AuthManager) ForceAddRecoveryToken(ctx context.Context, tokenValue string) (string, error) {
	if len(tokenValue) < 32 {
		return "", fmt.Errorf("bootstrap token must be at least 32 characters long")
	}

	am.logger.Warn().Msg("ARC_AUTH_FORCE_BOOTSTRAP=true: adding recovery admin token (existing tokens preserved)")

	token, err := am.CreateTokenWithValue(ctx, tokenValue, "arc-recovery", "Recovery admin token added via ARC_AUTH_FORCE_BOOTSTRAP", "read,write,delete,admin", nil)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			// Recovery token already exists from a previous restart — no-op, the caller
			// still holds the token value they provided and can use it.
			am.logger.Warn().Msg("Recovery token already exists — no-op; use the API to revoke it after regaining access")
			return "", nil
		}
		return "", err
	}

	am.logger.Warn().Msg("Recovery admin token created; existing tokens untouched — use the API to revoke any unwanted tokens after regaining access")

	return token, nil
}

// InvalidateCache clears the token cache
func (am *AuthManager) InvalidateCache() {
	am.cacheMu.Lock()
	cleared := len(am.cache)
	am.cache = make(map[string]cacheEntry)
	am.cacheMu.Unlock()

	am.logger.Info().Int("cleared", cleared).Msg("Token cache invalidated")
}

// GetCacheStats returns cache statistics
func (am *AuthManager) GetCacheStats() map[string]interface{} {
	am.cacheMu.RLock()
	cacheSize := len(am.cache)
	am.cacheMu.RUnlock()

	hits := am.cacheHits.Load()
	misses := am.cacheMisses.Load()
	evictions := am.cacheEvictions.Load()
	total := hits + misses
	hitRate := float64(0)
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return map[string]interface{}{
		"cache_size":          cacheSize,
		"max_cache_size":      am.maxCacheSize,
		"cache_ttl_seconds":   am.cacheTTL.Seconds(),
		"utilization_percent": float64(cacheSize) / float64(am.maxCacheSize) * 100,
		"total_requests":      total,
		"cache_hits":          hits,
		"cache_misses":        misses,
		"cache_evictions":     evictions,
		"hit_rate_percent":    hitRate,
	}
}

// Close shuts down the auth manager
func (am *AuthManager) Close() error {
	close(am.cleanupDone)
	return am.db.Close()
}

// GetDB returns the underlying database connection for use by RBACManager
func (am *AuthManager) GetDB() *sql.DB {
	return am.db
}
