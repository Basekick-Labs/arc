package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/rs/zerolog"
	"golang.org/x/crypto/bcrypt"

	_ "github.com/mattn/go-sqlite3"
)

// TokenInfo represents token metadata returned by verify
type TokenInfo struct {
	ID          int64     `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Permissions []string  `json:"permissions"`
	CreatedAt   time.Time `json:"created_at"`
	LastUsedAt  time.Time `json:"last_used_at,omitempty"`
	Enabled     bool      `json:"enabled"`
	ExpiresAt   time.Time `json:"expires_at,omitempty"`
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
}

// NewAuthManager creates a new authentication manager
func NewAuthManager(dbPath string, cacheTTL time.Duration, maxCacheSize int, logger zerolog.Logger) (*AuthManager, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("failed to open auth database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(1) // SQLite only supports one writer
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

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

	// Start background cleanup
	go am.cleanupLoop()

	am.logger.Info().
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
	// Bcrypt hash
	if strings.HasPrefix(hash, "$2") {
		return bcrypt.CompareHashAndPassword([]byte(hash), []byte(token)) == nil
	}
	// SHA256 hash (legacy compatibility)
	h := sha256.Sum256([]byte(token))
	return hash == hex.EncodeToString(h[:])
}

// cacheKey generates a cache key from a token (using SHA256 for fast lookup)
func cacheKey(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// tokenPrefix generates a prefix for fast token lookup (first 16 chars of SHA256)
// This enables O(1) database lookup instead of O(n) full table scan
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

// CreateToken creates a new API token
func (am *AuthManager) CreateToken(name, description, permissions string, expiresAt *time.Time) (string, error) {
	token, err := generateToken()
	if err != nil {
		return "", fmt.Errorf("failed to generate token: %w", err)
	}

	hash, err := am.hashToken(token)
	if err != nil {
		return "", fmt.Errorf("failed to hash token: %w", err)
	}

	// Generate prefix for O(1) lookup optimization
	prefix := tokenPrefix(token)

	if permissions == "" {
		permissions = "read,write"
	}

	var expiresAtVal interface{}
	if expiresAt != nil {
		expiresAtVal = *expiresAt
	}

	_, err = am.db.Exec(`
		INSERT INTO api_tokens (name, token_hash, token_prefix, description, permissions, expires_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, name, hash, prefix, description, permissions, expiresAtVal)

	if err != nil {
		if strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return "", fmt.Errorf("token with name '%s' already exists", name)
		}
		return "", fmt.Errorf("failed to create token: %w", err)
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
			Permissions: strings.Split(permissions.String, ","),
			CreatedAt:   createdAt,
			Enabled:     enabled,
		}
		if lastUsedAt.Valid {
			info.LastUsedAt = lastUsedAt.Time
		}
		if expiresAt.Valid {
			info.ExpiresAt = expiresAt.Time
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
			Permissions: []string{"read", "write"}, // default
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
			info.ExpiresAt = expiresAt.Time
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
		Permissions: []string{"read", "write"},
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
		info.ExpiresAt = expiresAt.Time
	}

	return info, nil
}

// UpdateToken updates token metadata
func (am *AuthManager) UpdateToken(id int64, name, description, permissions *string, expiresAt *time.Time) error {
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

// DeleteToken deletes a token by ID
func (am *AuthManager) DeleteToken(id int64) error {
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

// RevokeToken disables a token
func (am *AuthManager) RevokeToken(id int64) error {
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

// RotateToken generates a new token value while keeping metadata
func (am *AuthManager) RotateToken(id int64) (string, error) {
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

// EnsureInitialToken creates an admin token if no tokens exist
func (am *AuthManager) EnsureInitialToken() (string, error) {
	var count int
	err := am.db.QueryRow("SELECT COUNT(*) FROM api_tokens").Scan(&count)
	if err != nil {
		return "", err
	}

	if count > 0 {
		return "", nil // Tokens already exist
	}

	am.logger.Info().Msg("First run detected - creating initial admin token")

	token, err := am.CreateToken("admin", "Initial admin token (auto-generated on first run)", "read,write,delete,admin", nil)
	if err != nil {
		// Race condition - another process created it
		if strings.Contains(err.Error(), "already exists") {
			return "", nil
		}
		return "", err
	}

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
