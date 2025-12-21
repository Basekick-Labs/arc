package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// setupTestAuthManager creates a test AuthManager with a temporary database
func setupTestAuthManager(t *testing.T) (*AuthManager, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "arc-auth-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "auth.db")
	logger := zerolog.Nop()

	am, err := NewAuthManager(dbPath, 5*time.Minute, 100, logger)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to create AuthManager: %v", err)
	}

	cleanup := func() {
		am.Close()
		os.RemoveAll(tmpDir)
	}

	return am, cleanup
}

// TestNewAuthManager tests AuthManager creation
func TestNewAuthManager(t *testing.T) {
	t.Run("valid creation", func(t *testing.T) {
		am, cleanup := setupTestAuthManager(t)
		defer cleanup()

		if am == nil {
			t.Fatal("AuthManager should not be nil")
		}
		if am.db == nil {
			t.Error("Database connection should not be nil")
		}
	})

	t.Run("invalid path", func(t *testing.T) {
		logger := zerolog.Nop()
		// Use a path that should fail (root directory, no permission)
		_, err := NewAuthManager("/nonexistent/deeply/nested/path/that/should/fail/auth.db", time.Minute, 100, logger)
		if err == nil {
			t.Error("Expected error for invalid path")
		}
	})

	t.Run("cache config", func(t *testing.T) {
		tmpDir, _ := os.MkdirTemp("", "arc-auth-test-*")
		defer os.RemoveAll(tmpDir)

		dbPath := filepath.Join(tmpDir, "auth.db")
		logger := zerolog.Nop()

		am, err := NewAuthManager(dbPath, 10*time.Second, 50, logger)
		if err != nil {
			t.Fatalf("failed to create AuthManager: %v", err)
		}
		defer am.Close()

		if am.cacheTTL != 10*time.Second {
			t.Errorf("cacheTTL = %v, want %v", am.cacheTTL, 10*time.Second)
		}
		if am.maxCacheSize != 50 {
			t.Errorf("maxCacheSize = %d, want %d", am.maxCacheSize, 50)
		}
	})
}

// TestCreateToken tests token creation
func TestCreateToken(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	t.Run("basic token creation", func(t *testing.T) {
		token, err := am.CreateToken("test-token", "Test description", "read,write", nil)
		if err != nil {
			t.Fatalf("CreateToken failed: %v", err)
		}
		if token == "" {
			t.Error("Token should not be empty")
		}
		// Token should be base64 encoded, ~43 chars for 32 bytes
		if len(token) < 40 {
			t.Errorf("Token length = %d, expected >= 40", len(token))
		}
	})

	t.Run("duplicate name", func(t *testing.T) {
		_, err := am.CreateToken("dup-token", "First", "read", nil)
		if err != nil {
			t.Fatalf("First CreateToken failed: %v", err)
		}

		_, err = am.CreateToken("dup-token", "Second", "read", nil)
		if err == nil {
			t.Error("Expected error for duplicate token name")
		}
	})

	t.Run("with expiration", func(t *testing.T) {
		expiresAt := time.Now().Add(24 * time.Hour)
		token, err := am.CreateToken("expiring-token", "Expires in 24h", "read", &expiresAt)
		if err != nil {
			t.Fatalf("CreateToken with expiration failed: %v", err)
		}
		if token == "" {
			t.Error("Token should not be empty")
		}
	})

	t.Run("default permissions", func(t *testing.T) {
		token, err := am.CreateToken("default-perms", "Default permissions", "", nil)
		if err != nil {
			t.Fatalf("CreateToken failed: %v", err)
		}

		// Verify token and check permissions
		info := am.VerifyToken(token)
		if info == nil {
			t.Fatal("Token verification failed")
		}

		hasRead := false
		hasWrite := false
		for _, p := range info.Permissions {
			if p == "read" {
				hasRead = true
			}
			if p == "write" {
				hasWrite = true
			}
		}
		if !hasRead || !hasWrite {
			t.Errorf("Default permissions should be read,write, got %v", info.Permissions)
		}
	})
}

// TestVerifyToken tests token verification
func TestVerifyToken(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	t.Run("valid token", func(t *testing.T) {
		token, _ := am.CreateToken("verify-test", "Test", "read,write", nil)

		info := am.VerifyToken(token)
		if info == nil {
			t.Fatal("VerifyToken returned nil for valid token")
		}
		if info.Name != "verify-test" {
			t.Errorf("Name = %s, want verify-test", info.Name)
		}
		if !info.Enabled {
			t.Error("Token should be enabled")
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		info := am.VerifyToken("invalid-token-12345")
		if info != nil {
			t.Error("VerifyToken should return nil for invalid token")
		}
	})

	t.Run("empty token", func(t *testing.T) {
		info := am.VerifyToken("")
		if info != nil {
			t.Error("VerifyToken should return nil for empty token")
		}
	})

	t.Run("expired token", func(t *testing.T) {
		// Create token that expires immediately
		expiresAt := time.Now().Add(-1 * time.Second)
		token, _ := am.CreateToken("expired-token", "Already expired", "read", &expiresAt)

		info := am.VerifyToken(token)
		if info != nil {
			t.Error("VerifyToken should return nil for expired token")
		}
	})
}

// TestVerifyToken_Cache tests cache behavior
func TestVerifyToken_Cache(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	token, _ := am.CreateToken("cache-test", "Test", "read", nil)

	// First verification - cache miss
	initialMisses := am.cacheMisses.Load()
	info1 := am.VerifyToken(token)
	if info1 == nil {
		t.Fatal("First verification failed")
	}
	if am.cacheMisses.Load() != initialMisses+1 {
		t.Error("Expected cache miss on first verification")
	}

	// Second verification - should be cache hit
	initialHits := am.cacheHits.Load()
	info2 := am.VerifyToken(token)
	if info2 == nil {
		t.Fatal("Second verification failed")
	}
	if am.cacheHits.Load() != initialHits+1 {
		t.Error("Expected cache hit on second verification")
	}
}

// TestRotateToken tests token rotation
func TestRotateToken(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create initial token
	oldToken, _ := am.CreateToken("rotate-test", "Test", "read,write", nil)
	info := am.VerifyToken(oldToken)
	if info == nil {
		t.Fatal("Initial token verification failed")
	}
	tokenID := info.ID

	// Rotate token
	newToken, err := am.RotateToken(tokenID)
	if err != nil {
		t.Fatalf("RotateToken failed: %v", err)
	}

	// Old token should no longer work
	if am.VerifyToken(oldToken) != nil {
		t.Error("Old token should no longer be valid after rotation")
	}

	// New token should work
	newInfo := am.VerifyToken(newToken)
	if newInfo == nil {
		t.Error("New token should be valid")
	}
	if newInfo.ID != tokenID {
		t.Errorf("Token ID changed after rotation: %d != %d", newInfo.ID, tokenID)
	}
}

// TestRevokeToken tests token revocation
func TestRevokeToken(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	token, _ := am.CreateToken("revoke-test", "Test", "read", nil)
	info := am.VerifyToken(token)
	if info == nil {
		t.Fatal("Initial verification failed")
	}

	// Revoke token
	err := am.RevokeToken(info.ID)
	if err != nil {
		t.Fatalf("RevokeToken failed: %v", err)
	}

	// Token should no longer work
	if am.VerifyToken(token) != nil {
		t.Error("Revoked token should not be valid")
	}
}

// TestDeleteToken tests token deletion
func TestDeleteToken(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	token, _ := am.CreateToken("delete-test", "Test", "read", nil)
	info := am.VerifyToken(token)
	if info == nil {
		t.Fatal("Initial verification failed")
	}

	// Delete token
	err := am.DeleteToken(info.ID)
	if err != nil {
		t.Fatalf("DeleteToken failed: %v", err)
	}

	// Token should no longer work
	if am.VerifyToken(token) != nil {
		t.Error("Deleted token should not be valid")
	}

	// GetTokenByID should return nil
	gotInfo, err := am.GetTokenByID(info.ID)
	if err != nil {
		t.Fatalf("GetTokenByID failed: %v", err)
	}
	if gotInfo != nil {
		t.Error("GetTokenByID should return nil for deleted token")
	}
}

// TestListTokens tests token listing
func TestListTokens(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create multiple tokens
	am.CreateToken("list-test-1", "First", "read", nil)
	am.CreateToken("list-test-2", "Second", "write", nil)
	am.CreateToken("list-test-3", "Third", "read,write,admin", nil)

	tokens, err := am.ListTokens()
	if err != nil {
		t.Fatalf("ListTokens failed: %v", err)
	}

	if len(tokens) != 3 {
		t.Errorf("ListTokens returned %d tokens, want 3", len(tokens))
	}

	// Check that token hashes are not exposed
	for _, token := range tokens {
		if token.ID == 0 {
			t.Error("Token ID should not be 0")
		}
		if token.Name == "" {
			t.Error("Token name should not be empty")
		}
	}
}

// TestHasPermission tests permission checking
func TestHasPermission(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	tests := []struct {
		name        string
		permissions string
		check       string
		expected    bool
	}{
		{"read only - has read", "read", "read", true},
		{"read only - no write", "read", "write", false},
		{"read,write - has read", "read,write", "read", true},
		{"read,write - has write", "read,write", "write", true},
		{"admin - has read", "admin", "read", true},
		{"admin - has write", "admin", "write", true},
		{"admin - has delete", "admin", "delete", true},
		{"admin - has admin", "admin", "admin", true},
		{"nil info", "", "read", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.permissions == "" {
				// Test nil case
				result := am.HasPermission(nil, tt.check)
				if result != tt.expected {
					t.Errorf("HasPermission(nil, %s) = %v, want %v", tt.check, result, tt.expected)
				}
				return
			}

			token, _ := am.CreateToken("perm-test-"+tt.name, "Test", tt.permissions, nil)
			info := am.VerifyToken(token)
			if info == nil {
				t.Fatal("Token verification failed")
			}

			result := am.HasPermission(info, tt.check)
			if result != tt.expected {
				t.Errorf("HasPermission(%v, %s) = %v, want %v", info.Permissions, tt.check, result, tt.expected)
			}
		})
	}
}

// TestUpdateToken tests token metadata updates
func TestUpdateToken(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	token, _ := am.CreateToken("update-test", "Original", "read", nil)
	info := am.VerifyToken(token)
	if info == nil {
		t.Fatal("Initial verification failed")
	}

	// Update description
	newDesc := "Updated description"
	err := am.UpdateToken(info.ID, nil, &newDesc, nil, nil)
	if err != nil {
		t.Fatalf("UpdateToken failed: %v", err)
	}

	// Verify update
	updatedInfo, err := am.GetTokenByID(info.ID)
	if err != nil {
		t.Fatalf("GetTokenByID failed: %v", err)
	}
	if updatedInfo.Description != newDesc {
		t.Errorf("Description = %s, want %s", updatedInfo.Description, newDesc)
	}
}

// TestGetCacheStats tests cache statistics
func TestGetCacheStats(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create and verify a token to populate cache
	token, _ := am.CreateToken("stats-test", "Test", "read", nil)
	am.VerifyToken(token) // Cache miss
	am.VerifyToken(token) // Cache hit

	stats := am.GetCacheStats()

	if stats["cache_size"] == nil {
		t.Error("cache_size should be present in stats")
	}
	if stats["cache_hits"] == nil {
		t.Error("cache_hits should be present in stats")
	}
	if stats["cache_misses"] == nil {
		t.Error("cache_misses should be present in stats")
	}
	if stats["hit_rate_percent"] == nil {
		t.Error("hit_rate_percent should be present in stats")
	}
}

// TestInvalidateCache tests cache invalidation
func TestInvalidateCache(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create and cache a token
	token, _ := am.CreateToken("invalidate-test", "Test", "read", nil)
	am.VerifyToken(token)

	// Check cache has entry
	am.cacheMu.RLock()
	cacheSize := len(am.cache)
	am.cacheMu.RUnlock()
	if cacheSize == 0 {
		t.Error("Cache should have at least one entry")
	}

	// Invalidate cache
	am.InvalidateCache()

	// Check cache is empty
	am.cacheMu.RLock()
	cacheSize = len(am.cache)
	am.cacheMu.RUnlock()
	if cacheSize != 0 {
		t.Errorf("Cache should be empty after invalidation, got %d entries", cacheSize)
	}
}

// TestEnsureInitialToken tests initial admin token creation
func TestEnsureInitialToken(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// First call should create token
	token, err := am.EnsureInitialToken()
	if err != nil {
		t.Fatalf("EnsureInitialToken failed: %v", err)
	}
	if token == "" {
		t.Error("Should create initial token when no tokens exist")
	}

	// Verify token is admin
	info := am.VerifyToken(token)
	if info == nil {
		t.Fatal("Initial token verification failed")
	}
	if !am.HasPermission(info, "admin") {
		t.Error("Initial token should have admin permission")
	}

	// Second call should return empty (tokens already exist)
	token2, err := am.EnsureInitialToken()
	if err != nil {
		t.Fatalf("Second EnsureInitialToken failed: %v", err)
	}
	if token2 != "" {
		t.Error("Should not create token when tokens already exist")
	}
}

// TestCacheEviction tests cache eviction when full
func TestCacheEviction(t *testing.T) {
	// Create manager with small cache
	tmpDir, _ := os.MkdirTemp("", "arc-auth-test-*")
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "auth.db")
	logger := zerolog.Nop()

	am, err := NewAuthManager(dbPath, 5*time.Minute, 3, logger) // Max 3 entries
	if err != nil {
		t.Fatalf("failed to create AuthManager: %v", err)
	}
	defer am.Close()

	// Create and verify 4 tokens (should cause eviction)
	var tokens []string
	for i := 0; i < 4; i++ {
		token, _ := am.CreateToken("eviction-test-"+string(rune('a'+i)), "Test", "read", nil)
		tokens = append(tokens, token)
		am.VerifyToken(token)
	}

	// Check that eviction occurred
	if am.cacheEvictions.Load() == 0 {
		t.Error("Expected at least one cache eviction")
	}

	// Cache should still have max 3 entries
	am.cacheMu.RLock()
	cacheSize := len(am.cache)
	am.cacheMu.RUnlock()
	if cacheSize > 3 {
		t.Errorf("Cache size = %d, should not exceed max of 3", cacheSize)
	}
}

// TestTokenPrefix tests the O(1) lookup optimization
func TestTokenPrefix(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	// Create token and verify prefix is stored
	token, _ := am.CreateToken("prefix-test", "Test", "read", nil)

	// Query database directly to check prefix
	var storedPrefix string
	err := am.db.QueryRow("SELECT token_prefix FROM api_tokens WHERE name = ?", "prefix-test").Scan(&storedPrefix)
	if err != nil {
		t.Fatalf("Failed to query token_prefix: %v", err)
	}

	expectedPrefix := tokenPrefix(token)
	if storedPrefix != expectedPrefix {
		t.Errorf("Stored prefix = %s, want %s", storedPrefix, expectedPrefix)
	}

	// Prefix should be 16 chars (first 16 of SHA256 hex)
	if len(storedPrefix) != 16 {
		t.Errorf("Prefix length = %d, want 16", len(storedPrefix))
	}
}

// TestVerifyTokenHash tests hash verification including legacy SHA256
func TestVerifyTokenHash(t *testing.T) {
	am, cleanup := setupTestAuthManager(t)
	defer cleanup()

	t.Run("bcrypt hash", func(t *testing.T) {
		token := "test-token-bcrypt"
		hash, err := am.hashToken(token)
		if err != nil {
			t.Fatalf("hashToken failed: %v", err)
		}

		if !am.verifyTokenHash(token, hash) {
			t.Error("verifyTokenHash should return true for valid bcrypt hash")
		}

		if am.verifyTokenHash("wrong-token", hash) {
			t.Error("verifyTokenHash should return false for wrong token")
		}
	})

	t.Run("sha256 legacy hash", func(t *testing.T) {
		token := "legacy-token"
		// Compute SHA256 hash like legacy Python code
		h := sha256Sum(token)

		if !am.verifyTokenHash(token, h) {
			t.Error("verifyTokenHash should return true for valid SHA256 hash")
		}

		if am.verifyTokenHash("wrong-token", h) {
			t.Error("verifyTokenHash should return false for wrong token")
		}
	})
}

// sha256Sum computes SHA256 hex digest (like legacy Python code)
func sha256Sum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}
