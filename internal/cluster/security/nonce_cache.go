package security

// NonceCache provides replay protection for HMAC-authenticated messages.
// It tracks (nodeID:nonce) pairs and rejects duplicates within a configurable
// time window. The cache uses lazy eviction on Track calls — expired entries
// are cleaned up periodically without a background goroutine.

import (
	"sync"
	"time"
)

// NonceCache tracks recently seen nonces to prevent replay attacks.
// Safe for concurrent use from multiple goroutines.
type NonceCache struct {
	mu      sync.Mutex
	entries map[string]int64 // key: "nodeID:nonce", value: expiry unix nanos
	ttl     time.Duration

	// evictInterval controls how often lazy eviction runs. We don't evict
	// on every Track call (too expensive under load) — only when this
	// many seconds have passed since the last eviction.
	lastEvict time.Time
}

// NewNonceCache creates a cache that rejects duplicate nonces within the
// given TTL window. The TTL should match the HMAC timestamp tolerance
// (typically 5 minutes).
func NewNonceCache(ttl time.Duration) *NonceCache {
	return &NonceCache{
		entries:   make(map[string]int64),
		ttl:       ttl,
		lastEvict: time.Now(),
	}
}

// Track records a nonce and returns true if it's new (not a replay).
// Returns false if the same (nodeID, nonce) pair was already seen within
// the TTL window — the caller should reject the request as a replay.
func (nc *NonceCache) Track(nodeID, nonce string) bool {
	key := nodeID + ":" + nonce
	now := time.Now()
	expiry := now.Add(nc.ttl).UnixNano()

	nc.mu.Lock()
	defer nc.mu.Unlock()

	// Check for duplicate.
	if existingExpiry, seen := nc.entries[key]; seen {
		if now.UnixNano() < existingExpiry {
			return false // replay within TTL window
		}
		// Expired entry — allow reuse (theoretically a nonce could be
		// regenerated after TTL, though with 32 random bytes this is
		// astronomically unlikely).
	}

	nc.entries[key] = expiry

	// Lazy eviction: clean up expired entries every 60 seconds.
	if now.Sub(nc.lastEvict) > 60*time.Second {
		nc.evictExpiredLocked(now)
		nc.lastEvict = now
	}

	return true
}

// Len returns the number of tracked nonces (including potentially expired
// ones that haven't been evicted yet). Useful for tests and metrics.
func (nc *NonceCache) Len() int {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	return len(nc.entries)
}

// evictExpiredLocked removes entries whose expiry has passed. Must be
// called with nc.mu held.
func (nc *NonceCache) evictExpiredLocked(now time.Time) {
	nowNanos := now.UnixNano()
	for key, expiry := range nc.entries {
		if nowNanos >= expiry {
			delete(nc.entries, key)
		}
	}
}
