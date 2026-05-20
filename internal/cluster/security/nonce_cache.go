package security

// NonceCache provides replay protection for HMAC-authenticated messages.
// It tracks (nodeID:nonce) pairs and rejects duplicates within a configurable
// time window. The cache uses lazy eviction driven by Track calls — there
// is no background goroutine, so expired entries linger until the next Track.
// Memory is bounded by peak nonce-rate within the prior TTL window.

import (
	"sync"
	"time"
)

// nonceCacheEvictInterval bounds how often lazy eviction runs from
// inside Track(). The cache walks all entries under the mutex during
// eviction; running on every call would be expensive under load, so
// we amortize over this window. Eviction is *only* driven by Track
// calls — if no traffic arrives, expired entries linger until the
// next Track (memory cost is bounded by peak nonce-rate within the
// prior TTL window, so it bleeds off naturally on the next burst).
const nonceCacheEvictInterval = 60 * time.Second

// NonceCache tracks recently seen nonces to prevent replay attacks.
// Safe for concurrent use from multiple goroutines.
type NonceCache struct {
	mu      sync.Mutex
	entries map[string]int64 // key: "nodeID:nonce", value: expiry unix nanos
	ttl     time.Duration

	// lastEvict is the wall-clock time of the last full sweep. Track()
	// runs evictExpiredLocked when more than nonceCacheEvictInterval
	// has passed since this value.
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
	key := nodeID + "\x00" + nonce
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

	// Lazy eviction: clean up expired entries at most every
	// nonceCacheEvictInterval (gated to keep Track's hot path O(1)
	// amortized; the occasional sweep is O(N) under the mutex).
	if now.Sub(nc.lastEvict) > nonceCacheEvictInterval {
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
