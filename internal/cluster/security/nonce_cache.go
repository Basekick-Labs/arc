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
//
// ttl is derived from the HMAC tolerance by NewNonceCache and is longer than
// it; see that function for the arithmetic.
type NonceCache struct {
	mu      sync.Mutex
	entries map[string]int64 // key: "nodeID:nonce", value: expiry unix nanos
	ttl     time.Duration

	// lastEvict is the wall-clock time of the last full sweep. Track()
	// runs evictExpiredLocked when more than nonceCacheEvictInterval
	// has passed since this value.
	lastEvict time.Time
}

// NewNonceCache creates a cache that rejects duplicate nonces for as long as
// a MAC bearing that nonce could still be accepted.
//
// Pass the HMAC timestamp TOLERANCE, not a TTL — the cache derives its own
// lifetime, which is deliberately longer. Using the tolerance directly as the
// TTL leaves a replay window, because the two clocks are different:
//
//   - The validator compares the SENDER's timestamp against its own clock
//     truncated to seconds: accept iff |floor(receivedAt) - ts| <= T. So the
//     last instant a replay is still accepted is ts + T + 1s (exclusive).
//   - The cache expires an entry at RECEIPT time + ttl, at nanosecond
//     precision. The earliest a first receipt can be accepted is ts - T.
//
// Covering every accepted replay therefore needs (ts - T) + ttl >= ts + T + 1s,
// i.e. ttl >= 2T + 1s. With ttl = T a message from a peer whose clock runs
// ahead is evicted from the cache while its MAC is still fresh; with ttl = 2T
// a peer running (T - 1s) ahead still leaves a sub-second window. Hence the
// +1s, which also absorbs the validator's second-truncation.
func NewNonceCache(tolerance time.Duration) *NonceCache {
	return &NonceCache{
		entries:   make(map[string]int64),
		ttl:       2*tolerance + time.Second,
		lastEvict: time.Now(),
	}
}

// Track records a nonce and returns true if it's new (not a replay).
// Returns false if the same (nodeID, nonce) pair was already seen within
// the TTL window — the caller should reject the request as a replay.
func (nc *NonceCache) Track(nodeID, nonce string) bool {
	// A nil *NonceCache stored in a ReplayGuard interface is NOT == nil at
	// the interface level (Go's typed-nil trap), so a caller's `guard == nil`
	// check does not catch it. Report "not new" rather than panicking: every
	// caller treats false as "reject", so an absent cache fails closed.
	if nc == nil {
		return false
	}

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
