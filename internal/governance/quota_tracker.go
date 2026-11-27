package governance

import (
	"sync"
	"time"
)

// quotaTracker tracks hourly and daily query counts per token with automatic reset.
type quotaTracker struct {
	mu              sync.Mutex
	queriesThisHour int
	queriesThisDay  int
	hourResetAt     time.Time
	dayResetAt      time.Time
	maxPerHour      int
	maxPerDay       int
	now             func() time.Time
	// appliedHour, appliedDay: the limits Manager last set, under
	// Manager.admitMu (see slidingWindowCounter.applied).
	appliedHour, appliedDay int
}

func newQuotaTracker(maxPerHour, maxPerDay int) *quotaTracker {
	return newQuotaTrackerWithClock(maxPerHour, maxPerDay, time.Now)
}

// newQuotaTrackerWithClock is newQuotaTracker reading the clock now.
func newQuotaTrackerWithClock(maxPerHour, maxPerDay int, clock func() time.Time) *quotaTracker {
	now := clock()
	return &quotaTracker{
		maxPerHour:  maxPerHour,
		maxPerDay:   maxPerDay,
		hourResetAt: now.Truncate(time.Hour).Add(time.Hour),
		dayResetAt:  now.Truncate(24 * time.Hour).Add(24 * time.Hour),
		now:         clock,
	}
}

// maybeReset resets counters if the hour or day boundary has passed.
// Must be called with mu held.
func (q *quotaTracker) maybeReset() {
	now := q.now()
	if !now.Before(q.hourResetAt) {
		q.queriesThisHour = 0
		q.hourResetAt = now.Truncate(time.Hour).Add(time.Hour)
	}
	if !now.Before(q.dayResetAt) {
		q.queriesThisDay = 0
		q.dayResetAt = now.Truncate(24 * time.Hour).Add(24 * time.Hour)
	}
}

// AllowQuery checks if a query is allowed under the hourly/daily quotas.
// Test helper: the query path uses Manager.Admit, which checks every limit
// before recording on any (#1524).
// If allowed, it increments the counters.
// Returns (allowed, reason) where reason describes why the query was denied.
func (q *quotaTracker) AllowQuery() (bool, string) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if ok, reason := q.admitsLocked(); !ok {
		return false, reason
	}
	q.recordLocked()
	return true, ""
}

// admits reports whether a query would pass the quotas now, without counting
// it (#1524), and why not.
func (q *quotaTracker) admits() (bool, string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.admitsLocked()
}

// record counts a query that every limit admitted.
func (q *quotaTracker) record() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.recordLocked()
}

func (q *quotaTracker) admitsLocked() (bool, string) {
	q.maybeReset()
	// Daily first: when both are exhausted the daily reset is the one a retry
	// waits for (RetryAfterSec), so the reason names it.
	if q.maxPerDay > 0 && q.queriesThisDay >= q.maxPerDay {
		return false, "Daily query quota exceeded"
	}
	if q.maxPerHour > 0 && q.queriesThisHour >= q.maxPerHour {
		return false, "Hourly query quota exceeded"
	}
	return true, ""
}

func (q *quotaTracker) recordLocked() {
	q.maybeReset()
	q.queriesThisHour++
	q.queriesThisDay++
}

// RetryAfterSec returns the number of seconds until a query can pass every quota
// it has exhausted. Returns 0 if neither is exhausted.
func (q *quotaTracker) RetryAfterSec() int {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.maybeReset()

	// Both reset times are in the future here, and the next day boundary is
	// never before the next hour boundary, so the daily quota decides when
	// both are exhausted.
	var resetAt time.Time
	if q.maxPerHour > 0 && q.queriesThisHour >= q.maxPerHour {
		resetAt = q.hourResetAt
	}
	if q.maxPerDay > 0 && q.queriesThisDay >= q.maxPerDay {
		resetAt = q.dayResetAt
	}
	if resetAt.IsZero() {
		return 0
	}
	return retryAfterSeconds(resetAt.Sub(q.now()))
}

// GetUsage returns the current usage and reset times.
func (q *quotaTracker) GetUsage() (hourUsed, dayUsed int, hourReset, dayReset time.Time) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.maybeReset()
	return q.queriesThisHour, q.queriesThisDay, q.hourResetAt, q.dayResetAt
}

// applyLimits makes these the tracker's limits, if they are not already.
// Called with Manager.admitMu held. The counts are kept.
func (q *quotaTracker) applyLimits(maxPerHour, maxPerDay int) {
	if q.appliedHour != maxPerHour || q.appliedDay != maxPerDay {
		q.UpdateLimits(maxPerHour, maxPerDay)
		q.appliedHour, q.appliedDay = maxPerHour, maxPerDay
	}
}

// UpdateLimits changes the quota limits. Takes effect immediately.
func (q *quotaTracker) UpdateLimits(maxPerHour, maxPerDay int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxPerHour = maxPerHour
	q.maxPerDay = maxPerDay
}
