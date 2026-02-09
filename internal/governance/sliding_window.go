package governance

import (
	"sync"
	"time"
)

// slidingWindowCounter implements a sliding window rate limiter using a circular buffer
// of fixed-size time slots. It is safe for concurrent use.
type slidingWindowCounter struct {
	mu           sync.Mutex
	windowSize   time.Duration
	slotDuration time.Duration
	slots        []int
	slotCount    int
	currentSlot  int
	lastSlotTime time.Time
	total        int
	limit        int
}

func newSlidingWindowCounter(windowSize time.Duration, slotCount int, limit int) *slidingWindowCounter {
	if slotCount <= 0 {
		slotCount = 60
	}
	slotDuration := windowSize / time.Duration(slotCount)
	if slotDuration < time.Millisecond {
		slotDuration = time.Millisecond
	}
	return &slidingWindowCounter{
		windowSize:   windowSize,
		slotDuration: slotDuration,
		slots:        make([]int, slotCount),
		slotCount:    slotCount,
		currentSlot:  0,
		lastSlotTime: time.Now().Truncate(slotDuration),
		total:        0,
		limit:        limit,
	}
}

// advance moves the window forward to the current time, clearing expired slots.
func (s *slidingWindowCounter) advance() {
	now := time.Now().Truncate(s.slotDuration)
	elapsed := now.Sub(s.lastSlotTime)
	if elapsed <= 0 {
		return
	}

	slotsToAdvance := int(elapsed / s.slotDuration)
	if slotsToAdvance >= s.slotCount {
		// Entire window has expired — reset everything
		for i := range s.slots {
			s.slots[i] = 0
		}
		s.total = 0
		s.currentSlot = 0
	} else {
		for i := 0; i < slotsToAdvance; i++ {
			s.currentSlot = (s.currentSlot + 1) % s.slotCount
			s.total -= s.slots[s.currentSlot]
			s.slots[s.currentSlot] = 0
		}
	}
	s.lastSlotTime = now
}

// Allow checks if a request is allowed under the rate limit.
// If allowed, it increments the counter and returns true.
func (s *slidingWindowCounter) Allow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.advance()

	if s.limit > 0 && s.total >= s.limit {
		return false
	}

	s.slots[s.currentSlot]++
	s.total++
	return true
}

// Remaining returns the number of requests remaining in the current window.
func (s *slidingWindowCounter) Remaining() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.advance()

	remaining := s.limit - s.total
	if remaining < 0 {
		return 0
	}
	return remaining
}

// RetryAfterSec returns the number of seconds until the oldest slot in the window
// expires, freeing capacity. Returns 0 if there is remaining capacity.
func (s *slidingWindowCounter) RetryAfterSec() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.advance()

	if s.limit <= 0 || s.total < s.limit {
		return 0
	}

	// The oldest slot will expire after one slotDuration
	retryAfter := s.slotDuration - time.Since(s.lastSlotTime)
	if retryAfter <= 0 {
		retryAfter = s.slotDuration
	}
	secs := int(retryAfter.Seconds()) + 1
	if secs < 1 {
		secs = 1
	}
	return secs
}

// UpdateLimit changes the rate limit. Takes effect immediately.
func (s *slidingWindowCounter) UpdateLimit(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limit = limit
}
