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
	now          func() time.Time
}

func newSlidingWindowCounter(windowSize time.Duration, slotCount int, limit int) *slidingWindowCounter {
	return newSlidingWindowCounterWithClock(windowSize, slotCount, limit, time.Now)
}

// newSlidingWindowCounterWithClock is newSlidingWindowCounter reading the clock now.
func newSlidingWindowCounterWithClock(windowSize time.Duration, slotCount int, limit int, now func() time.Time) *slidingWindowCounter {
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
		lastSlotTime: now().Truncate(slotDuration),
		total:        0,
		limit:        limit,
		now:          now,
	}
}

// advance moves the window forward to the current time, clearing expired slots.
func (s *slidingWindowCounter) advance() {
	now := s.now().Truncate(s.slotDuration)
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
// Test helper: the query path uses Manager.Admit, which checks every limit
// before recording on any (#1524).
// If allowed, it increments the counter and returns true.
func (s *slidingWindowCounter) Allow() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.admitsLocked() {
		return false
	}
	s.recordLocked()
	return true
}

// admits reports whether a request would be allowed now, without counting it
// (#1524: a query is counted only once every limit admits it).
func (s *slidingWindowCounter) admits() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.admitsLocked()
}

// record counts a request that every limit admitted.
func (s *slidingWindowCounter) record() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked()
}

func (s *slidingWindowCounter) admitsLocked() bool {
	s.advance()
	return s.limit <= 0 || s.total < s.limit
}

func (s *slidingWindowCounter) recordLocked() {
	s.advance()
	s.slots[s.currentSlot]++
	s.total++
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

// RetryAfterSec returns the number of seconds until enough of the window has
// expired for Allow to succeed. Returns 0 if there is remaining capacity.
func (s *slidingWindowCounter) RetryAfterSec() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.advance()

	if s.limit <= 0 || s.total < s.limit {
		return 0
	}

	// A slot is cleared when currentSlot comes round to it, so the slot after
	// currentSlot goes first, one slotDuration after lastSlotTime, and
	// currentSlot itself goes last, slotCount slotDurations after it. Slots
	// can be empty, and a lowered limit can need more than one of them gone,
	// so walk them oldest first until what is left fits under the limit.
	held := s.total
	steps := 1
	for ; steps < s.slotCount; steps++ {
		held -= s.slots[(s.currentSlot+steps)%s.slotCount]
		if held < s.limit {
			break
		}
	}
	return retryAfterSeconds(s.lastSlotTime.Add(time.Duration(steps) * s.slotDuration).Sub(s.now()))
}

// UpdateLimit changes the rate limit. Takes effect immediately.
func (s *slidingWindowCounter) UpdateLimit(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.limit = limit
}

// retryAfterSeconds rounds a wait up to whole seconds, as a Retry-After header
// carries it, so a client that waits that long is not early. It is at least 1,
// as the wait can have ended between reading the state and reading the clock.
func retryAfterSeconds(wait time.Duration) int {
	secs := int((wait + time.Second - 1) / time.Second)
	if secs < 1 {
		return 1
	}
	return secs
}
