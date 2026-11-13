package governance

import (
	"testing"
	"time"
)

func TestSlidingWindowCounter_AllowWithinLimit(t *testing.T) {
	counter := newSlidingWindowCounter(time.Minute, 60, 5)

	for i := 0; i < 5; i++ {
		if !counter.Allow() {
			t.Fatalf("expected Allow() to return true on request %d", i+1)
		}
	}
}

func TestSlidingWindowCounter_DenyOverLimit(t *testing.T) {
	counter := newSlidingWindowCounter(time.Minute, 60, 3)

	for i := 0; i < 3; i++ {
		counter.Allow()
	}

	if counter.Allow() {
		t.Fatal("expected Allow() to return false after exceeding limit")
	}
}

func TestSlidingWindowCounter_UnlimitedWhenZero(t *testing.T) {
	counter := newSlidingWindowCounter(time.Minute, 60, 0)

	for i := 0; i < 1000; i++ {
		if !counter.Allow() {
			t.Fatalf("expected Allow() to return true for unlimited counter on request %d", i+1)
		}
	}
}

func TestSlidingWindowCounter_Remaining(t *testing.T) {
	counter := newSlidingWindowCounter(time.Minute, 60, 10)

	if r := counter.Remaining(); r != 10 {
		t.Fatalf("expected Remaining() = 10, got %d", r)
	}

	for i := 0; i < 3; i++ {
		counter.Allow()
	}

	if r := counter.Remaining(); r != 7 {
		t.Fatalf("expected Remaining() = 7, got %d", r)
	}
}

func TestSlidingWindowCounter_RetryAfterSec(t *testing.T) {
	counter := newSlidingWindowCounter(time.Minute, 60, 2)

	// Not at limit yet
	if r := counter.RetryAfterSec(); r != 0 {
		t.Fatalf("expected RetryAfterSec() = 0 when under limit, got %d", r)
	}

	counter.Allow()
	counter.Allow()

	// At limit
	retryAfter := counter.RetryAfterSec()
	if retryAfter < 1 {
		t.Fatalf("expected RetryAfterSec() >= 1 when at limit, got %d", retryAfter)
	}
}

func TestSlidingWindowCounter_UpdateLimit(t *testing.T) {
	counter := newSlidingWindowCounter(time.Minute, 60, 2)

	counter.Allow()
	counter.Allow()

	if counter.Allow() {
		t.Fatal("expected denial at limit=2")
	}

	// Increase limit
	counter.UpdateLimit(5)

	if !counter.Allow() {
		t.Fatal("expected Allow() to return true after increasing limit")
	}
}

func TestSlidingWindowCounter_DefaultSlotCount(t *testing.T) {
	counter := newSlidingWindowCounter(time.Minute, 0, 10)
	if counter.slotCount != 60 {
		t.Fatalf("expected default slotCount=60, got %d", counter.slotCount)
	}
}

// fakeClock is a clock tests move by hand, so a window's expiry is exact.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// newFakeClock starts on a slot, hour and day boundary, so a test's offsets
// are also the offsets into its slots.
func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)}
}

func newCounterAt(clock *fakeClock, window time.Duration, limit int) *slidingWindowCounter {
	counter := newSlidingWindowCounter(window, 60, limit)
	counter.now = clock.Now
	counter.lastSlotTime = clock.Now().Truncate(counter.slotDuration)
	return counter
}

func allowN(t *testing.T, counter *slidingWindowCounter, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if !counter.Allow() {
			t.Fatalf("request %d of %d: expected Allow() to return true", i+1, n)
		}
	}
}

// assertRetryAfter pins the answer and checks that it is the truth: a client
// retrying a second sooner is still refused, and one retrying on time is let in.
func assertRetryAfter(t *testing.T, counter *slidingWindowCounter, clock *fakeClock, want int) {
	t.Helper()
	if got := counter.RetryAfterSec(); got != want {
		t.Fatalf("RetryAfterSec() = %d, want %d", got, want)
	}
	clock.Advance(time.Duration(want-1) * time.Second)
	if counter.Allow() {
		t.Fatalf("Allow() succeeded %d seconds into a %d second wait", want-1, want)
	}
	clock.Advance(time.Second)
	if !counter.Allow() {
		t.Fatalf("Allow() refused after waiting the %d seconds RetryAfterSec() gave", want)
	}
}

func TestSlidingWindowCounter_RetryAfterSec_Burst(t *testing.T) {
	clock := newFakeClock()
	counter := newCounterAt(clock, time.Minute, 5)
	allowN(t, counter, 5)

	// Nothing frees up until the slot holding the whole burst leaves the window.
	assertRetryAfter(t, counter, clock, 60)
}

func TestSlidingWindowCounter_RetryAfterSec_RoundsUpInsideASlot(t *testing.T) {
	clock := newFakeClock()
	counter := newCounterAt(clock, time.Minute, 5)
	allowN(t, counter, 5)

	clock.Advance(400 * time.Millisecond)
	assertRetryAfter(t, counter, clock, 60)
}

func TestSlidingWindowCounter_RetryAfterSec_OldestUsedSlot(t *testing.T) {
	clock := newFakeClock()
	counter := newCounterAt(clock, time.Minute, 3)
	allowN(t, counter, 1)
	clock.Advance(20 * time.Second)
	allowN(t, counter, 1)
	clock.Advance(20 * time.Second)
	allowN(t, counter, 1)

	// The request 40 seconds ago is the first to leave. The empty slots
	// between now and then free nothing.
	assertRetryAfter(t, counter, clock, 20)
}

func TestSlidingWindowCounter_RetryAfterSec_NextSlotIsOldest(t *testing.T) {
	clock := newFakeClock()
	counter := newCounterAt(clock, time.Minute, 2)
	allowN(t, counter, 1)
	clock.Advance(59 * time.Second)
	allowN(t, counter, 1)

	assertRetryAfter(t, counter, clock, 1)
}

func TestSlidingWindowCounter_RetryAfterSec_WrapsAroundTheBuffer(t *testing.T) {
	clock := newFakeClock()
	counter := newCounterAt(clock, time.Minute, 2)
	clock.Advance(50 * time.Second)
	allowN(t, counter, 1) // slot 50
	clock.Advance(50 * time.Second)
	allowN(t, counter, 1) // slot 40, past the end of the buffer

	// Slot 50 was filled 50 seconds ago.
	assertRetryAfter(t, counter, clock, 10)
}

func TestSlidingWindowCounter_RetryAfterSec_HourWindow(t *testing.T) {
	clock := newFakeClock()
	counter := newCounterAt(clock, time.Hour, 2)
	allowN(t, counter, 2)

	clock.Advance(90 * time.Second)
	assertRetryAfter(t, counter, clock, 3600-90)
}

func TestSlidingWindowCounter_RetryAfterSec_LoweredLimit(t *testing.T) {
	clock := newFakeClock()
	counter := newCounterAt(clock, time.Minute, 4)
	allowN(t, counter, 2)
	clock.Advance(10 * time.Second)
	allowN(t, counter, 2)
	counter.UpdateLimit(1)

	// The older two leaving still leaves two, over the new limit of one, so
	// the wait is for the newer two.
	assertRetryAfter(t, counter, clock, 60)
}
