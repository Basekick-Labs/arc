package throttle

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDebouncer_FirstCallProceeds is the regression guard for the #420 bug: with
// a monotonic anchor, time.Since(processStart) is small early in the process's
// life, so `now - 0` is INSIDE any practical interval. Without the last==0
// sentinel the very first call would be throttled. It must fire.
func TestDebouncer_FirstCallProceeds(t *testing.T) {
	d := New(30 * time.Second)
	if !d.TryAcquire() {
		t.Fatal("first TryAcquire must succeed (last==0 sentinel); it was throttled")
	}
}

// TestDebouncer_SecondCallThrottled verifies the window actually throttles: after
// a successful acquire, an immediate second call is rejected.
func TestDebouncer_SecondCallThrottled(t *testing.T) {
	d := New(30 * time.Second)
	if !d.TryAcquire() {
		t.Fatal("first acquire should succeed")
	}
	if d.TryAcquire() {
		t.Fatal("second acquire within the window should be throttled")
	}
}

// TestDebouncer_ReAcquiresAfterInterval verifies eligibility returns once the
// interval elapses. Uses a tiny interval so the test is fast.
func TestDebouncer_ReAcquiresAfterInterval(t *testing.T) {
	d := New(10 * time.Millisecond)
	if !d.TryAcquire() {
		t.Fatal("first acquire should succeed")
	}
	if d.TryAcquire() {
		t.Fatal("immediate re-acquire should be throttled")
	}
	time.Sleep(15 * time.Millisecond)
	if !d.TryAcquire() {
		t.Fatal("acquire after the interval elapsed should succeed")
	}
}

// TestDebouncer_NonPositiveIntervalClamped verifies New clamps a zero or
// negative interval to the floor rather than producing a degenerate throttle
// (where the time check always passes). The first call still fires (sentinel),
// and an immediate second call is throttled by the clamped floor.
func TestDebouncer_NonPositiveIntervalClamped(t *testing.T) {
	for _, iv := range []time.Duration{0, -1 * time.Second} {
		d := New(iv)
		if !d.TryAcquire() {
			t.Fatalf("New(%v): first call should fire", iv)
		}
		if d.TryAcquire() {
			t.Fatalf("New(%v): immediate second call should be throttled by the clamped floor, not allowed through", iv)
		}
	}
}

// TestDebouncer_ConcurrentSingleWinner is the CAS-contention test: many
// goroutines race on the first acquire; exactly one must win.
func TestDebouncer_ConcurrentSingleWinner(t *testing.T) {
	d := New(30 * time.Second)

	const goroutines = 200
	var winners atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			<-start // release all at once to maximize contention
			if d.TryAcquire() {
				winners.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := winners.Load(); got != 1 {
		t.Fatalf("expected exactly 1 winner under concurrent contention, got %d", got)
	}
}

// TestDebouncer_Remaining verifies the remaining-time accounting used for HTTP
// 429 retry-after.
func TestDebouncer_Remaining(t *testing.T) {
	// Before any fire, a call would be eligible now → 0.
	d := New(30 * time.Second)
	if r := d.Remaining(); r != 0 {
		t.Errorf("Remaining before first fire = %v, want 0", r)
	}

	// After firing, remaining should be > 0 and <= interval.
	if !d.TryAcquire() {
		t.Fatal("acquire should succeed")
	}
	r := d.Remaining()
	if r <= 0 || r > 30*time.Second {
		t.Errorf("Remaining after fire = %v, want (0, 30s]", r)
	}

	// After the interval elapses, remaining is 0 again.
	d2 := New(10 * time.Millisecond)
	d2.TryAcquire()
	time.Sleep(15 * time.Millisecond)
	if r := d2.Remaining(); r != 0 {
		t.Errorf("Remaining after interval elapsed = %v, want 0", r)
	}
}

// TestDebouncer_RemainingRoundsUpForCaller documents the contract debug.go
// relies on: Remaining returns a sub-second value in the last part of the
// window, which the caller rounds UP to avoid retry storms. Here we just assert
// Remaining is a positive sub-interval so the caller's round-up has something to
// work with.
func TestDebouncer_RemainingSubSecond(t *testing.T) {
	d := New(500 * time.Millisecond)
	d.TryAcquire()
	time.Sleep(50 * time.Millisecond)
	r := d.Remaining()
	if r <= 0 || r >= 500*time.Millisecond {
		t.Errorf("Remaining mid-window = %v, want in (0, 500ms)", r)
	}
}
