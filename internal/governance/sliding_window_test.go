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
