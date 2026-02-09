package governance

import (
	"testing"
)

func TestQuotaTracker_AllowWithinLimits(t *testing.T) {
	tracker := newQuotaTracker(5, 100)

	for i := 0; i < 5; i++ {
		allowed, reason := tracker.AllowQuery()
		if !allowed {
			t.Fatalf("expected AllowQuery() = true on query %d, got reason: %s", i+1, reason)
		}
	}
}

func TestQuotaTracker_DenyHourlyOverLimit(t *testing.T) {
	tracker := newQuotaTracker(3, 100)

	for i := 0; i < 3; i++ {
		tracker.AllowQuery()
	}

	allowed, reason := tracker.AllowQuery()
	if allowed {
		t.Fatal("expected AllowQuery() = false after exceeding hourly limit")
	}
	if reason != "Hourly query quota exceeded" {
		t.Fatalf("unexpected reason: %s", reason)
	}
}

func TestQuotaTracker_DenyDailyOverLimit(t *testing.T) {
	tracker := newQuotaTracker(0, 3)

	for i := 0; i < 3; i++ {
		tracker.AllowQuery()
	}

	allowed, reason := tracker.AllowQuery()
	if allowed {
		t.Fatal("expected AllowQuery() = false after exceeding daily limit")
	}
	if reason != "Daily query quota exceeded" {
		t.Fatalf("unexpected reason: %s", reason)
	}
}

func TestQuotaTracker_UnlimitedWhenZero(t *testing.T) {
	tracker := newQuotaTracker(0, 0)

	for i := 0; i < 1000; i++ {
		allowed, _ := tracker.AllowQuery()
		if !allowed {
			t.Fatalf("expected AllowQuery() = true for unlimited tracker on query %d", i+1)
		}
	}
}

func TestQuotaTracker_GetUsage(t *testing.T) {
	tracker := newQuotaTracker(10, 100)

	for i := 0; i < 3; i++ {
		tracker.AllowQuery()
	}

	hourUsed, dayUsed, hourReset, dayReset := tracker.GetUsage()
	if hourUsed != 3 {
		t.Fatalf("expected hourUsed=3, got %d", hourUsed)
	}
	if dayUsed != 3 {
		t.Fatalf("expected dayUsed=3, got %d", dayUsed)
	}
	if hourReset.IsZero() {
		t.Fatal("expected non-zero hourReset")
	}
	if dayReset.IsZero() {
		t.Fatal("expected non-zero dayReset")
	}
}

func TestQuotaTracker_UpdateLimits(t *testing.T) {
	tracker := newQuotaTracker(2, 100)

	tracker.AllowQuery()
	tracker.AllowQuery()

	allowed, _ := tracker.AllowQuery()
	if allowed {
		t.Fatal("expected denial at limit=2")
	}

	tracker.UpdateLimits(5, 100)

	allowed, _ = tracker.AllowQuery()
	if !allowed {
		t.Fatal("expected AllowQuery() = true after increasing limit")
	}
}
