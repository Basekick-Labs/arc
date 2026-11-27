package governance

import (
	"testing"
	"time"
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

func newQuotaTrackerAt(clock *fakeClock, maxPerHour, maxPerDay int) *quotaTracker {
	tracker := newQuotaTracker(maxPerHour, maxPerDay)
	tracker.now = clock.Now
	tracker.hourResetAt = clock.Now().Truncate(time.Hour).Add(time.Hour)
	tracker.dayResetAt = clock.Now().Truncate(24 * time.Hour).Add(24 * time.Hour)
	return tracker
}

func TestQuotaTracker_RetryAfterSec(t *testing.T) {
	const offset = 10*time.Minute + 15*time.Second + 400*time.Millisecond

	tests := []struct {
		name       string
		maxPerHour int
		maxPerDay  int
		queries    int
		want       int
	}{
		{name: "hourly exhausted waits for the next hour", maxPerHour: 2, maxPerDay: 100, queries: 2, want: 3600 - 615},
		{name: "daily exhausted waits for the next day", maxPerDay: 2, queries: 2, want: 86400 - 615},
		{name: "both exhausted waits for the later reset", maxPerHour: 2, maxPerDay: 2, queries: 2, want: 86400 - 615},
		{name: "within both quotas", maxPerHour: 3, maxPerDay: 3, queries: 2, want: 0},
		{name: "no quotas", queries: 2, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeClock()
			tracker := newQuotaTrackerAt(clock, tt.maxPerHour, tt.maxPerDay)
			clock.Advance(offset)
			for i := 0; i < tt.queries; i++ {
				if allowed, reason := tracker.AllowQuery(); !allowed {
					t.Fatalf("query %d: refused: %s", i+1, reason)
				}
			}

			got := tracker.RetryAfterSec()
			if got != tt.want {
				t.Fatalf("RetryAfterSec() = %d, want %d", got, tt.want)
			}
			if tt.want == 0 {
				return
			}

			clock.Advance(time.Duration(got-1) * time.Second)
			if allowed, _ := tracker.AllowQuery(); allowed {
				t.Fatalf("AllowQuery() succeeded %d seconds into a %d second wait", got-1, got)
			}
			clock.Advance(time.Second)
			if allowed, reason := tracker.AllowQuery(); !allowed {
				t.Fatalf("AllowQuery() refused after waiting the %d seconds RetryAfterSec() gave: %s", got, reason)
			}
		})
	}
}

// A wait that ends exactly on the reset time has waited long enough, so the
// reset time itself belongs to the new hour and the new day.
func TestQuotaTracker_ResetsOnTheBoundary(t *testing.T) {
	tests := []struct {
		name       string
		maxPerHour int
		maxPerDay  int
		want       int
	}{
		{name: "hour", maxPerHour: 1, want: 3600},
		{name: "day", maxPerDay: 1, want: 86400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newFakeClock()
			tracker := newQuotaTrackerAt(clock, tt.maxPerHour, tt.maxPerDay)
			if allowed, reason := tracker.AllowQuery(); !allowed {
				t.Fatalf("first query refused: %s", reason)
			}

			got := tracker.RetryAfterSec()
			if got != tt.want {
				t.Fatalf("RetryAfterSec() = %d, want %d", got, tt.want)
			}
			clock.Advance(time.Duration(got) * time.Second)
			if allowed, reason := tracker.AllowQuery(); !allowed {
				t.Fatalf("query on the reset time refused: %s", reason)
			}
		})
	}
}
