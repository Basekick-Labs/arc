package governance

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// #1524: a query refused by one limit is counted against none of them.
func TestAdmitRefusalSpendsNothing(t *testing.T) {
	t.Run("hour refusal spends no minute slot", func(t *testing.T) {
		m := setupTestManager(t, nil)
		m.CreatePolicy(context.Background(), &Policy{TokenID: 1, RateLimitPerMinute: 10, RateLimitPerHour: 2})
		for i := 0; i < 2; i++ {
			if !m.Admit(1).Allowed {
				t.Fatalf("query %d refused", i+1)
			}
		}
		if r := m.Admit(1); r.Allowed || r.Kind != KindRateLimit {
			t.Fatalf("third query: %+v", r)
		}
		if u := m.GetTokenUsage(1); u.RateLimitRemainingPerMin != 8 {
			t.Fatalf("minute remaining %d, want 8: the refusal spent a slot", u.RateLimitRemainingPerMin)
		}
	})
	t.Run("quota refusal spends no rate slots", func(t *testing.T) {
		m := setupTestManager(t, nil)
		m.CreatePolicy(context.Background(), &Policy{TokenID: 1, RateLimitPerMinute: 10, RateLimitPerHour: 10, MaxQueriesPerHour: 1})
		if !m.Admit(1).Allowed {
			t.Fatal("first query refused")
		}
		for i := 0; i < 5; i++ {
			if r := m.Admit(1); r.Allowed || r.Kind != KindQuota {
				t.Fatalf("refusal %d: %+v", i+1, r)
			}
		}
		u := m.GetTokenUsage(1)
		if u.RateLimitRemainingPerMin != 9 || u.RateLimitRemainingPerHour != 9 || u.QueriesThisHour != 1 {
			t.Fatalf("usage after refusals: %+v", u)
		}
	})
}

// The refusal names the limit with the longest wait: with the minute limit and
// the daily quota both exhausted, the quota decides, and the reason, kind and
// wait all say so.
func TestAdmitNamesTheBindingLimit(t *testing.T) {
	m := setupTestManager(t, nil)
	m.CreatePolicy(context.Background(), &Policy{TokenID: 1, RateLimitPerMinute: 1, MaxQueriesPerDay: 1})
	if !m.Admit(1).Allowed {
		t.Fatal("first query refused")
	}
	r := m.Admit(1)
	if r.Allowed || r.Kind != KindQuota || r.Reason != "Daily query quota exceeded" || r.RetryAfterSec <= 60 {
		t.Fatalf("refusal %+v: want the daily quota with a wait past the minute", r)
	}
}

// Check and record are one step: with the window between them held open,
// concurrent queries against a limit of 3 admit exactly 3.
func TestAdmitIsAtomic(t *testing.T) {
	m := setupTestManager(t, nil)
	m.CreatePolicy(context.Background(), &Policy{TokenID: 1, RateLimitPerMinute: 3})
	m.afterAdmitCheckForTest = func() { time.Sleep(2 * time.Millisecond) }
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m.Admit(1).Allowed {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := admitted.Load(); n != 3 {
		t.Fatalf("%d admitted, want 3", n)
	}
}

// A client that waits exactly Retry-After after a quota refusal is admitted:
// the refusals spent nothing on the rate limits checked alongside.
func TestAdmitRetryAfterIsHonoured(t *testing.T) {
	m := setupTestManager(t, nil)
	clock := &fakeClock{t: time.Date(2026, 6, 1, 12, 30, 0, 0, time.UTC)}
	m.now = clock.Now
	m.CreatePolicy(context.Background(), &Policy{TokenID: 1, RateLimitPerMinute: 2, MaxQueriesPerHour: 1})
	if !m.Admit(1).Allowed {
		t.Fatal("first query refused")
	}
	var r *EnforcementResult
	for i := 0; i < 3; i++ { // a client hammering after the refusal
		r = m.Admit(1)
	}
	if r.Allowed || r.Kind != KindQuota || r.RetryAfterSec != 1800 {
		t.Fatalf("refusal %+v, want the quota with 1800 s to the hour", r)
	}
	clock.Advance(time.Duration(r.RetryAfterSec-1) * time.Second)
	if m.Admit(1).Allowed {
		t.Fatal("admitted a second early")
	}
	clock.Advance(time.Second)
	if r := m.Admit(1); !r.Allowed {
		t.Fatalf("refused at Retry-After: %+v", r)
	}
}

// Policy updates take the same lock: no race with Admit.
func TestAdmitWithConcurrentPolicyUpdates(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()
	m.CreatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 100, MaxQueriesPerHour: 100})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if i == 0 {
					m.UpdatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 100 + j, MaxQueriesPerHour: 100})
				} else {
					m.Admit(1)
				}
			}
		}(i)
	}
	wg.Wait()
}
