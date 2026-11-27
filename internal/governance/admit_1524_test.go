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

	// Both quotas exhausted: the daily reset is the wait, so the daily quota
	// is the reason.
	m2 := setupTestManager(t, nil)
	m2.CreatePolicy(context.Background(), &Policy{TokenID: 1, MaxQueriesPerHour: 1, MaxQueriesPerDay: 1})
	m2.Admit(1)
	if r := m2.Admit(1); r.Reason != "Daily query quota exceeded" {
		t.Fatalf("hour and day exhausted: %+v", r)
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
	// The hour rate limit is the one hammering would have used up before
	// #1524: refusals counted in its sliding window and the retry at the
	// quota's reset was refused by it.
	m.CreatePolicy(context.Background(), &Policy{TokenID: 1, RateLimitPerMinute: 2, RateLimitPerHour: 3, MaxQueriesPerHour: 1})
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

// A policy update waits for an Admit in progress: a limit lowered while a
// query is between its check and its record applies only after the record.
func TestPolicyUpdateWaitsForAdmit(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()
	m.CreatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 5})
	m.Admit(1) // creates the limiter
	l := m.getOrCreateMinuteLimiter(1, 5)
	limit := func() int { l.mu.Lock(); defer l.mu.Unlock(); return l.limit }
	done := make(chan struct{})
	var during int
	m.afterAdmitCheckForTest = func() {
		m.afterAdmitCheckForTest = nil
		go func() {
			m.UpdatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 1})
			close(done)
		}()
		time.Sleep(20 * time.Millisecond)
		during = limit()
	}
	m.Admit(1)
	<-done
	if during != 5 {
		t.Fatalf("limit %d applied while the query was between check and record", during)
	}
	if limit() != 1 {
		t.Fatalf("the update never applied: limit %d", limit())
	}
}

// A policy deleted while a query reads it does not leave a limiter on the
// deleted limits: the delete and Admit are serialised.
func TestPolicyDeleteWithAdmit(t *testing.T) {
	m := setupTestManager(t, nil)
	ctx := context.Background()
	m.CreatePolicy(ctx, &Policy{TokenID: 1, RateLimitPerMinute: 100})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); m.Admit(1) }()
	go func() { defer wg.Done(); m.DeletePolicy(ctx, 1) }()
	wg.Wait()
	if _, ok := m.minuteLimiters[1]; ok {
		// Admit ran first and DeletePolicy removed its limiter, or Admit ran
		// after and found no policy: either way no limiter is left.
		t.Fatal("a limiter outlived its deleted policy")
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
