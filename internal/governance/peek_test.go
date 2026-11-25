package governance

import (
	"context"
	"testing"
	"time"
)

// Peek answers what Admit would, and counts nothing: any number of peeks
// leave every limit as it was, and the limits it reports are the policy's.
func TestPeekCountsNothing(t *testing.T) {
	m := setupTestManager(t, nil)
	m.CreatePolicy(context.Background(), &Policy{
		TokenID: 1, RateLimitPerMinute: 2, RateLimitPerHour: 5, MaxQueriesPerHour: 3,
		MaxRowsPerQuery: 100, MaxScanDurationSec: 7,
	})

	for i := 0; i < 10; i++ {
		r := m.Peek(1, "")
		if !r.Allowed || r.MaxRows != 100 || r.MaxDuration != 7*time.Second {
			t.Fatalf("peek %d: %+v", i+1, r)
		}
	}
	u := m.GetTokenUsage(1)
	if u.RateLimitRemainingPerMin != 2 || u.RateLimitRemainingPerHour != 5 || u.QueriesThisHour != 0 {
		t.Fatalf("usage after peeks: %+v", u)
	}

	// Two queries use the minute limit up; a peek then gives Admit's refusal.
	for i := 0; i < 2; i++ {
		if !m.Admit(1).Allowed {
			t.Fatalf("query %d refused", i+1)
		}
	}
	peeked, admitted := m.Peek(1, ""), m.Admit(1)
	if peeked.Allowed || peeked.Kind != KindRateLimit || peeked.Reason != admitted.Reason || peeked.RetryAfterSec < 1 {
		t.Fatalf("peek %+v, want the refusal Admit gives: %+v", peeked, admitted)
	}
}

// A token without a policy is admitted with no limits, as by Admit.
func TestPeekWithoutAPolicy(t *testing.T) {
	m := setupTestManager(t, nil)
	if r := m.Peek(42, ""); !r.Allowed || r.MaxRows != 0 || r.MaxDuration != 0 {
		t.Fatalf("peek without a policy: %+v", r)
	}
}
