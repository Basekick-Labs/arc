package governance

import (
	"context"
	"testing"
)

// A refusal names the limit it hit, by its period: the reasons Admit has
// always answered with.
func TestAdmitRefusalReasons(t *testing.T) {
	for _, c := range []struct {
		policy Policy
		reason string
	}{
		{Policy{TokenID: 1, RateLimitPerMinute: 1}, "Rate limit exceeded: 1 queries per minute"},
		{Policy{TokenID: 1, RateLimitPerHour: 1}, "Rate limit exceeded: 1 queries per hour"},
		{Policy{TokenID: 1, MaxQueriesPerHour: 1}, "Hourly query quota exceeded"},
	} {
		m := setupTestManager(t, nil)
		p := c.policy
		m.CreatePolicy(context.Background(), &p)
		m.Admit(1)
		if r := m.Admit(1); r.Allowed || r.Reason != c.reason {
			t.Errorf("%+v: refusal %+v, want %q", c.policy, r, c.reason)
		}
	}
}
