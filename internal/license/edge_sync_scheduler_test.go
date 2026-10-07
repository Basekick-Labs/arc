package license

import "testing"

func TestClientEdgeSyncSchedulerReadsCurrentLicense(t *testing.T) {
	var absent *Client
	if absent.CanUseEdgeSyncScheduler() {
		t.Fatal("nil client permits scheduling")
	}
	c := &Client{}
	for _, l := range []*License{nil, {Tier: TierStarter, Status: "active"}, {Tier: TierStarter, Status: "expired"}, {Tier: TierProfessional, Status: "grace_period"}, {Tier: TierProfessional, Status: "read_only"}, nil} {
		c.mu.Lock()
		c.license = l
		c.mu.Unlock()
		if got := c.CanUseEdgeSyncScheduler(); got != l.CanUseCQScheduler() || got != l.CanUseRetentionScheduler() {
			t.Fatal("edge sync entitlement differs from CQ/retention after license replacement")
		}
	}
}
