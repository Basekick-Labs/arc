package security

import (
	"testing"
	"time"
)

func TestReplicationTrackedCapabilityIsAuthenticated(t *testing.T) {
	now := time.Now().Unix()
	tracked := ComputeReplicateSyncHMAC("secret", "nonce", "reader", "cluster", 17, true, now, true)
	legacy := ComputeReplicateSyncHMAC("secret", "nonce", "reader", "cluster", 17, true, now)
	if tracked == legacy {
		t.Fatal("tracked capability absent from handshake MAC")
	}
	if err := ValidateReplicateSyncHMAC("secret", "nonce", "reader", "cluster", 17, true, now, tracked, time.Minute, true); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplicateSyncHMAC("secret", "nonce", "reader", "cluster", 17, true, now, tracked, time.Minute, false); err == nil {
		t.Fatal("capability downgrade accepted")
	}
	if err := ValidateReplicateSyncHMAC("secret", "nonce", "reader", "cluster", 17, true, now, legacy, time.Minute, true); err == nil {
		t.Fatal("legacy MAC promoted to identity-aware replication")
	}
}
