package main

import (
	"strings"
	"testing"
)

func TestLocalClusterWithoutReplication(t *testing.T) {
	for _, tc := range []struct {
		name                string
		coord               bool
		backend             string
		shared, replication bool
		want                bool
	}{
		{"standalone", false, "local", false, false, false},
		{"local cluster, replication off", true, "local", false, false, true},
		{"local cluster with replication (Pattern 1)", true, "local", false, true, false},
		{"shared storage (Pattern 2)", true, "s3", true, false, false},
		// #1603: an object store without shared_storage_mode is its own
		// misconfiguration, not this one; "readers hold no data" is false there.
		{"object store without shared mode", true, "s3", false, false, false},
	} {
		if got := localClusterWithoutReplication(tc.coord, tc.backend, tc.shared, tc.replication); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestLocalClusterWithoutReplicationWarningBranchesOnFailover(t *testing.T) {
	without := localClusterWithoutReplicationWarning(false)
	with := localClusterWithoutReplicationWarning(true)
	if !strings.Contains(without, "every writer-role node acts as a primary writer") || strings.Contains(without, "promoted") {
		t.Errorf("without failover: %q", without)
	}
	if !strings.Contains(with, "promoted by failover starts from an empty disk") {
		t.Errorf("with failover: %q", with)
	}
	for _, w := range []string{without, with} {
		if !strings.Contains(w, "cluster.replication_enabled=true") || strings.Contains(w, "prefix") {
			t.Errorf("advice: %q; want replication, and no per-node cold prefix (the cold mark requires one store)", w)
		}
	}
}

func TestGateTieringInLocalClusterWithoutReplication(t *testing.T) {
	for role, want := range map[string]bool{"reader": true, "compactor": true, "writer": false, "standalone": false} {
		if got := gateTieringInLocalClusterWithoutReplication(role); got != want {
			t.Errorf("role %q: %v, want %v", role, got, want)
		}
	}
}
