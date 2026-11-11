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

func TestLocalClusterWithoutReplicationWarning(t *testing.T) {
	w := localClusterWithoutReplicationWarning()
	for _, want := range []string{
		"each writer keeps only the data it ingested",
		"primary writer reach only its own files",
		"report cold rows another node deleted as lost",
		"cluster.replication_enabled=true",
	} {
		if !strings.Contains(w, want) {
			t.Errorf("warning lacks %q: %q", want, w)
		}
	}
	// One primary is elected whenever Raft runs (#872), and the cold mark
	// requires one cold store everywhere (#1552): neither "every writer is a
	// primary" nor a per-node cold prefix is true or good advice.
	for _, bad := range []string{"every writer-role node acts as a primary", "prefix"} {
		if strings.Contains(w, bad) {
			t.Errorf("warning says %q: %q", bad, w)
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
