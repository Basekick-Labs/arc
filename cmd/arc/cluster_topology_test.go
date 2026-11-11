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

func TestObjectStoreCluster(t *testing.T) {
	for _, tc := range []struct {
		enabled bool
		backend string
		want    bool
	}{
		{false, "s3", false}, {true, "local", false}, {true, "", false},
		{true, "s3", true}, {true, "minio", true}, {true, "azure", true}, {true, "azblob", true}, {true, "gcs", true},
	} {
		if got := objectStoreCluster(tc.enabled, tc.backend); got != tc.want {
			t.Errorf("enabled=%v backend=%q: %v, want %v", tc.enabled, tc.backend, got, tc.want)
		}
	}
}

func TestTieringGateWired(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		shared, repl, objectStore bool
		backend, role             string
		want                      bool
	}{
		{"Pattern 2 (flag)", true, false, true, "s3", "writer", true},
		{"single-writer Pattern 2, no flag (#1603)", false, false, true, "s3", "writer", true},
		{"single-writer Pattern 2, no flag, reader", false, false, true, "minio", "reader", true},
		{"Pattern 1 (replication)", false, true, false, "local", "writer", true},
		{"local, no replication, writer (#1599)", false, false, false, "local", "writer", false},
		{"local, no replication, reader (#1599)", false, false, false, "local", "reader", true},
	} {
		if got := tieringGateWired(tc.shared, tc.repl, tc.objectStore, tc.backend, tc.role); got != tc.want {
			t.Errorf("%s: %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIgnoreReplicationOnObjectStore(t *testing.T) {
	for _, tc := range []struct {
		enabled, repl, objectStore, want bool
	}{
		{true, true, true, true},   // the shipped enterprise-shared reader
		{true, true, false, false}, // Pattern 1: replication between disks
		{true, false, true, false}, // nothing to ignore
		{false, true, true, false}, // no cluster
	} {
		if got := ignoreReplicationOnObjectStore(tc.enabled, tc.repl, tc.objectStore); got != tc.want {
			t.Errorf("cluster=%v replication=%v objectStore=%v: %v, want %v", tc.enabled, tc.repl, tc.objectStore, got, tc.want)
		}
	}
}

func TestIcebergTopologyFor(t *testing.T) {
	type in struct{ sharedMode, sharedBucket, coord, failover, compactionGate bool }
	for _, tc := range []struct {
		name string
		in   in
		want icebergTopology
	}{
		{"OSS local", in{false, false, false, false, false}, icebergTopology{gate: icebergGateNone}},
		{"standalone S3 (restore can leave the catalog behind)", in{false, true, false, false, false}, icebergTopology{authoritative: true, gate: icebergGateNone}},
		{"local cluster", in{false, false, true, true, true}, icebergTopology{gate: icebergGateCompaction}},
		{"multi-writer with failover", in{true, true, true, true, true}, icebergTopology{authoritative: true, barrier: true, gate: icebergGateLease}},
		// Blocker row: no failover manager (licence or Raft) must not drop a
		// multi-writer bucket to the static gate.
		{"multi-writer without a failover manager", in{true, true, true, false, true}, icebergTopology{authoritative: true, barrier: true, gate: icebergGateLease}},
		{"single-writer bucket with failover (#1609)", in{false, true, true, true, true}, icebergTopology{authoritative: true, barrier: true, gate: icebergGateLease}},
		{"single-writer bucket without failover", in{false, true, true, false, true}, icebergTopology{authoritative: true, barrier: true, gate: icebergGateCompaction, warnNoElection: true}},
		{"cluster on S3 without a clustering licence (no coordinator)", in{false, true, false, false, true}, icebergTopology{authoritative: true, gate: icebergGateCompaction, warnNoElection: true}},
	} {
		got := icebergTopologyFor(tc.in.sharedMode, tc.in.sharedBucket, tc.in.coord, tc.in.failover, tc.in.compactionGate)
		if got != tc.want {
			t.Errorf("%s: %+v, want %+v", tc.name, got, tc.want)
		}
	}
}
