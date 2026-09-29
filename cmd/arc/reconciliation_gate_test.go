package main

import (
	"testing"

	"github.com/basekick-labs/arc/internal/cluster"
)

type fakeGateCoordinator struct {
	active bool
	ready  bool
}

func (f fakeGateCoordinator) IsActiveCompactor() bool   { return f.active }
func (f fakeGateCoordinator) ReplicationReady() bool    { return f.ready }
func (f fakeGateCoordinator) GetRole() cluster.NodeRole { return cluster.RoleWriter }

// On per-node storage the manifest sweep waits for file replication to
// converge on this node (#959); the storage scan never waits. The hold is
// armed only when replication and its catch-up walker are both enabled,
// since with the walker off readiness is never reached. Shared storage keeps
// its one-node-sweeps rule for both halves.
func TestReconciliationGate_LocalHoldsManifestSweepUntilCaughtUp(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		backend              string
		replication, catchUp bool
		active, ready        bool
		wantScan, wantSweep  bool
	}{
		{"local, replicating, not caught up", "local", true, true, false, false, true, false},
		{"local, replicating, caught up", "local", true, true, false, true, true, true},
		{"local, replicating, walker disabled, not caught up", "local", true, false, false, false, true, true},
		{"local, no replication, not ready", "local", false, true, false, false, true, true},
		{"shared, active compactor, not caught up", "s3", true, true, true, false, true, true},
		{"shared, not the active compactor, caught up", "s3", true, true, false, true, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newReconciliationClusterGate(fakeGateCoordinator{active: tc.active, ready: tc.ready}, tc.backend, tc.replication, tc.catchUp)
			if got := g.ShouldRunStorageScan(); got != tc.wantScan {
				t.Errorf("ShouldRunStorageScan = %v, want %v", got, tc.wantScan)
			}
			if got := g.ShouldRunManifestSweep(); got != tc.wantSweep {
				t.Errorf("ShouldRunManifestSweep = %v, want %v", got, tc.wantSweep)
			}
		})
	}
}
