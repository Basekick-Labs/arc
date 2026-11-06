package main

import (
	"testing"

	"github.com/basekick-labs/arc/internal/cluster"
)

func TestLicenseCapacityCompactorWaitsForCoordinator(t *testing.T) {
	gate := newCompactionClusterGate("compactor")
	if !gate.CanScheduleCompaction() {
		t.Fatal("temporary startup fence disabled future scheduling")
	}
	if gate.CanCompact() || (icebergWriterGate{inner: gate}).CanRun() {
		t.Fatal("compactor or Iceberg ran before coordinator established serving permission")
	}
	// An unrestricted coordinator permits this static role without a lease,
	// preserving deployments with automatic compactor failover disabled.
	gate.setCoordinator(&cluster.Coordinator{})
	if !gate.CanCompact() || !(icebergWriterGate{inner: gate}).CanRun() {
		t.Fatal("no-failover static compactor failed to resume after initialization")
	}
	reader := newCompactionClusterGate("reader")
	reader.setCoordinator(&cluster.Coordinator{})
	if reader.CanScheduleCompaction() || reader.CanCompact() {
		t.Fatal("reader acquired compaction capability")
	}
}
