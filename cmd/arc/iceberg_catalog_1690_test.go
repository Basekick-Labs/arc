package main

import "testing"

type fakeIcebergGate struct{ run bool }

func (g *fakeIcebergGate) CanRun() bool { return g.run }

// #1690: the catalog is gated, per request, on the export's own gate unless
// every node sees the warehouse. Keyed on the gate, not the coordinator:
// cluster.enabled without a coordinator still gates the export on the
// static compactor role.
func TestIcebergCatalogServable(t *testing.T) {
	gate := &fakeIcebergGate{}
	if icebergCatalogServable(false, nil) != nil {
		t.Fatal("no gate (standalone): the catalog was gated")
	}
	if icebergCatalogServable(true, gate) != nil {
		t.Fatal("a shared warehouse gated the catalog")
	}
	// Node-local storage with a gate: a cluster, or cluster.enabled with no
	// coordinator (compaction gate on the static role).
	f := icebergCatalogServable(false, gate)
	if f == nil {
		t.Fatal("node-local storage with a gate: no check")
	}
	if ok, why := f(); ok || why == "" {
		t.Fatalf("closed gate: ok=%v why=%q", ok, why)
	}
	gate.run = true // the compactor role moved here without a restart
	if ok, _ := f(); !ok {
		t.Fatal("open gate refused")
	}
}
