package main

import "testing"

type fakeIcebergGate struct{ run bool }

func (g *fakeIcebergGate) CanRun() bool { return g.run }

// #1690: only a node-local-storage cluster gates the catalog, per request,
// on the export's own gate; a nil gate always serves, as the export runs.
func TestIcebergCatalogServable(t *testing.T) {
	gate := &fakeIcebergGate{}
	if icebergCatalogServable(false, false, gate) != nil {
		t.Fatal("standalone gated the catalog")
	}
	if icebergCatalogServable(true, true, gate) != nil {
		t.Fatal("a shared-warehouse cluster gated the catalog")
	}
	if f := icebergCatalogServable(true, false, nil); f == nil {
		t.Fatal("node-local cluster: no check")
	} else if ok, _ := f(); !ok {
		t.Fatal("a nil gate refused")
	}
	f := icebergCatalogServable(true, false, gate)
	if ok, why := f(); ok || why == "" {
		t.Fatalf("closed gate: ok=%v why=%q", ok, why)
	}
	gate.run = true // the compactor role moved here without a restart
	if ok, _ := f(); !ok {
		t.Fatal("open gate refused")
	}
}
