package api

import (
	"context"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// exportingCluster is the fake cluster with an answer to "does a node
// export".
type exportingCluster struct {
	*fakeCluster
	present bool
}

func (e *exportingCluster) IcebergExporterPresent() bool { return e.present }

// fakeClusterExport answers as one node's export would.
type fakeClusterExport struct {
	mu       sync.Mutex
	exporter bool // this node's gate is open
	clear    bool // the published table does not name the file
	asked    []string
}

func (f *fakeClusterExport) ask(_ context.Context, db, m, key string) (RehomeExportAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.asked = append(f.asked, key)
	if !f.exporter {
		return RehomeExportAnswer{Why: "this node is not the one exporting to Iceberg", NotExporter: true}, nil
	}
	if f.clear || key == "" {
		return RehomeExportAnswer{Clear: true}, nil
	}
	return RehomeExportAnswer{Why: "the published table still names the file"}, nil
}

func (f *fakeClusterExport) set(exporter, clear bool) {
	f.mu.Lock()
	f.exporter, f.clear = exporter, clear
	f.mu.Unlock()
}

func exportRig(t *testing.T) (*RehomeHandler, *exportingCluster, *fakeClusterExport, raft.RehomeJob) {
	t.Helper()
	r := newLocalRig(t)
	c := &exportingCluster{fakeCluster: r.c.fakeCluster, present: true}
	r.h.swap.ExportExpected = true
	r.h.SetCluster(c, func() (bool, string) { return true, "" })
	f := &fakeClusterExport{}
	r.h.SetExport(f.ask)
	j, _ := c.RehomeJob("ab12")
	return r.h, c, f, j
}

// #1870: the ready check before committed — the exporting node publishes, a
// non-exporting node needs an exporter to exist.
func TestClusterExportReady1870(t *testing.T) {
	h, c, f, j := exportRig(t)
	ctx := context.Background()
	f.set(true, false)
	if err := h.clusterExportReady(ctx, j); err != nil {
		t.Fatalf("exporting node: %v", err)
	}
	f.set(false, false)
	if err := h.clusterExportReady(ctx, j); err != nil {
		t.Fatalf("non-exporting node with an exporter: %v", err)
	}
	c.present = false
	if err := h.clusterExportReady(ctx, j); err == nil {
		t.Fatal("ready with no node exporting")
	}
}

// #1870: the exporting node holds latched until the table no longer names the
// source; once holding, it keeps holding if the lease moves away; a waiver
// ends it; a node that never exported does not hold.
func TestClusterExportHold1870(t *testing.T) {
	h, _, f, j := exportRig(t)
	ctx := context.Background()
	f.set(false, false)
	if err := h.clusterExportHold(ctx, j); err != nil {
		t.Fatalf("a node that does not export held: %v", err)
	}
	f.set(true, false)
	if err := h.clusterExportHold(ctx, j); err == nil {
		t.Fatal("the exporting node did not hold while the table names the source")
	}
	f.set(false, false) // the lease moved away
	if err := h.clusterExportHold(ctx, j); err == nil {
		t.Fatal("stopped holding when the lease moved away")
	}
	f.set(true, true) // the lease is back, the table is published without it
	if err := h.clusterExportHold(ctx, j); err != nil {
		t.Fatalf("held after the table was confirmed: %v", err)
	}
	// Holding again, then waived.
	f.set(true, false)
	if err := h.clusterExportHold(ctx, j); err == nil {
		t.Fatal("not holding")
	}
	if !h.waiveClusterExport(j.ID) {
		t.Fatal("waiver refused for a live job")
	}
	if err := h.clusterExportHold(ctx, j); err != nil {
		t.Fatalf("held after the waiver: %v", err)
	}
	if h.waiveClusterExport("ffff") {
		t.Fatal("waived a job the cluster does not have")
	}
}
