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
	*localFake
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
	old, oldMax := rehomeExportRetry, rehomeExportRetryMax
	rehomeExportRetry, rehomeExportRetryMax = 0, 0 // no pacing between the test's questions
	t.Cleanup(func() { rehomeExportRetry, rehomeExportRetryMax = old, oldMax })
	r := newLocalRig(t)
	c := &exportingCluster{localFake: r.c, present: true}
	r.h.swap.ExportExpected = true
	r.h.SetCluster(c, func() (bool, string) { return true, "" })
	f := &fakeClusterExport{}
	r.h.SetExport(f.ask)
	r.h.Wired()
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

// #1870, local storage: the exporting node holds until its table no longer
// names the source; a non-exporting node goes on while some node exports,
// and holds while none does; a waiver ends a hold.
func TestClusterExportHoldLocal1870(t *testing.T) {
	h, c, f, j := exportRig(t)
	ctx := context.Background()
	f.set(true, false)
	if err := h.clusterExportHold(ctx, j); err == nil {
		t.Fatal("the exporting node did not hold while the table names the source")
	}
	f.set(true, true)
	if err := h.clusterExportHold(ctx, j); err != nil {
		t.Fatalf("held after the table was confirmed: %v", err)
	}
	f.set(false, false)
	if err := h.clusterExportHold(ctx, j); err != nil {
		t.Fatalf("a non-exporting node held while a node exports: %v", err)
	}
	c.present = false
	if err := h.clusterExportHold(ctx, j); err == nil {
		t.Fatal("went on with no node exporting")
	}
	ok, held := h.waiveClusterExport(j.ID)
	if !ok || !held {
		t.Fatalf("waiver: ok %v held %v", ok, held)
	}
	if err := h.clusterExportHold(ctx, j); err != nil {
		t.Fatalf("held after the waiver: %v", err)
	}
	if ok, _ := h.waiveClusterExport("ffff"); ok {
		t.Fatal("waived a job the cluster does not have")
	}
}

// #1870: the latched step does not unlink the source while the export holds.
func TestLocalLatchWaitsForTheExport1870(t *testing.T) {
	old, oldMax := rehomeExportRetry, rehomeExportRetryMax
	rehomeExportRetry, rehomeExportRetryMax = 0, 0
	t.Cleanup(func() { rehomeExportRetry, rehomeExportRetryMax = old, oldMax })
	r := newLocalRig(t)
	c := &exportingCluster{localFake: r.c, present: true}
	r.h.swap.ExportExpected = true
	r.h.SetCluster(c, func() (bool, string) { return true, "" })
	f := &fakeClusterExport{exporter: true}
	r.h.SetExport(f.ask)
	r.h.Wired()
	r.advance(raft.RehomeCommitted)
	r.advance(raft.RehomePublished)
	if err := r.c.RehomeClaim(context.Background(), "ab12"); err != nil {
		t.Fatal(err)
	}
	r.advanceOnly(raft.RehomeLatched)
	j, _ := r.c.RehomeJob("ab12")
	if err := r.h.ParticipateLocal(context.Background(), j, true); err == nil || !r.has(lrSource) {
		t.Fatalf("latched with the table still naming the source: err %v, source here %v", err, r.has(lrSource))
	}
	f.set(true, true)
	if err := r.h.ParticipateLocal(context.Background(), j, true); err != nil || r.has(lrSource) {
		t.Fatalf("after the table was published: err %v, source here %v", err, r.has(lrSource))
	}
}
