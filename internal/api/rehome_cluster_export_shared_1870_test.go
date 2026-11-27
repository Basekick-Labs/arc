//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
)

// sharedExportCluster is the shared-store fake cluster on a node that is not
// the primary writer, with an answer to "does a node export".
type sharedExportCluster struct {
	*fakeCluster
	present bool
}

func (s *sharedExportCluster) IcebergExporterPresent() bool { return s.present }
func (s *sharedExportCluster) IsPrimaryWriter() bool        { return false }

// publishedTable is the shared warehouse's published table as every node
// reads it.
type publishedTable struct {
	mu    sync.Mutex
	named bool
}

func (p *publishedTable) check(context.Context, string, string, string) (bool, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return true, p.named, nil
}

func sharedExportRig(t *testing.T) (*clusterRig, *sharedExportCluster, *fakeClusterExport, *publishedTable, raft.RehomeJob) {
	t.Helper()
	old, oldMax := rehomeExportRetry, rehomeExportRetryMax
	rehomeExportRetry, rehomeExportRetryMax = 0, 0
	t.Cleanup(func() { rehomeExportRetry, rehomeExportRetryMax = old, oldMax })
	r := newClusterRig(t)
	c := &sharedExportCluster{fakeCluster: r.c, present: true}
	r.swap.ExportExpected = true
	r.h.SetCluster(c, func() (bool, string) { return true, "" })
	f := &fakeClusterExport{}
	r.h.SetExport(f.ask)
	tbl := &publishedTable{named: true}
	r.h.SetExportCheck(tbl.check)
	r.h.Wired()
	e, _ := r.c.ManifestFile(r.key)
	r.c.must(raft.CommandStageRehome, raft.StageRehomePayload{Job: "ab12", Origin: "w1", Database: "db", Measurement: "cpu",
		Source:  raft.CompactionInput{Path: r.key, SizeBytes: e.SizeBytes, SHA256: e.SHA256},
		Outputs: []raft.RehomeOutput{{Key: "db/cpu/2026/11/19/05/o.parquet", StagedKey: raft.RehomeStagedKey("ab12", 0), SizeBytes: 1, SHA256: "o"}},
		Now:     time.Now().UTC()})
	j, _ := c.RehomeJob("ab12")
	return r, c, f, tbl, j
}

// #1870, shared warehouse: a node that does not export waits for the same
// published table as the one that does, so a lease move or a lease gap does
// not let the source go while the table names it.
func TestClusterExportHoldShared1870(t *testing.T) {
	r, c, f, tbl, j := sharedExportRig(t)
	ctx := context.Background()
	f.set(false, false)
	if err := r.h.clusterExportHold(ctx, j); err == nil {
		t.Fatal("a non-exporting node went on while the published table names the source")
	}
	c.present = false // no node exports: still the table decides
	if err := r.h.clusterExportHold(ctx, j); err == nil {
		t.Fatal("went on with no node exporting and the table naming the source")
	}
	tbl.mu.Lock()
	tbl.named = false // the (new) exporter published the swap
	tbl.mu.Unlock()
	if err := r.h.clusterExportHold(ctx, j); err != nil {
		t.Fatalf("held after the published table stopped naming the source: %v", err)
	}
}

// #1870: the waiver works on a node that is not the primary writer, through
// the route, and says whether this node was waiting.
func TestClusterExportWaiverOnANonPrimary1870(t *testing.T) {
	r, _, f, _, j := sharedExportRig(t)
	f.set(false, false)
	_ = r.h.clusterExportHold(context.Background(), j) // holding: the table names the source
	code, out := rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+j.ID+"/resume", "", `{"skip_export_wait": true}`)
	if code != 202 || out["waived"] != true || out["was_waiting"] != true {
		t.Fatalf("waiver on a non-primary node: %d %v", code, out)
	}
	if err := r.h.clusterExportHold(context.Background(), j); err != nil {
		t.Fatalf("held after the waiver: %v", err)
	}
	code, out = rehomeDo(t, r.app, "POST", "/api/v1/rehome/"+j.ID+"/resume", "", `{}`)
	if code != 409 {
		t.Fatalf("a plain resume of a cluster job: %d %v", code, out)
	}
	code, _ = rehomeDo(t, r.app, "POST", fmt.Sprintf("/api/v1/rehome/%s/resume", "ffff"), "", `{"skip_export_wait": true}`)
	if code == 202 {
		t.Fatal("waived a job the cluster does not have")
	}
}

// #1870: the shared-store participant does not ack latched while the export
// holds.
func TestClusterParticipantLatchedWaitsForTheExport1870(t *testing.T) {
	r, _, f, tbl, j := sharedExportRig(t)
	f.set(false, false)
	j.State, j.StateIndex = raft.RehomeLatched, 9
	if err := r.h.ParticipateCluster(context.Background(), j, true); err == nil {
		t.Fatal("acked latched while the published table names the source")
	}
	tbl.mu.Lock()
	tbl.named = false
	tbl.mu.Unlock()
	if err := r.h.ParticipateCluster(context.Background(), j, true); err != nil {
		t.Fatalf("after the table was published: %v", err)
	}
}
