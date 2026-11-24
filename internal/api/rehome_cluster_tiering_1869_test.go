//go:build duckdb_arrow

package api

import (
	"context"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/tiering"
)

// #1869: with tiered storage, each node's part of a job keeps its tier rows:
// its check of the source before it acks published, the outputs' rows from
// published, the source's row forgotten once the source is gone here.
func TestLocalParticipantKeepsTierRows1869(t *testing.T) {
	r := newLocalRig(t)
	ft := &fakeTiering1825{rows: map[string]bool{lrSource: true}}
	r.h.swap.TieringExpected = true
	r.h.SetTiering(ft)
	r.h.Wired()
	r.advance(raft.RehomeCommitted)
	// A cold copy found by this node's check: published is not acked.
	ft.checkErr = tiering.ErrRehomeColdCopy
	if err := r.c.RehomeAdvance(context.Background(), "ab12", raft.RehomePublished); err != nil {
		t.Fatal(err)
	}
	j, _ := r.c.RehomeJob("ab12")
	if err := r.h.ParticipateLocal(context.Background(), j, true); err == nil {
		t.Fatal("acked published with a cold copy of the source")
	}
	if r.has(r.job.Outputs[0].Key) {
		t.Fatal("placed an output after the check failed")
	}
	ft.checkErr = nil
	r.part()
	for _, o := range r.job.Outputs {
		if !ft.rows[o.Key] {
			t.Fatalf("published: no tier row for %s", o.Key)
		}
	}
	if err := r.c.RehomeClaim(context.Background(), "ab12"); err != nil {
		t.Fatal(err)
	}
	r.advance(raft.RehomeLatched)
	if ft.rows[lrSource] {
		t.Fatal("latched: the source's row is kept after the source went")
	}
	r.advance(raft.RehomeRemoved)
	for _, o := range r.job.Outputs {
		if !ft.rows[o.Key] {
			t.Fatalf("removed: no tier row for %s", o.Key)
		}
	}
}

// #1869: a revert forgets the outputs' rows on this node.
func TestLocalParticipantRevertForgetsTierRows1869(t *testing.T) {
	r := newLocalRig(t)
	ft := &fakeTiering1825{rows: map[string]bool{lrSource: true}}
	r.h.swap.TieringExpected = true
	r.h.SetTiering(ft)
	r.h.Wired()
	r.advance(raft.RehomeCommitted)
	r.advance(raft.RehomePublished)
	if !ft.rows[r.job.Outputs[0].Key] {
		t.Fatal("no output row at published")
	}
	if err := r.c.RehomeAdvance(context.Background(), "ab12", raft.RehomeReverting); err != nil {
		t.Fatal(err)
	}
	r.end() // to reverted
	r.part()
	for _, o := range r.job.Outputs {
		if ft.rows[o.Key] {
			t.Fatalf("reverted: %s keeps its row", o.Key)
		}
	}
	if !ft.rows[lrSource] {
		t.Fatal("reverted: the source lost its row")
	}
}

// #1869: the shared-store participant keeps this node's tier rows: its check
// and the outputs' rows at published, the source's row forgotten at removed,
// the outputs' rows forgotten at reverted.
func TestClusterParticipantKeepsTierRows1869(t *testing.T) {
	r := newClusterRig(t)
	ft := &fakeTiering1825{rows: map[string]bool{r.key: true}}
	r.swap.TieringExpected = true
	r.h.SetTiering(ft)
	r.h.Wired()
	out := raft.RehomeOutput{Key: "db/cpu/2026/11/19/05/o.parquet", SizeBytes: 10, SHA256: "s"}
	j := raft.RehomeJob{ID: "ab12", Database: "db", Measurement: "cpu", Source: raft.FileEntry{Path: r.key}, Outputs: []raft.RehomeOutput{out}}
	ctx := context.Background()
	j.State, j.StateIndex = raft.RehomePublished, 1
	ft.checkErr = tiering.ErrRehomeColdCopy
	if err := r.h.ParticipateCluster(ctx, j, true); err == nil || ft.rows[out.Key] {
		t.Fatalf("published with a cold copy: err %v, row %v", err, ft.rows[out.Key])
	}
	ft.checkErr = nil
	if err := r.h.ParticipateCluster(ctx, j, true); err != nil || !ft.rows[out.Key] {
		t.Fatalf("published: err %v, row %v", err, ft.rows[out.Key])
	}
	j.State, j.StateIndex = raft.RehomeRemoved, 3
	if err := r.h.ParticipateCluster(ctx, j, true); err != nil || ft.rows[r.key] || !ft.rows[out.Key] {
		t.Fatalf("removed: err %v, source row %v, output row %v", err, ft.rows[r.key], ft.rows[out.Key])
	}
	j.State, j.StateIndex = raft.RehomeReverted, 4
	if err := r.h.ParticipateCluster(ctx, j, false); err != nil || ft.rows[out.Key] {
		t.Fatalf("reverted: err %v, output row %v", err, ft.rows[out.Key])
	}
}

// Not wired yet: every tier step fails (and is retried).
func TestClusterTierStepWaitsForWiring1869(t *testing.T) {
	r := newClusterRig(t)
	r.swap.TieringExpected = true
	j := raft.RehomeJob{ID: "ab12", State: raft.RehomePublished, Source: raft.FileEntry{Path: r.key}}
	if err := r.h.ParticipateCluster(context.Background(), j, true); err == nil {
		t.Fatal("acked published before tiering was wired")
	}
}

// A missed end settled with tiering: rows follow what the manifest lists.
func TestLocalSweepSettlesTierRows1869(t *testing.T) {
	r := newLocalRig(t)
	ft := &fakeTiering1825{rows: map[string]bool{lrSource: true}}
	r.h.swap.TieringExpected = true
	r.h.SetTiering(ft)
	r.h.Wired()
	r.advance(raft.RehomeCommitted)
	r.advance(raft.RehomePublished)
	ctx := context.Background()
	if err := r.c.RehomeClaim(ctx, "ab12"); err != nil {
		t.Fatal(err)
	}
	for _, to := range []string{raft.RehomeLatched, raft.RehomeRemoved} {
		for r.c.RehomeAdvance(ctx, "ab12", to) != nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	delete(ft.rows, r.job.Outputs[0].Key) // as if never recorded here
	r.end()
	if err := r.h.SweepLocal(ctx); err != nil {
		t.Fatal(err)
	}
	if ft.rows[lrSource] || !ft.rows[r.job.Outputs[0].Key] {
		t.Fatalf("rows after the settle: %v", ft.rows)
	}
}
