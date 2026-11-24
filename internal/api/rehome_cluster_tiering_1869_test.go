//go:build duckdb_arrow

package api

import (
	"context"
	"testing"

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
