package iceberg

import (
	"context"
	"testing"
	"time"
)

// #1886: a version published after the sync loop stopped (the export's
// final drain at shutdown) reaches Glue through Flush.
func TestGlueSyncFlushSyncsWhatWasPublishedAfterTheLoop(t *testing.T) {
	r := newGlueRig(t)
	r.pass(t)
	r.sync.Start()
	deadline := time.Now().Add(10 * time.Second)
	for r.glue.table("arc_mydb", "cpu") == nil && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if r.glue.table("arc_mydb", "cpu") == nil {
		t.Fatal("the loop never registered the table")
	}
	r.sync.Stop() // the loop is gone, as at shutdown
	r.publish(t, "cpu_b.parquet", 1_700_000_100_000_000)
	if got := r.glue.table("arc_mydb", "cpu").Parameters["metadata_location"]; got == r.published(t) {
		t.Fatal("Glue already points at the new version: the test does not exercise Flush")
	}
	r.sync.Flush(context.Background())
	if got, want := r.glue.table("arc_mydb", "cpu").Parameters["metadata_location"], r.published(t); got != want {
		t.Fatalf("after Flush Glue points at %s, want %s", got, want)
	}
}

// Flush does nothing for a sync that never started, or where another node
// exports.
func TestGlueSyncFlushNoOps(t *testing.T) {
	t.Run("never started", func(t *testing.T) {
		r := newGlueRig(t)
		r.pass(t)
		r.sync.Flush(context.Background())
		if r.glue.table("arc_mydb", "cpu") != nil {
			t.Fatal("Flush of a sync that never started wrote to Glue")
		}
	})
	t.Run("closed gate", func(t *testing.T) {
		r := newGlueRig(t)
		r.gate.open = false
		r.sync.Start()
		r.pass(t)
		r.sync.Flush(context.Background())
		if w := r.glue.takeWrites(); len(w) != 0 {
			t.Fatalf("Flush on a closed gate wrote %v", w)
		}
	})
}
