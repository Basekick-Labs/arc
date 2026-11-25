package rehome

import (
	"context"
	"testing"
)

// #1845 item 1: a waived job's record outlives the job for the Iceberg
// export, survives a restart, is never a job, and goes when told.
func TestReplacedRecordLifecycle(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	r := seeded(t, store)
	j := testJob("ab")
	if err := r.Begin(j); err != nil {
		t.Fatal(err)
	}
	if err := r.SetOutputs("ab", testOutputs("ab")); err != nil {
		t.Fatal(err)
	}
	for _, s := range []State{StateStaged, StateCommitted, StateLatched} {
		if err := r.Advance(ctx, "ab", s); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := r.Get("ab")
	if err := r.KeepReplaced(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err := r.End(ctx, "ab"); err != nil {
		t.Fatal(err)
	}
	check := func(r *Registry, what string) {
		t.Helper()
		w := r.ReplacedWaived(prefix)
		if len(w) != 1 || len(w[srcKey]) != 2 || w[srcKey][0] != outA || w[srcKey][1] != outB {
			t.Fatalf("%s: waived %v", what, w)
		}
		if len(r.ReplacedWaived("db/mem/")) != 0 {
			t.Fatalf("%s: another measurement answered", what)
		}
		// Never a job: no slot, no ownership, nothing left out of reads.
		if r.Active() || len(r.Jobs()) != 0 || r.Owns(srcKey) || r.Owns(outA) || len(r.Replaced(prefix)) != 0 || len(r.ExcludedFor(ctx, prefix)) != 0 {
			t.Fatalf("%s: the record acts as a job", what)
		}
	}
	check(r, "after the end")
	again := seeded(t, store)
	check(again, "after a restart")
	if err := again.ReplacedDone(ctx, "db", "cpu", "db/cpu/2026/11/18/23/other.parquet"); err != nil || len(again.ReplacedWaived(prefix)) != 1 {
		t.Fatalf("another file's done dropped the record: %v", err)
	}
	if err := again.ReplacedDone(ctx, "db", "cpu", srcKey); err != nil {
		t.Fatal(err)
	}
	if len(again.ReplacedWaived(prefix)) != 0 || len(seeded(t, store).ReplacedWaived(prefix)) != 0 {
		t.Fatal("the record outlived its done")
	}
}

// A record that cannot be used is skipped and named, never a reason to stop.
func TestSeedSkipsABadReplacedRecord(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	for name, body := range map[string]string{
		"bad.json":   "{not json",
		"wrong.json": `{"id":"other","database":"db","measurement":"cpu","source":"db/cpu/2026/11/18/23/x.parquet","outputs":["db/cpu/2026/11/18/23/a.parquet"]}`,
		"esc.json":   `{"id":"esc","database":"db","measurement":"cpu","source":"db/cpu/2026/11/18/23/x.parquet","outputs":["_rehome/x.parquet"]}`,
		"cross.json": `{"id":"cross","database":"db","measurement":"cpu","source":"db/cpu/2026/11/18/23/x.parquet","outputs":["db/mem/2026/11/18/23/a.parquet"]}`,
	} {
		if err := store.Write(ctx, ReplacedRoot+"/"+name, []byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	r := NewRegistry(store)
	if err := r.Seed(ctx); err != nil {
		t.Fatalf("a bad replacement record stopped the start: %v", err)
	}
	if n := len(r.ReplacedSkipped()); n != 4 {
		t.Fatalf("skipped %v, want 4", r.ReplacedSkipped())
	}
	if len(r.ReplacedWaived(prefix)) != 0 {
		t.Fatal("a bad record was used")
	}
}
