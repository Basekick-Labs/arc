package edgesync

import (
	"context"
	"slices"
	"testing"
)

func holderOf(t *testing.T, idx *HubIndex, spoke, path string) string {
	t.Helper()
	hf, _ := receiptState(t, idx, spoke, path)
	return hf.CompactedInto
}

// A compaction job records its output as the holder of its inputs' receipts;
// a compaction of compactions moves receipts whose holder it consumed; a
// re-fired mark of the earlier job cannot move them back (#1683).
func TestMarkCompactedInto_ChainAndRefire(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	const s = "rocket-01"
	a, b := "db/m/2026/01/01/00/a.parquet", "db/m/2026/01/01/01/b.parquet"
	recordFile(t, idx, s, a, "aa")
	recordFile(t, idx, s, b, "bb")
	hourly0 := s + "/db/m/2026/01/01/00/x_compacted.parquet"
	hourly1 := s + "/db/m/2026/01/01/01/y_compacted.parquet"
	daily := s + "/db/m/2026/01/01/z_daily.parquet"

	if err := idx.MarkCompactedInto(ctx, s, []string{a}, []string{s + "/" + a}, hourly0); err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkCompactedInto(ctx, s, []string{b}, []string{s + "/" + b}, hourly1); err != nil {
		t.Fatal(err)
	}
	if h := holderOf(t, idx, s, a); h != hourly0 {
		t.Fatalf("a holder = %q, want %q", h, hourly0)
	}
	// Daily consumes both hourly outputs (no receipts of their own).
	if err := idx.MarkCompactedInto(ctx, s, []string{"db/m/2026/01/01/00/x_compacted.parquet", "db/m/2026/01/01/01/y_compacted.parquet"}, []string{hourly0, hourly1}, daily); err != nil {
		t.Fatal(err)
	}
	if holderOf(t, idx, s, a) != daily || holderOf(t, idx, s, b) != daily {
		t.Fatalf("holders after daily = %q, %q; want the daily output", holderOf(t, idx, s, a), holderOf(t, idx, s, b))
	}
	// Recovery re-fires the first job: a must stay on the daily output.
	if err := idx.MarkCompactedInto(ctx, s, []string{a}, []string{s + "/" + a}, hourly0); err != nil {
		t.Fatal(err)
	}
	if h := holderOf(t, idx, s, a); h != daily {
		t.Fatalf("a re-fired mark moved the holder back to %q", h)
	}
}

// Tiering sets a holder only where none is recorded (the receipt's own path,
// now cold); a DELETE about to rewrite clears it; Record clears it.
func TestMarkCompactedInPlaceAndNoHolder(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	const s = "rocket-01"
	a, b := "db/m/2026/01/01/00/a.parquet", "db/m/2026/01/01/00/b.parquet"
	recordFile(t, idx, s, a, "aa")
	recordFile(t, idx, s, b, "bb")
	out := s + "/db/m/2026/01/01/00/x_compacted.parquet"
	if err := idx.MarkCompactedInto(ctx, s, []string{b}, []string{s + "/" + b}, out); err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkCompactedInPlace(ctx, s, []string{a, b}); err != nil {
		t.Fatal(err)
	}
	if h := holderOf(t, idx, s, a); h != s+"/"+a {
		t.Fatalf("migrated a holder = %q, want its own path", h)
	}
	if h := holderOf(t, idx, s, b); h != out {
		t.Fatalf("migration overwrote b's compaction holder with %q", h)
	}
	if err := idx.MarkCompactedNoHolder(ctx, s, []string{a}); err != nil {
		t.Fatal(err)
	}
	if hf, _ := receiptState(t, idx, s, a); !hf.Compacted || hf.CompactedInto != "" {
		t.Fatalf("after a DELETE pre-mark: %+v, want compacted with no holder", hf)
	}
	recordFile(t, idx, s, b, "bb2")
	if hf, _ := receiptState(t, idx, s, b); hf.Compacted || hf.CompactedInto != "" {
		t.Fatalf("re-recorded receipt kept %+v", hf)
	}
}

// Verify reports a compacted receipt in HeldCompacted only while its holder
// exists: hot (and listed, on a cluster) or cold. No holder, a removed mark
// or a gone holder: neither held nor missing.
func TestVerify_HeldCompacted(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	backend := newLocalBackendForTest(t, t.TempDir())
	cold := map[string]bool{}
	rec, err := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend,
		ColdExists: func(_ context.Context, p string) (bool, error) { return cold[p], nil }})
	if err != nil {
		t.Fatal(err)
	}
	const s = "rocket-01"
	sha := func(p string) string { return sha256Hex([]byte(p)) }
	paths := map[string]string{
		"hot":     "db/m/2026/01/01/00/hot.parquet",
		"cold":    "db/m/2026/01/01/00/cold.parquet",
		"gone":    "db/m/2026/01/01/00/gone.parquet",
		"legacy":  "db/m/2026/01/01/00/legacy.parquet",
		"removed": "db/m/2026/01/01/00/removed.parquet",
	}
	for _, p := range paths {
		recordFile(t, idx, s, p, sha(p))
	}
	hotOut := s + "/db/m/2026/01/01/00/h_compacted.parquet"
	if err := backend.Write(ctx, hotOut, []byte("x")); err != nil {
		t.Fatal(err)
	}
	mark := func(p, out string) {
		if err := idx.MarkCompactedInto(ctx, s, []string{p}, []string{s + "/" + p}, out); err != nil {
			t.Fatal(err)
		}
	}
	mark(paths["hot"], hotOut)
	mark(paths["removed"], hotOut)
	mark(paths["gone"], s+"/db/m/2026/01/01/00/gone_compacted.parquet")
	if err := idx.MarkCompactedInPlace(ctx, s, []string{paths["cold"]}); err != nil {
		t.Fatal(err)
	}
	cold[s+"/"+paths["cold"]] = true
	if err := idx.MarkCompacted(ctx, s, []string{paths["legacy"]}); err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkRemoved(ctx, s, []string{paths["removed"]}, RemovedByRetention); err != nil {
		t.Fatal(err)
	}
	var entries []ReconcileEntry
	for _, p := range paths {
		entries = append(entries, ReconcileEntry{Path: p, SHA256: sha(p), SizeBytes: 1})
	}
	res, err := rec.Verify(ctx, s, entries, nil)
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(res.HeldCompacted)
	want := []string{paths["cold"], paths["hot"]}
	slices.Sort(want)
	if !slices.Equal(res.HeldCompacted, want) || len(res.Held) != 0 || len(res.Missing) != 0 {
		t.Fatalf("held=%v held_compacted=%v missing=%v, want held_compacted %v only", res.Held, res.HeldCompacted, res.Missing, want)
	}

	// On a cluster a hot holder must be listed; a cold one needs no listing.
	recC, _ := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend, Manifest: &fakeManifest{listed: map[string]bool{}},
		ColdExists: func(_ context.Context, p string) (bool, error) { return cold[p], nil }})
	res, err = recC.Verify(ctx, s, entries, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.HeldCompacted, []string{paths["cold"]}) {
		t.Fatalf("cluster held_compacted = %v, want only the cold holder", res.HeldCompacted)
	}
}

// The spoke deletes a file the hub holds only compacted when, and only when,
// it opted in.
func TestDeleteAfterSync_AcceptCompactedOptIn(t *testing.T) {
	for _, accept := range []bool{false, true} {
		r := newAgentRig(t)
		r.writeFile(t, agentPath, []byte("payload"))
		if _, err := r.agent.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		r.transport.MarkHeldCompacted(DefaultHubID, agentPath)
		r.agent.SetDeleteAfterSyncAcceptCompacted(accept)
		r.agent.SetDeleteAfterSync(0, deleterFor(r, nil), nil)
		res, err := r.agent.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if localExists(t, r, agentPath) == accept {
			t.Fatalf("accept=%v: local exists=%v (deleted=%d kept=%d)", accept, localExists(t, r, agentPath), res.LocalDeleted, res.VerifyKept)
		}
	}
}
