package edgesync

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// Verify answers held only for a plain receipt whose own file exists, and
// changes nothing: compacted, removed, gone, different-digest and denied
// entries are neither held nor missing, and no receipt is forgotten or marked.
func TestVerify_HeldOnlyForProvablyHeldFiles(t *testing.T) {
	ctx := context.Background()
	rec, idx, backend := newTestReconcilerWithBackend(t)
	sha := func(s string) string { return sha256Hex([]byte(s)) }
	const (
		plain     = "m/cpu/2026/01/01/00/plain.parquet"
		compacted = "m/cpu/2026/01/01/00/compacted.parquet"
		removed   = "m/cpu/2026/01/01/00/removed.parquet"
		gone      = "m/cpu/2026/01/01/00/gone.parquet"
		other     = "m/cpu/2026/01/01/00/other.parquet"
		denied    = "m/cpu/2026/01/01/00/denied.parquet"
		unknown   = "m/cpu/2026/01/01/00/unknown.parquet"
	)
	for _, p := range []string{plain, compacted, removed, other, denied} {
		seedFile(t, idx, backend, "rocket-01", p, sha(p))
	}
	recordFile(t, idx, "rocket-01", gone, sha(gone)) // receipt, no file
	if err := idx.MarkCompacted(ctx, "rocket-01", []string{compacted}); err != nil {
		t.Fatal(err)
	}
	if err := idx.MarkRemoved(ctx, "rocket-01", []string{removed}, RemovedByRetention); err != nil {
		t.Fatal(err)
	}
	entries := []ReconcileEntry{}
	for _, p := range []string{plain, compacted, removed, gone, denied, unknown} {
		entries = append(entries, ReconcileEntry{Path: p, SHA256: sha(p), SizeBytes: 1})
	}
	entries = append(entries, ReconcileEntry{Path: other, SHA256: sha("different"), SizeBytes: 1})
	allow := func(p string) (bool, error) { return p != denied, nil }

	res, err := rec.Verify(ctx, "rocket-01", entries, allow)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Held, []string{plain}) {
		t.Fatalf("held = %v, want only the plain receipt with its file", res.Held)
	}
	if !slices.Equal(res.Missing, []string{unknown}) {
		t.Fatalf("missing = %v, want only the path with no receipt (a denied entry is never missing)", res.Missing)
	}
	// Nothing changed: the gone receipt is still there and still plain.
	if hf, ok := receiptState(t, idx, "rocket-01", gone); !ok || hf.Vouched() {
		t.Fatalf("verify changed the gone receipt: %+v ok=%v", hf, ok)
	}
}

// On a cluster hub, a plain receipt whose file exists here but which the
// manifest no longer lists is not held; a sync failure fails the verify.
func TestVerify_ClusterManifest(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	backend := newLocalBackendForTest(t, t.TempDir())
	const a, b = "m/cpu/2026/01/01/00/a.parquet", "m/cpu/2026/01/01/00/b.parquet"
	seedFile(t, idx, backend, "rocket-01", a, sha256Hex([]byte(a)))
	seedFile(t, idx, backend, "rocket-01", b, sha256Hex([]byte(b)))
	m := &fakeManifest{listed: map[string]bool{NamespacedPath("rocket-01", a): true}}
	rec, _ := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend, Manifest: m})
	entries := []ReconcileEntry{{Path: a, SHA256: sha256Hex([]byte(a)), SizeBytes: 1}, {Path: b, SHA256: sha256Hex([]byte(b)), SizeBytes: 1}}
	res, err := rec.Verify(ctx, "rocket-01", entries, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Held, []string{a}) || len(res.Missing) != 0 {
		t.Fatalf("held=%v missing=%v, want only the listed file held", res.Held, res.Missing)
	}
	m.syncErr = errors.New("no leader")
	if _, err := rec.Verify(ctx, "rocket-01", entries, nil); !errors.Is(err, ErrReceiveInternal) {
		t.Fatalf("err = %v, want ErrReceiveInternal on a failed catch-up", err)
	}
}
