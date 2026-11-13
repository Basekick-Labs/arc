package edgesync

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// fakeManifest is a cluster manifest listing the given hub paths.
type fakeManifest struct {
	listed  map[string]bool
	syncErr error
	syncs   int
}

func (m *fakeManifest) Sync(context.Context) error { m.syncs++; return m.syncErr }
func (m *fakeManifest) Lists(p string) bool        { return m.listed[p] }

// On a cluster hub a receipt whose file is gone is forgotten only while the
// manifest still lists the file (a missing local copy: the re-send heals it).
// A file the manifest no longer lists was removed on another node: its
// receipt is marked removed and stays present, so the spoke does not
// resurrect it (#1667). Before the fix both were forgotten.
func TestReconcile_ClusterManifestDecidesStaleReceipts(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	backend := newLocalBackendForTest(t, t.TempDir())
	const lost, removed = "metrics/cpu/2026/01/01/00/lost.parquet", "metrics/cpu/2026/01/01/00/removed.parquet"
	shaL, shaR := sha256Hex([]byte("l")), sha256Hex([]byte("r"))
	recordFile(t, idx, "rocket-01", lost, shaL) // files never written: both gone from storage
	recordFile(t, idx, "rocket-01", removed, shaR)
	m := &fakeManifest{listed: map[string]bool{NamespacedPath("rocket-01", lost): true}}
	rec, err := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend, Manifest: m})
	if err != nil {
		t.Fatal(err)
	}

	res, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: lost, SHA256: shaL, SizeBytes: 1}, {Path: removed, SHA256: shaR, SizeBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Missing) != 1 || res.Missing[0] != lost || len(res.Present) != 1 || res.Present[0] != removed {
		t.Fatalf("missing=%v present=%v, want the lost copy missing and the removed file present", res.Missing, res.Present)
	}
	if m.syncs != 1 {
		t.Errorf("manifest synced %d times, want once before forgetting", m.syncs)
	}
	if _, ok := receiptState(t, idx, "rocket-01", lost); ok {
		t.Error("the receipt of a file the manifest still lists was kept")
	}
	if hf, ok := receiptState(t, idx, "rocket-01", removed); !ok || !hf.Removed {
		t.Errorf("receipt of a file the manifest dropped = %+v ok=%v, want kept and removed", hf, ok)
	}

	// Off a cluster (no manifest) both are forgotten, as before.
	idx2 := newTestHubIndex(t)
	recordFile(t, idx2, "rocket-01", removed, shaR)
	rec2, _ := NewReconciler(ReconcilerConfig{Index: idx2, Backend: backend})
	if res, err := rec2.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: removed, SHA256: shaR, SizeBytes: 1}}); err != nil || len(res.Missing) != 1 {
		t.Fatalf("standalone: %v %v, want missing", res, err)
	}
}

// A manifest that cannot be caught up with forgets nothing: the reconcile
// fails and the spoke retries, rather than re-sending deleted content.
func TestReconcile_ManifestSyncFailureForgetsNothing(t *testing.T) {
	ctx := context.Background()
	idx := newTestHubIndex(t)
	backend := newLocalBackendForTest(t, t.TempDir())
	const p = "metrics/cpu/2026/01/01/00/x.parquet"
	sha := sha256Hex([]byte("x"))
	recordFile(t, idx, "rocket-01", p, sha)
	rec, _ := NewReconciler(ReconcilerConfig{Index: idx, Backend: backend, Manifest: &fakeManifest{syncErr: errors.New("no leader")}})
	if _, err := rec.Reconcile(ctx, "rocket-01", []ReconcileEntry{{Path: p, SHA256: sha, SizeBytes: 1}}); !errors.Is(err, ErrReceiveInternal) {
		t.Fatalf("err = %v, want ErrReceiveInternal", err)
	}
	if hf, ok := receiptState(t, idx, "rocket-01", p); !ok || hf.Vouched() {
		t.Fatalf("receipt = %+v ok=%v, want untouched", hf, ok)
	}
}

// Receive on a cluster hub: a stale bundle re-sending a file removed on
// another node is answered already-present; a file whose local copy is
// missing but which the manifest lists is accepted again.
func TestReceiver_ClusterManifestBlocksResurrection(t *testing.T) {
	ctx := context.Background()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	idx := newTestHubIndex(t)
	m := &fakeManifest{listed: map[string]bool{}}
	r, err := NewReceiver(ReceiverConfig{Backend: backend, Index: idx, Manifest: m, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("payload")
	digest := sha256Hex(content)
	if res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content)); err != nil || res.Outcome != OutcomeCommitted {
		t.Fatalf("first receive: %v %v", res, err)
	}
	// Removed on the writer: gone from storage and from the manifest.
	if err := backend.Delete(ctx, "rocket-01/"+testPath); err != nil {
		t.Fatal(err)
	}
	res, err := r.Receive(ctx, "rocket-01", testPath, digest, int64(len(content)), 0, bytes.NewReader(content))
	if err != nil || res.Outcome != OutcomeAlreadyPresent {
		t.Fatalf("re-receive of a removed file = %v %v, want already-present", res, err)
	}
	if present, _ := backend.Exists(ctx, "rocket-01/"+testPath); present {
		t.Fatal("content removed cluster-wide was accepted again")
	}
	if hf, _ := receiptState(t, idx, "rocket-01", testPath); !hf.Removed {
		t.Fatal("receipt not marked removed")
	}

	// A path the manifest still lists, missing here: accepted, receipt clean.
	const other = "metrics/cpu/2026/08/07/14/other.parquet"
	if res, err := r.Receive(ctx, "rocket-01", other, digest, int64(len(content)), 0, bytes.NewReader(content)); err != nil || res.Outcome != OutcomeCommitted {
		t.Fatalf("receive other: %v %v", res, err)
	}
	m.listed[NamespacedPath("rocket-01", other)] = true
	if err := backend.Delete(ctx, "rocket-01/"+other); err != nil {
		t.Fatal(err)
	}
	if res, err := r.Receive(ctx, "rocket-01", other, digest, int64(len(content)), 0, bytes.NewReader(content)); err != nil || res.Outcome != OutcomeCommitted {
		t.Fatalf("re-receive of a lost local copy = %v %v, want committed", res, err)
	}
}

// SettleMissing settles a whole set with one manifest catch-up: the import
// path's pre-pass, instead of one Raft barrier per bundle entry.
func TestReceiver_SettleMissingOneSyncForTheSet(t *testing.T) {
	ctx := context.Background()
	backend, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	idx := newTestHubIndex(t)
	m := &fakeManifest{listed: map[string]bool{}}
	r, err := NewReceiver(ReceiverConfig{Backend: backend, Index: idx, Manifest: m, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for i := 0; i < 5; i++ {
		p := fmt.Sprintf("metrics/cpu/2026/01/01/00/f%d.parquet", i)
		recordFile(t, idx, "rocket-01", p, sha256Hex([]byte(p))) // files absent from storage
		paths = append(paths, p)
	}
	m.listed[NamespacedPath("rocket-01", paths[0])] = true // one lost local copy
	if err := r.SettleMissing(ctx, "rocket-01", paths); err != nil {
		t.Fatal(err)
	}
	if m.syncs != 1 {
		t.Fatalf("manifest synced %d times for 5 missing files, want 1", m.syncs)
	}
	if _, ok := receiptState(t, idx, "rocket-01", paths[0]); ok {
		t.Error("the receipt of a lost local copy was kept")
	}
	for _, p := range paths[1:] {
		if hf, ok := receiptState(t, idx, "rocket-01", p); !ok || !hf.Removed {
			t.Errorf("%s: %+v ok=%v, want marked removed", p, hf, ok)
		}
	}
}
