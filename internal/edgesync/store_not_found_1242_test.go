package edgesync

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// hubBucketNotCreatedYet answers Exists the way an S3 hub does before its
// bucket exists (#1242); the staging write is what creates it.
type hubBucketNotCreatedYet struct{ storage.Backend }

func (hubBucketNotCreatedYet) Exists(context.Context, string) (bool, error) {
	return false, fmt.Errorf("failed to check S3 object existence: bucket hub: %w", storage.ErrStoreNotFound)
}

func TestReceiverCommitsIntoAHubBucketNotCreatedYet(t *testing.T) {
	local := newLocalBackendForTest(t, t.TempDir())
	r, err := NewReceiver(ReceiverConfig{Backend: hubBucketNotCreatedYet{local}, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("parquet payload bytes")
	res, err := r.Receive(context.Background(), "rocket-01", testPath, sha256Hex(content), int64(len(content)), 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("receive into a hub bucket not created yet: %v", err)
	}
	if res.Outcome != OutcomeCommitted {
		t.Fatalf("outcome = %q, want committed", res.Outcome)
	}
}

func TestReconcileReportsMissingFromAHubBucketNotCreatedYet(t *testing.T) {
	idx := newTestHubIndex(t)
	local := newLocalBackendForTest(t, t.TempDir())
	rec, err := NewReconciler(ReconcilerConfig{Index: idx, Backend: hubBucketNotCreatedYet{local}})
	if err != nil {
		t.Fatal(err)
	}
	p := "metrics/cpu/2026/08/07/14/a.parquet"
	sha := sha256Hex([]byte("a"))
	recordFile(t, idx, "rocket-01", p, sha)

	res, err := rec.Reconcile(context.Background(), "rocket-01", []ReconcileEntry{{Path: p, SHA256: sha}})
	if err != nil {
		t.Fatalf("reconcile against a hub bucket not created yet: %v", err)
	}
	if len(res.Present) != 0 {
		t.Fatalf("present = %v, want none: the hub holds no bytes", res.Present)
	}
}
