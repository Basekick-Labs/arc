package ingest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type recordingFileRegistrar struct {
	calls int
}

func (r *recordingFileRegistrar) RegisterFile(database, measurement, path string, partitionTime time.Time, sizeBytes int64, sha256 string) {
	r.calls++
}

type recordingReplicatedFileReconciler struct {
	calls       int
	path        string
	contentHash string
}

func (r *recordingReplicatedFileReconciler) ReconcileReplicatedFile(path, contentHash string) {
	r.calls++
	r.path = path
	r.contentHash = contentHash
}

func newReplicationTestBuffer(t *testing.T) (*ArrowBuffer, context.Context, context.CancelFunc) {
	t.Helper()

	cfg := &config.IngestConfig{
		MaxBufferSize:       1000000,
		MaxBufferAgeMS:      60000,
		FlushWorkers:        1,
		FlushQueueSize:      4,
		ShardCount:          2,
		Compression:         "none",
		FlushTimeoutSeconds: 5,
	}
	store, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("create local storage: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	buf := NewArrowBuffer(cfg, store, zerolog.Nop())
	t.Cleanup(func() { _ = buf.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return buf, ctx, cancel
}

func replicationTestColumns() map[string][]interface{} {
	return map[string][]interface{}{
		"time":  {time.Now().UTC().UnixMicro()},
		"host":  {"replica-test"},
		"value": {42.0},
	}
}

func TestReplicatedFlushDoesNotRegisterManifestFile(t *testing.T) {
	buf, ctx, _ := newReplicationTestBuffer(t)
	reg := &recordingFileRegistrar{}
	buf.SetFileRegistrar(reg)

	if err := buf.WriteColumnarDirectNoWALReplicated(ctx, "testdb", "cpu", replicationTestColumns(), "test-payload-hash"); err != nil {
		t.Fatalf("write replicated rows: %v", err)
	}
	if err := buf.FlushAll(ctx); err != nil {
		t.Fatalf("flush replicated rows: %v", err)
	}
	if reg.calls != 0 {
		t.Fatalf("replicated flush registered %d manifest files, want 0", reg.calls)
	}
}

func TestReplicatedFlushPublishesReconciliationIdentity(t *testing.T) {
	buf, ctx, _ := newReplicationTestBuffer(t)
	reconciler := &recordingReplicatedFileReconciler{}
	buf.SetReplicatedFileReconciler(reconciler)

	if err := buf.WriteColumnarDirectNoWALReplicated(ctx, "testdb", "cpu", replicationTestColumns(), "test-payload-hash"); err != nil {
		t.Fatalf("write replicated rows: %v", err)
	}
	if err := buf.FlushAll(ctx); err != nil {
		t.Fatalf("flush replicated rows: %v", err)
	}

	wantHash := payloadHashFingerprint([]string{"test-payload-hash"})
	if reconciler.calls != 1 {
		t.Fatalf("reconciliation hook called %d times, want 1", reconciler.calls)
	}
	if reconciler.contentHash != wantHash {
		t.Fatalf("content hash = %q, want %q", reconciler.contentHash, wantHash)
	}
	if !strings.Contains(reconciler.path, "_replicated_"+wantHash+"_") {
		t.Fatalf("replicated path %q does not carry reconciliation identity %q", reconciler.path, wantHash)
	}
}

func TestLocalAndReplicatedFlushesKeepManifestRegistrationSeparate(t *testing.T) {
	buf, ctx, _ := newReplicationTestBuffer(t)
	reg := &recordingFileRegistrar{}
	buf.SetFileRegistrar(reg)

	if err := buf.WriteColumnarDirect(ctx, "testdb", "cpu", replicationTestColumns()); err != nil {
		t.Fatalf("write local rows: %v", err)
	}
	if err := buf.FlushAll(ctx); err != nil {
		t.Fatalf("flush local rows: %v", err)
	}
	if reg.calls != 1 {
		t.Fatalf("local flush registered %d manifest files, want 1", reg.calls)
	}

	if err := buf.WriteColumnarDirectNoWALReplicated(ctx, "testdb", "cpu", replicationTestColumns(), "test-payload-hash"); err != nil {
		t.Fatalf("write replicated rows: %v", err)
	}
	if err := buf.FlushAll(ctx); err != nil {
		t.Fatalf("flush replicated rows: %v", err)
	}
	if reg.calls != 1 {
		t.Fatalf("replicated flush changed manifest registration count to %d, want 1", reg.calls)
	}
}
