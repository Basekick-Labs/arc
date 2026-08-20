package backup

// Regression tests for #626: DeleteBackup must refuse to run while a backup or
// restore operation holds the manager, rather than racing its storage reads.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// blockingLister wraps a backend and parks ListObjects until released, so a
// test can hold CreateBackup (and therefore m.mu) at a known point.
type blockingLister struct {
	storage.Backend

	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (b *blockingLister) ListObjects(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	b.startOnce.Do(func() { close(b.started) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return b.Backend.(storage.ObjectLister).ListObjects(ctx, prefix)
}

func TestDeleteBackup_RefusesWhileOperationHoldsManager(t *testing.T) {
	data, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("data backend: %v", err)
	}
	t.Cleanup(func() { data.Close() })

	blocked := &blockingLister{
		Backend: data,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	m, err := NewManager(&ManagerConfig{
		DataStorage: blocked,
		BackupPath:  t.TempDir(),
		Logger:      zerolog.Nop(),
	})
	if err != nil {
		t.Fatalf("manager: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		m.CreateBackup(ctx, BackupOptions{}) //nolint:errcheck // completion is all this test needs
	}()

	select {
	case <-blocked.started:
	case <-time.After(5 * time.Second):
		t.Fatal("backup never reached storage discovery")
	}

	// The operation holds m.mu: the delete must refuse immediately, not queue.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.DeleteBackup(ctx, "backup-20260820-120000-deadbeef"); !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("delete during running backup: err = %v, want ErrOperationInProgress", err)
	}

	close(blocked.release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("backup did not finish after release")
	}

	// The lock is released: the delete now proceeds far enough to discover the
	// ID does not exist — a different error, proving the guard did not stick.
	// Fresh context: the first one has been aging across the waits above.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	err = m.DeleteBackup(ctx2, "backup-20260820-120000-deadbeef")
	if err == nil || errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("delete after completion: err = %v, want a not-found error", err)
	}
}
