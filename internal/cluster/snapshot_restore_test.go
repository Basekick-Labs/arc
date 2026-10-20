package cluster

import (
	"context"
	"testing"
	"time"
)

func TestCoordinatorPostSnapshotRestoreHandlerCanRegisterAfterRestore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Coordinator{ctx: ctx}
	called := make(chan context.Context, 2)

	// A snapshot can arrive before main.go has finished constructing the
	// reconciler. The successful barrier leaves one pending notification.
	c.snapshotRestoreGeneration = 1
	c.snapshotRestorePending = true
	c.SetPostSnapshotRestoreHandler(func(got context.Context) {
		called <- got
	})

	select {
	case got := <-called:
		if got != ctx {
			t.Fatal("handler received a different lifecycle context")
		}
	case <-time.After(time.Second):
		t.Fatal("pending snapshot restore was not delivered")
	}

	// Further snapshot restores in this process must not repeat the one-shot
	// storage cleanup once the handler has run.
	c.onSnapshotRestored()
	select {
	case <-called:
		t.Fatal("snapshot restore handler ran more than once")
	case <-time.After(50 * time.Millisecond):
	}
}
