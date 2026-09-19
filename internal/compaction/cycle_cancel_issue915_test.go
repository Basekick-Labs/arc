package compaction

import (
	"context"
	"errors"
	"testing"
)

func TestCancelledCycleReturnsContextIssue915(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := manager.RunCompactionCycleForTiers(
		ctx, []string{"hourly"},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}

	if manager.IsCycleRunning() {
		t.Fatal("manager remained busy after cancellation")
	}

	_, err = manager.RunCompactionCycleForTiers(
		context.Background(), []string{"hourly"},
	)
	if err != nil {
		t.Fatalf("next cycle failed: %v", err)
	}
}
