package ingest

import (
	"context"
	"testing"
	"time"
)

func TestFlushAllAndWaitWaitsForAdmittedAsyncTasks(t *testing.T) {
	buffer := &ArrowBuffer{}
	taskID := buffer.beginAsyncFlushTask()

	finished := make(chan error, 1)
	go func() {
		finished <- buffer.FlushAllAndWait(context.Background())
	}()

	select {
	case err := <-finished:
		t.Fatalf("FlushAllAndWait returned before async task completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	buffer.completeAsyncFlushTask(taskID)
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("FlushAllAndWait: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("FlushAllAndWait did not return after task completion")
	}
}

func TestFlushAllAndWaitHonorsContextWhileWaiting(t *testing.T) {
	buffer := &ArrowBuffer{shards: []*bufferShard{{}}}
	taskID := buffer.beginAsyncFlushTask()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := buffer.FlushAllAndWait(ctx); err == nil {
		t.Fatal("FlushAllAndWait returned nil after its context expired")
	}
	buffer.completeAsyncFlushTask(taskID)

	shardUnlocked := make(chan struct{})
	go func() {
		buffer.shards[0].mu.Lock()
		buffer.shards[0].mu.Unlock()
		close(shardUnlocked)
	}()
	select {
	case <-shardUnlocked:
	case <-time.After(time.Second):
		t.Fatal("FlushAllAndWait left a shard locked after its context expired")
	}
}

func TestResetFlushFailureRequiresUnchangedGeneration(t *testing.T) {
	buffer := &ArrowBuffer{}
	oldGeneration := buffer.FlushFailureGeneration()
	buffer.markFlushFailure()

	if buffer.ResetFlushFailure(oldGeneration) {
		t.Fatal("ResetFlushFailure cleared a failure from a newer generation")
	}
	if !buffer.HasFlushFailure() {
		t.Fatal("newer flush failure was cleared")
	}

	currentGeneration := buffer.FlushFailureGeneration()
	if !buffer.ResetFlushFailure(currentGeneration) {
		t.Fatal("ResetFlushFailure refused an unchanged generation")
	}
	if buffer.HasFlushFailure() {
		t.Fatal("flush failure latch remains set after a matching reset")
	}
}
