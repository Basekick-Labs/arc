package edgesync

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAgentRunRejectsOverlapIssue828(t *testing.T) {
	rig := newAgentRig(t)

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() {
		once.Do(func() { close(release) })
	}
	defer unblock()

	var calls atomic.Int32

	rig.agent.SetNamespaceExcluder(func(
		context.Context,
	) (map[string]struct{}, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil, nil
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := rig.agent.Run(context.Background())
		firstDone <- err
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("first pass did not reach discovery")
	}

	secondDone := make(chan error, 1)
	go func() {
		_, err := rig.agent.Run(context.Background())
		secondDone <- err
	}()

	var secondErr error

	select {
	case secondErr = <-secondDone:
	case <-time.After(10 * time.Second):
		unblock()
		t.Fatal("overlapping pass did not return promptly")
	}

	unblock()

	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first pass failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first pass did not finish")
	}

	if secondErr == nil ||
		!strings.Contains(secondErr.Error(), "already running") {
		t.Fatalf(
			"second run error = %v, want overlap rejection",
			secondErr,
		)
	}

	if calls.Load() != 1 {
		t.Fatalf(
			"discovery calls = %d, want 1",
			calls.Load(),
		)
	}

	if _, err := rig.agent.Run(context.Background()); err != nil {
		t.Fatalf("subsequent pass failed: %v", err)
	}
}

func TestAgentRunReleasesGuardAfterCancellationIssue828(t *testing.T) {
	rig := newAgentRig(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	var first atomic.Bool

	rig.agent.SetNamespaceExcluder(func(
		ctx context.Context,
	) (map[string]struct{}, error) {
		if first.CompareAndSwap(false, true) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return nil, nil
	})

	done := make(chan error, 1)
	go func() {
		_, err := rig.agent.Run(ctx)
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("pass did not reach discovery")
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled pass error = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled pass did not exit")
	}

	if _, err := rig.agent.Run(context.Background()); err != nil {
		t.Fatalf("pass after cancellation failed: %v", err)
	}
}

func TestAgentRunReleasesGuardAfterFailureIssue828(t *testing.T) {
	rig := newAgentRig(t)

	failure := errors.New("independent discovery failure")
	var first atomic.Bool

	rig.agent.SetNamespaceExcluder(func(
		context.Context,
	) (map[string]struct{}, error) {
		if first.CompareAndSwap(false, true) {
			return nil, failure
		}
		return nil, nil
	})

	if _, err := rig.agent.Run(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("first pass error = %v, want discovery failure", err)
	}

	if _, err := rig.agent.Run(context.Background()); err != nil {
		t.Fatalf("pass after failure failed: %v", err)
	}
}
