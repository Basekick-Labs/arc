package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestEdgeSyncManualOverlapReturnsConflictIssue828(t *testing.T) {
	rig := newSpokeRig(t)

	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)

	var once sync.Once
	unblock := func() {
		once.Do(func() {
			close(release)
		})
	}
	defer unblock()

	rig.agent.SetNamespaceExcluder(func(
		ctx context.Context,
	) (map[string]struct{}, error) {
		select {
		case <-started:
		default:
			close(started)
		}

		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	go func() {
		_, err := rig.agent.Run(context.Background())
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("first sync pass did not reach discovery")
	}

	resp, body := rig.do(
		t,
		http.MethodPost,
		"/api/v1/spoke-sync/run",
	)

	unblock()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first sync pass failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first sync pass did not finish")
	}

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf(
			"HTTP status = %d, want 409; body=%v",
			resp.StatusCode,
			body,
		)
	}

	message, ok := body["error"].(string)
	if !ok || !strings.Contains(message, "already running") {
		t.Fatalf("conflict response = %v", body)
	}

	// Remove the blocking hook before confirming the guard was released.
	rig.agent.SetNamespaceExcluder(nil)

	retry, retryBody := rig.do(
		t,
		http.MethodPost,
		"/api/v1/spoke-sync/run",
	)

	if retry.StatusCode != http.StatusOK {
		t.Fatalf(
			"HTTP retry status = %d, want 200; body=%v",
			retry.StatusCode,
			retryBody,
		)
	}
}

func TestEdgeSyncManualIndependentFailureRemains503Issue828(t *testing.T) {
	rig := newSpokeRig(t)

	failure := errors.New("independent discovery failure")

	rig.agent.SetNamespaceExcluder(func(
		context.Context,
	) (map[string]struct{}, error) {
		return nil, failure
	})

	resp, body := rig.do(
		t,
		http.MethodPost,
		"/api/v1/spoke-sync/run",
	)

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf(
			"HTTP status = %d, want 503; body=%v",
			resp.StatusCode,
			body,
		)
	}

	message, ok := body["error"].(string)
	if !ok || !strings.Contains(message, failure.Error()) {
		t.Fatalf("failure response = %v", body)
	}
}
