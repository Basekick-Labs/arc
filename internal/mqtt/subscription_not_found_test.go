package mqtt

import (
	"context"
	"errors"
	"testing"
)

// TestManager_UnknownIDIsNotFound: every manager call that takes a
// subscription ID reports an ID with no stored subscription as
// ErrSubscriptionNotFound, which the API answers with 404 instead of 500.
func TestManager_UnknownIDIsNotFound(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	const id = "no-such-subscription"
	newName := "renamed"

	calls := map[string]func() error{
		"Get": func() error { _, err := mgr.Get(ctx, id); return err },
		"Update": func() error {
			_, err := mgr.Update(ctx, id, &UpdateSubscriptionRequest{Name: &newName})
			return err
		},
		"Delete":              func() error { return mgr.Delete(ctx, id) },
		"StartSubscription":   func() error { return mgr.StartSubscription(ctx, id) },
		"StopSubscription":    func() error { return mgr.StopSubscription(ctx, id) },
		"PauseSubscription":   func() error { return mgr.PauseSubscription(ctx, id) },
		"RestartSubscription": func() error { return mgr.RestartSubscription(ctx, id) },
		"GetStats":            func() error { _, err := mgr.GetStats(ctx, id); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if !errors.Is(err, ErrSubscriptionNotFound) {
				t.Fatalf("err = %v, want ErrSubscriptionNotFound", err)
			}
			mgr.mu.Lock()
			_, reserved := mgr.subscribers[id]
			mgr.mu.Unlock()
			if reserved {
				t.Error("a call on an unknown ID left a subscriber slot reserved")
			}
		})
	}
}

// TestManager_StopOrPauseStoppedSubscriptionIsNotRunning: a stored
// subscription that is not running still answers ErrSubscriptionNotRunning
// (409), not ErrSubscriptionNotFound.
func TestManager_StopOrPauseStoppedSubscriptionIsNotRunning(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	sub, err := mgr.Create(ctx, createReq("stopped", "tcp://localhost:1883", false), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mgr.StopSubscription(ctx, sub.ID); !errors.Is(err, ErrSubscriptionNotRunning) {
		t.Errorf("StopSubscription err = %v, want ErrSubscriptionNotRunning", err)
	}
	if err := mgr.PauseSubscription(ctx, sub.ID); !errors.Is(err, ErrSubscriptionNotRunning) {
		t.Errorf("PauseSubscription err = %v, want ErrSubscriptionNotRunning", err)
	}
}

// TestManager_DeletedSubscriptionIsNotFound: once deleted, a subscription's ID
// is unknown to every call, a second delete included.
func TestManager_DeletedSubscriptionIsNotFound(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	sub, err := mgr.Create(ctx, createReq("gone", "tcp://localhost:1883", false), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := mgr.Delete(ctx, sub.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := mgr.Delete(ctx, sub.ID); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Errorf("second Delete err = %v, want ErrSubscriptionNotFound", err)
	}
	if _, err := mgr.Get(ctx, sub.ID); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Errorf("Get after Delete err = %v, want ErrSubscriptionNotFound", err)
	}
}
