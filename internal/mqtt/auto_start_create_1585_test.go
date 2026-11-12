package mqtt

import (
	"context"
	"fmt"
	"net"
	"testing"
)

// closedBrokerURL returns a broker address nothing listens on, so a connect
// fails at once with connection refused.
func closedBrokerURL(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return fmt.Sprintf("tcp://%s", addr)
}

func createReq(name, broker string, autoStart bool) *CreateSubscriptionRequest {
	return &CreateSubscriptionRequest{
		Name:      name,
		Broker:    broker,
		ClientID:  name,
		Topics:    []string{"sensors/#"},
		Database:  "iot",
		AutoStart: autoStart,
	}
}

// TestManager_Create_AutoStartStartsTheSubscription is the #1585 regression:
// a subscription created with auto_start runs at once, and the create
// reports it running, instead of staying stopped until a server restart.
func TestManager_Create_AutoStartStartsTheSubscription(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = mgr.Shutdown(ctx) })

	sub, err := mgr.Create(ctx, createReq("auto", newMockMQTTBroker(t), true), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if sub.Status != StatusRunning {
		t.Errorf("returned status = %q, want %q", sub.Status, StatusRunning)
	}
	mgr.mu.Lock()
	running := mgr.subscribers[sub.ID]
	mgr.mu.Unlock()
	if running == nil {
		t.Fatal("subscription created with auto_start is not running (#1585)")
	}
	got, err := mgr.repo.Get(ctx, sub.ID)
	if err != nil {
		t.Fatalf("repo.Get: %v", err)
	}
	if got.Status != StatusRunning {
		t.Errorf("stored status = %q, want %q", got.Status, StatusRunning)
	}
}

// TestManager_Create_AutoStartFailureKeepsTheSubscription pins what a start
// failure means for the create: the subscription is kept, with status error
// and the reason, and the create itself succeeds.
func TestManager_Create_AutoStartFailureKeepsTheSubscription(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	sub, err := mgr.Create(ctx, createReq("auto-bad", closedBrokerURL(t), true), "")
	if err != nil {
		t.Fatalf("Create failed because the start failed: %v", err)
	}
	if sub.Status != StatusError || sub.ErrorMessage == "" {
		t.Errorf("returned status = %q, error %q; want %q with a reason", sub.Status, sub.ErrorMessage, StatusError)
	}
	got, err := mgr.repo.Get(ctx, sub.ID)
	if err != nil || got == nil {
		t.Fatalf("subscription not kept after a failed start: %v", err)
	}
	if got.Status != StatusError || got.ErrorMessage == "" {
		t.Errorf("stored status = %q, error %q; want %q with a reason", got.Status, got.ErrorMessage, StatusError)
	}
	mgr.mu.Lock()
	_, reserved := mgr.subscribers[sub.ID]
	mgr.mu.Unlock()
	if reserved {
		t.Error("a failed start left its slot reserved; POST /start would answer already running")
	}
}

// TestManager_Create_WithoutAutoStartStaysStopped: without auto_start nothing
// connects.
func TestManager_Create_WithoutAutoStartStaysStopped(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	sub, err := mgr.Create(ctx, createReq("manual", newMockMQTTBroker(t), false), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if sub.Status != StatusStopped {
		t.Errorf("status = %q, want %q", sub.Status, StatusStopped)
	}
	mgr.mu.Lock()
	_, started := mgr.subscribers[sub.ID]
	mgr.mu.Unlock()
	if started {
		t.Error("subscription without auto_start was started")
	}
}
