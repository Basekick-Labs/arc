package mqtt

import (
	"context"
	"errors"
	"testing"

	"github.com/rs/zerolog"
)

// idleSubscriber is a Subscriber that was never started: Stop returns at once
// without touching a broker, which lets a test stand it in the manager's map
// as the subscriber a request finds running.
func idleSubscriber(t *testing.T, mgr *SubscriptionManager, sub *Subscription) *Subscriber {
	t.Helper()
	return NewSubscriber(sub, nil, mgr.encryptor, mgr.onStatusChange, zerolog.Nop())
}

// #1597: a stop or pause that took the subscriber just before a concurrent
// Delete removed the row finds no row for its status write. That is the
// subscription being gone, answered as not found, not a store failure.
func TestManager_StopOrPauseOfDeletedRowIsNotFound(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*SubscriptionManager, context.Context, string) error
	}{
		{"StopSubscription", (*SubscriptionManager).StopSubscription},
		{"PauseSubscription", (*SubscriptionManager).PauseSubscription},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mgr := newTestManager(t)
			ctx := context.Background()
			sub, err := mgr.Create(ctx, createReq("racing", "tcp://localhost:1883", false), "")
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			mgr.mu.Lock()
			mgr.subscribers[sub.ID] = idleSubscriber(t, mgr, sub)
			mgr.mu.Unlock()
			// The Delete's row removal, landing after this request took the
			// subscriber and before its status write.
			if err := mgr.repo.Delete(ctx, sub.ID); err != nil {
				t.Fatalf("repo.Delete: %v", err)
			}

			if err := tc.call(mgr, ctx, sub.ID); !errors.Is(err, ErrSubscriptionNotFound) {
				t.Fatalf("err = %v, want ErrSubscriptionNotFound", err)
			}
		})
	}
}

// #1597: Delete removes the row before it stops what runs. A delete whose row
// removal fails therefore leaves the running subscriber in place: the
// subscription is still stored, so it must still be running.
func TestManager_FailedDeleteLeavesTheSubscriberRunning(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	sub, err := mgr.Create(ctx, createReq("kept", "tcp://localhost:1883", false), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	running := idleSubscriber(t, mgr, sub)
	mgr.mu.Lock()
	mgr.subscribers[sub.ID] = running
	mgr.mu.Unlock()

	_ = mgr.repo.db.Close() // every statement now fails
	err = mgr.Delete(ctx, sub.ID)
	if err == nil || errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("Delete err = %v, want the store error", err)
	}
	mgr.mu.Lock()
	got := mgr.subscribers[sub.ID]
	mgr.mu.Unlock()
	if got != running {
		t.Error("a Delete that failed to remove the row stopped the running subscriber")
	}
}

// #1597: a Delete of an ID with a running subscriber but no stored row still
// stops that subscriber, and answers not found.
func TestManager_DeleteStopsASubscriberWithNoRow(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	sub := &Subscription{ID: "orphan", Name: "orphan", Broker: "tcp://localhost:1883", Topics: []string{"a"}, Database: "iot"}
	mgr.mu.Lock()
	mgr.subscribers[sub.ID] = idleSubscriber(t, mgr, sub)
	mgr.mu.Unlock()

	if err := mgr.Delete(ctx, sub.ID); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("Delete err = %v, want ErrSubscriptionNotFound", err)
	}
	mgr.mu.Lock()
	_, left := mgr.subscribers[sub.ID]
	mgr.mu.Unlock()
	if left {
		t.Error("the subscriber with no stored row was left in the map")
	}
}

// #1597: a start that fails because a Delete removed the subscription meanwhile
// (the Delete took its reservation, so the registration failed) answers not
// found. A start that fails with the row still stored keeps its own error and
// records status error.
func TestManager_StartFailedOnDeletedRowIsNotFound(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	sub, err := mgr.Create(ctx, createReq("starting", "tcp://localhost:1883", false), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	startErr := errors.New("connect refused")

	if got := mgr.startFailed(ctx, sub.ID, startErr); got != startErr {
		t.Fatalf("stored row: startFailed = %v, want the start error", got)
	}
	stored, err := mgr.repo.Get(ctx, sub.ID)
	if err != nil || stored == nil || stored.Status != StatusError {
		t.Fatalf("stored row after a failed start: %+v, %v; want status error", stored, err)
	}

	if err := mgr.repo.Delete(ctx, sub.ID); err != nil {
		t.Fatalf("repo.Delete: %v", err)
	}
	if got := mgr.startFailed(ctx, sub.ID, ErrSubscriptionNotRunning); !errors.Is(got, ErrSubscriptionNotFound) {
		t.Fatalf("deleted row: startFailed = %v, want ErrSubscriptionNotFound", got)
	}
}

// reserve puts a start's nil placeholder in the slot for id, as
// StartSubscription and RestartSubscription do before they read the row.
func reserve(mgr *SubscriptionManager, id string) {
	mgr.mu.Lock()
	mgr.subscribers[id] = nil
	mgr.mu.Unlock()
}

// #1597: a start whose reservation a Delete took, after the start had read the
// row, fails to register. Through the real start path it answers not found,
// and nothing is left running.
func TestManager_StartWhoseReservationADeleteTookIsNotFound(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	sub, err := mgr.Create(ctx, createReq("taken", newMockMQTTBroker(t), false), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	reserve(mgr, sub.ID) // the start has reserved and read the row
	if err := mgr.Delete(ctx, sub.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if err := mgr.runReserved(ctx, sub); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("err = %v, want ErrSubscriptionNotFound", err)
	}
	mgr.mu.Lock()
	running := mgr.subscribers[sub.ID]
	mgr.mu.Unlock()
	if running != nil {
		t.Error("a subscriber is running for a deleted subscription")
	}
}

// #1597: a Delete takes start A's reservation after A read the row, and a
// second start reserves the slot again before A registers. A then registers
// into the second start's placeholder. Its re-read finds no row, so it stops
// the subscriber and answers not found instead of running it with nothing
// stored.
func TestManager_StartRegisteringIntoAnotherReservationOfADeletedRowIsNotFound(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	sub, err := mgr.Create(ctx, createReq("hijacked", newMockMQTTBroker(t), false), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	reserve(mgr, sub.ID) // start A has reserved and read the row
	if err := mgr.Delete(ctx, sub.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	reserve(mgr, sub.ID) // start B reserves the slot again

	if err := mgr.runReserved(ctx, sub); !errors.Is(err, ErrSubscriptionNotFound) {
		t.Fatalf("start A err = %v, want ErrSubscriptionNotFound", err)
	}
	mgr.mu.Lock()
	running := mgr.subscribers[sub.ID]
	mgr.mu.Unlock()
	if running != nil {
		t.Error("start A left a subscriber running for a deleted subscription")
	}
}

// #1597: StartSubscription reserves its slot before it reads the row. Read
// first, a Delete could complete between the read and the reservation and the
// start would run a subscriber with no stored row. Observed with a stored
// subscription whose slot is already reserved and a store that fails every
// read: the start is refused as already running without reading the row.
func TestManager_StartReservesBeforeReading(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()
	sub, err := mgr.Create(ctx, createReq("busy", "tcp://localhost:1883", false), "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	reserve(mgr, sub.ID)
	_ = mgr.repo.db.Close() // a read would fail

	if err := mgr.StartSubscription(ctx, sub.ID); !errors.Is(err, ErrSubscriptionAlreadyRunning) {
		t.Fatalf("err = %v, want ErrSubscriptionAlreadyRunning before any read", err)
	}
}
