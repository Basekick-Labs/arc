package mqtt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
)

// TestSubscriptionManager_GetAllStats_NilSubscriber verifies that GetAllStats
// returns the DB-fallback SubscriptionStats for a known ID whose entry in the
// subscribers map is nil (e.g. a startup placeholder), instead of panicking
// when dereferencing the nil pointer to call GetStats().
func TestSubscriptionManager_GetAllStats_NilSubscriber(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "mqtt.db")

	repo, err := NewSQLiteRepository(dbPath, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSQLiteRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })

	encryptor, err := NewPasswordEncryptor(nil)
	if err != nil {
		t.Fatalf("NewPasswordEncryptor: %v", err)
	}

	mgr := NewSubscriptionManager(repo, encryptor, nil, zerolog.Nop())

	// Persist one subscription so List() returns it, and inject a nil
	// placeholder into subscribers (mimics an in-progress start).
	sub := &Subscription{
		Name:     "test-sub",
		Broker:   "tcp://localhost:1883",
		ClientID: "test-client",
		Topics:   []string{"sensors/#"},
		QoS:      1,
		Database: "iot",
	}
	sub.SetDefaults()
	if err := repo.Create(context.Background(), sub); err != nil {
		t.Fatalf("repo.Create: %v", err)
	}

	mgr.mu.Lock()
	mgr.subscribers[sub.ID] = nil
	mgr.mu.Unlock()

	stats, err := mgr.GetAllStats(context.Background())
	if err != nil {
		t.Fatalf("GetAllStats: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("len(stats): got %d, want 1", len(stats))
	}
	got := stats[0]
	if got.ID != sub.ID {
		t.Errorf("ID: got %q, want %q", got.ID, sub.ID)
	}
	if got.Name != sub.Name {
		t.Errorf("Name: got %q, want %q", got.Name, sub.Name)
	}
	if got.Status != string(sub.Status) {
		t.Errorf("Status: got %q, want %q (DB fallback)", got.Status, sub.Status)
	}
}

// newTestManager builds a manager backed by a temp SQLite repo (no encryption).
func newTestManager(t *testing.T) *SubscriptionManager {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "mqtt.db")
	repo, err := NewSQLiteRepository(dbPath, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewSQLiteRepository: %v", err)
	}
	t.Cleanup(func() { _ = repo.Close() })
	encryptor, err := NewPasswordEncryptor(nil)
	if err != nil {
		t.Fatalf("NewPasswordEncryptor: %v", err)
	}
	return NewSubscriptionManager(repo, encryptor, nil, zerolog.Nop())
}

// TestManager_Create_QoSZeroPersisted is the end-to-end #326 regression: an
// explicit QoS 0 survives Create + the repository round-trip as 0, and an
// omitted QoS becomes the default 1.
func TestManager_Create_QoSZeroPersisted(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	zero := 0
	sub, err := mgr.Create(ctx, &CreateSubscriptionRequest{
		Name:     "qos0",
		Broker:   "tcp://localhost:1883",
		ClientID: "c0",
		Topics:   []string{"sensors/#"},
		QoS:      &zero,
		Database: "iot",
	}, "")
	if err != nil {
		t.Fatalf("Create with QoS 0: %v", err)
	}
	if sub.QoS != 0 {
		t.Errorf("Create rewrote explicit QoS 0 to %d (#326)", sub.QoS)
	}
	// Read it back from the repo to prove persistence, not just the in-memory value.
	got, err := mgr.repo.Get(ctx, sub.ID)
	if err != nil {
		t.Fatalf("repo.Get: %v", err)
	}
	if got.QoS != 0 {
		t.Errorf("persisted QoS = %d, want 0", got.QoS)
	}

	// Omitted QoS → default 1.
	sub2, err := mgr.Create(ctx, &CreateSubscriptionRequest{
		Name:     "qosdef",
		Broker:   "tcp://localhost:1883",
		ClientID: "cd",
		Topics:   []string{"sensors/#"},
		Database: "iot",
	}, "")
	if err != nil {
		t.Fatalf("Create with omitted QoS: %v", err)
	}
	if sub2.QoS != 1 {
		t.Errorf("omitted QoS = %d, want default 1", sub2.QoS)
	}
}

// TestManager_Create_InvalidQoSIsValidationError verifies an out-of-range QoS
// surfaces as ErrValidation so the handler can map it to 400 (not 500).
func TestManager_Create_InvalidQoSIsValidationError(t *testing.T) {
	mgr := newTestManager(t)
	three := 3
	_, err := mgr.Create(context.Background(), &CreateSubscriptionRequest{
		Name:     "bad",
		Broker:   "tcp://localhost:1883",
		ClientID: "cb",
		Topics:   []string{"sensors/#"},
		QoS:      &three,
		Database: "iot",
	}, "")
	if err == nil {
		t.Fatal("expected error for QoS 3, got nil")
	}
	if !errors.Is(err, ErrValidation) {
		t.Errorf("expected ErrValidation, got %v", err)
	}
}

// TestManager_RestartSubscription_AlreadyRunning verifies that calling
// RestartSubscription when a slot is already reserved (in-progress start/restart)
// returns ErrSubscriptionAlreadyRunning (#301).
func TestManager_RestartSubscription_AlreadyRunning(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	sub := &Subscription{
		Name:     "test-sub",
		Broker:   "tcp://localhost:1883",
		ClientID: "test-client",
		Topics:   []string{"sensors/#"},
		QoS:      1,
		Database: "iot",
	}
	sub.SetDefaults()
	if err := mgr.repo.Create(ctx, sub); err != nil {
		t.Fatalf("repo.Create: %v", err)
	}

	// Inject nil placeholder to simulate in-flight start or restart
	mgr.mu.Lock()
	mgr.subscribers[sub.ID] = nil
	mgr.mu.Unlock()

	err := mgr.RestartSubscription(ctx, sub.ID)
	if !errors.Is(err, ErrSubscriptionAlreadyRunning) {
		t.Fatalf("expected ErrSubscriptionAlreadyRunning, got %v", err)
	}
}

// TestManager_RestartSubscription_PlaceholderCleanedOnNotFound verifies that
// if the subscription does not exist in the repository, the reserved slot
// placeholder is cleaned up so future attempts are not locked out (#301).
func TestManager_RestartSubscription_PlaceholderCleanedOnNotFound(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	err := mgr.RestartSubscription(ctx, "nonexistent-id")
	if err == nil {
		t.Fatal("expected error for nonexistent subscription, got nil")
	}

	mgr.mu.RLock()
	_, exists := mgr.subscribers["nonexistent-id"]
	mgr.mu.RUnlock()

	if exists {
		t.Errorf("expected placeholder to be cleaned up from subscribers map, but it exists")
	}
}

// TestManager_RestartSubscription_PlaceholderCleanedOnStartFailure verifies the
// reserved slot is released when the new subscriber fails to start, so the
// subscription is not locked out (#301).
func TestManager_RestartSubscription_PlaceholderCleanedOnStartFailure(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	sub := &Subscription{
		Name:       "test-sub",
		Broker:     "ssl://localhost:8883",
		ClientID:   "test-client",
		Topics:     []string{"sensors/#"},
		QoS:        1,
		Database:   "iot",
		TLSEnabled: true,
		TLSCAPath:  filepath.Join(t.TempDir(), "missing-ca.pem"),
	}
	sub.SetDefaults()
	if err := mgr.repo.Create(ctx, sub); err != nil {
		t.Fatalf("repo.Create: %v", err)
	}

	if err := mgr.RestartSubscription(ctx, sub.ID); err == nil {
		t.Fatal("expected start failure, got nil")
	}

	mgr.mu.RLock()
	_, exists := mgr.subscribers[sub.ID]
	mgr.mu.RUnlock()
	if exists {
		t.Fatal("placeholder left in subscribers map after failed start; subscription would be locked out")
	}

	got, err := mgr.repo.Get(ctx, sub.ID)
	if err != nil || got == nil {
		t.Fatalf("repo.Get: %v", err)
	}
	if got.Status != StatusError {
		t.Errorf("status = %q, want %q", got.Status, StatusError)
	}

	if err := mgr.RestartSubscription(ctx, sub.ID); errors.Is(err, ErrSubscriptionAlreadyRunning) {
		t.Fatalf("second restart rejected as already running: %v", err)
	}
}

// newMockMQTTBroker creates an in-process TCP listener that responds to MQTT
// 3.1.1 CONNECT, SUBSCRIBE, and UNSUBSCRIBE control packets, allowing live Subscribers
// to connect and disconnect in sub-millisecond tests without a real broker.
func newMockMQTTBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					if err != nil || n == 0 {
						return
					}
					pktType := buf[0] & 0xF0
					switch pktType {
					case 0x10: // CONNECT -> reply CONNACK
						_, _ = c.Write([]byte{0x20, 0x02, 0x00, 0x00})
					case 0x80: // SUBSCRIBE -> reply SUBACK
						if n >= 4 {
							_, _ = c.Write([]byte{0x90, 0x03, buf[2], buf[3], 0x01})
						}
					case 0xA0: // UNSUBSCRIBE -> reply UNSUBACK
						if n >= 4 {
							_, _ = c.Write([]byte{0xB0, 0x02, buf[2], buf[3]})
						}
					case 0xC0: // PINGREQ -> reply PINGRESP
						_, _ = c.Write([]byte{0xD0, 0x00})
					case 0xE0: // DISCONNECT
						return
					}
				}
			}(conn)
		}
	}()

	return fmt.Sprintf("tcp://%s", ln.Addr().String())
}

// TestManager_StartSubscriber_AbortsIfReservationDeleted verifies that if
// Delete or Shutdown removes the placeholder while startSubscriber is connecting,
// startSubscriber aborts, stops the live subscriber, and does not install it
// into the subscribers map (#770).
func TestManager_StartSubscriber_AbortsIfReservationDeleted(t *testing.T) {
	mgr := newTestManager(t)
	brokerURL := newMockMQTTBroker(t)

	sub := &Subscription{
		ID:       "sub-cas-del",
		Name:     "test-sub-del",
		Broker:   brokerURL,
		ClientID: "test-client-del",
		Topics:   []string{"sensors/#"},
		QoS:      1,
		Database: "iot",
	}
	sub.SetDefaults()

	// Simulate reservation having been deleted while startSubscriber was connecting.
	// Slot is missing from subscribers map.
	err := mgr.startSubscriber(sub)
	if !errors.Is(err, ErrSubscriptionNotRunning) {
		t.Fatalf("expected ErrSubscriptionNotRunning, got %v", err)
	}

	mgr.mu.RLock()
	installed, exists := mgr.subscribers[sub.ID]
	mgr.mu.RUnlock()

	if exists || installed != nil {
		t.Fatalf("orphaned subscriber was installed into subscribers map despite missing reservation")
	}
}

// TestManager_StartSubscriber_AbortsIfReservationReplaced verifies that if
// another subscriber is present in the slot when startSubscriber attempts to
// install, it aborts, stops the new subscriber, and preserves the existing one (#770).
func TestManager_StartSubscriber_AbortsIfReservationReplaced(t *testing.T) {
	mgr := newTestManager(t)
	brokerURL := newMockMQTTBroker(t)

	sub := &Subscription{
		ID:       "sub-cas-replaced",
		Name:     "test-sub-replaced",
		Broker:   brokerURL,
		ClientID: "test-client-rep",
		Topics:   []string{"sensors/#"},
		QoS:      1,
		Database: "iot",
	}
	sub.SetDefaults()

	existing := &Subscriber{id: sub.ID, config: sub, logger: zerolog.Nop()}
	mgr.mu.Lock()
	mgr.subscribers[sub.ID] = existing
	mgr.mu.Unlock()

	err := mgr.startSubscriber(sub)
	if !errors.Is(err, ErrSubscriptionNotRunning) {
		t.Fatalf("expected ErrSubscriptionNotRunning, got %v", err)
	}

	mgr.mu.RLock()
	current := mgr.subscribers[sub.ID]
	mgr.mu.RUnlock()

	if current != existing {
		t.Fatalf("existing subscriber was overwritten: got %v, want %v", current, existing)
	}
}

// TestManager_Start_PlaceholderCleanedOnFailure verifies that during boot
// auto-start, if startSubscriber fails, the reserved slot is cleaned up and
// status is set to StatusError (#770).
func TestManager_Start_PlaceholderCleanedOnFailure(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	sub := &Subscription{
		Name:       "auto-start-sub",
		Broker:     "ssl://localhost:8883",
		ClientID:   "test-client-auto",
		Topics:     []string{"sensors/#"},
		QoS:        1,
		Database:   "iot",
		AutoStart:  true,
		TLSEnabled: true,
		TLSCAPath:  filepath.Join(t.TempDir(), "nonexistent-ca.pem"),
	}
	sub.SetDefaults()
	if err := mgr.repo.Create(ctx, sub); err != nil {
		t.Fatalf("repo.Create: %v", err)
	}

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("mgr.Start: %v", err)
	}

	mgr.mu.RLock()
	_, exists := mgr.subscribers[sub.ID]
	mgr.mu.RUnlock()

	if exists {
		t.Fatal("placeholder left in subscribers map after failed auto-start; subscription would be locked out")
	}

	got, err := mgr.repo.Get(ctx, sub.ID)
	if err != nil || got == nil {
		t.Fatalf("repo.Get: %v", err)
	}
	if got.Status != StatusError {
		t.Errorf("status = %q, want %q", got.Status, StatusError)
	}
}

// TestManager_Start_SkipsExistingSubscriber verifies that if a subscriber is
// already installed or starting in the slot, Start will not overwrite it (#770).
func TestManager_Start_SkipsExistingSubscriber(t *testing.T) {
	mgr := newTestManager(t)
	ctx := context.Background()

	sub := &Subscription{
		Name:      "auto-start-sub-existing",
		Broker:    "tcp://localhost:1883",
		ClientID:  "test-client-existing",
		Topics:    []string{"sensors/#"},
		QoS:       1,
		Database:  "iot",
		AutoStart: true,
	}
	sub.SetDefaults()
	if err := mgr.repo.Create(ctx, sub); err != nil {
		t.Fatalf("repo.Create: %v", err)
	}

	// Pre-install an existing subscriber in the slot.
	existingSub := &Subscriber{}
	mgr.subscribers[sub.ID] = existingSub

	if err := mgr.Start(ctx); err != nil {
		t.Fatalf("mgr.Start: %v", err)
	}

	mgr.mu.RLock()
	cur := mgr.subscribers[sub.ID]
	mgr.mu.RUnlock()

	if cur != existingSub {
		t.Fatalf("subscribers[%s] was overwritten; got %p, want %p", sub.ID, cur, existingSub)
	}
}
