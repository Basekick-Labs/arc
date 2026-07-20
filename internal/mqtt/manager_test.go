package mqtt

import (
	"context"
	"errors"
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

	repo, err := NewRepository(dbPath, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
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
	repo, err := NewRepository(dbPath, nil, zerolog.Nop())
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
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
