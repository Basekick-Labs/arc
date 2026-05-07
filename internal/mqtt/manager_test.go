package mqtt

import (
	"context"
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
