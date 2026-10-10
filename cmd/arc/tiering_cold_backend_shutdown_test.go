package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/shutdown"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type shutdownTrackingBackend struct {
	storage.Backend
	closeCalls int
	onClose    func()
}

func (b *shutdownTrackingBackend) Close() error {
	b.closeCalls++
	if b.onClose != nil {
		b.onClose()
	}
	return nil
}

func TestTieringColdBackendClosesAfterManager(t *testing.T) {
	coordinator := shutdown.New(5*time.Second, zerolog.Nop())
	var order []string
	backend := &shutdownTrackingBackend{
		onClose: func() { order = append(order, "cold-backend") },
	}

	registerTieringColdBackendShutdown(coordinator, backend)
	registerTieringShutdown(coordinator, func() error {
		order = append(order, "tiering")
		if backend.closeCalls != 0 {
			t.Error("cold backend closed while the tiering manager was still stopping")
		}
		return nil
	})

	if err := coordinator.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if backend.closeCalls != 1 {
		t.Fatalf("cold backend Close() calls = %d, want 1", backend.closeCalls)
	}
	if want := []string{"tiering", "cold-backend"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("shutdown order = %v, want %v", order, want)
	}
}

func TestTieringColdBackendClosesWhenManagerCreationFails(t *testing.T) {
	coordinator := shutdown.New(5*time.Second, zerolog.Nop())
	backend := &shutdownTrackingBackend{}
	registerTieringColdBackendShutdown(coordinator, backend)

	// A failed tiering.Manager construction means there is no manager component
	// to register, but the backend was already created and still needs closing.
	if err := coordinator.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if backend.closeCalls != 1 {
		t.Fatalf("cold backend Close() calls = %d, want 1", backend.closeCalls)
	}
}

func TestTieringColdBackendShutdownIgnoresNilBackend(t *testing.T) {
	coordinator := shutdown.New(5*time.Second, zerolog.Nop())
	registerTieringColdBackendShutdown(coordinator, nil)

	if err := coordinator.Shutdown(); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
}
