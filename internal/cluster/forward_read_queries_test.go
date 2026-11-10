package cluster

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/rs/zerolog"
)

// Every build that forwards reads from a compactor also accepts them, and
// says so at join (#1567).
func TestForwardReadQueriesCapabilityIsAdvertised(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if !hasCapability(c.localCapabilities(), CapabilityForwardReadQueries) {
		t.Fatalf("capabilities %v lack %s", c.localCapabilities(), CapabilityForwardReadQueries)
	}
}

// Without Raft there is no record of a peer's capabilities: never assume one,
// the query runs locally instead.
func TestPeerReadQueriesWithoutRaftIsFalse(t *testing.T) {
	c := &Coordinator{}
	if c.peerReadQueries("anyone") {
		t.Fatal("a peer was assumed to accept a forwarded Arrow query with no Raft state")
	}
}

// RouteReadQuery forwards only to capable peers, and a node that can serve
// queries never forwards.
func TestRouteReadQueryNeedsACapablePeer(t *testing.T) {
	local := NewNode("local", "Local", RoleCompactor, "c")
	registry := NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	reader := NewNode("reader", "Reader", RoleReader, "c")
	reader.UpdateState(StateHealthy)
	reader.APIAddress = "127.0.0.1:1"
	if err := registry.Register(reader); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://x/api/v1/query/arrow", nil)
	for _, peer := range []func(string) bool{nil, func(string) bool { return false }} {
		r := NewRouter(&RouterConfig{Registry: registry, LocalNode: local, Logger: zerolog.Nop(), PeerReadQueries: peer})
		if _, err := r.RouteReadQuery(context.Background(), req); !errors.Is(err, ErrNoCapablePeer) {
			t.Fatalf("err = %v, want ErrNoCapablePeer", err)
		}
	}
	readerLocal := NewNode("rl", "RL", RoleReader, "c")
	r := NewRouter(&RouterConfig{Registry: registry, LocalNode: readerLocal, Logger: zerolog.Nop(), PeerReadQueries: func(string) bool { return true }})
	if _, err := r.RouteReadQuery(context.Background(), req); !errors.Is(err, ErrLocalNodeCanHandle) {
		t.Fatalf("a reader: err = %v, want ErrLocalNodeCanHandle", err)
	}
}

// Readers that all lack the capability (a rolling upgrade that reached a
// writer first) do not send Arrow queries to the writer: writers are used only
// when the cluster has no readers, as RouteQuery does.
func TestRouteReadQueryDoesNotFallBackToAWriterPastIncapableReaders(t *testing.T) {
	local := NewNode("local", "Local", RoleCompactor, "c")
	registry := NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	reader := NewNode("old-reader", "R", RoleReader, "c")
	reader.UpdateState(StateHealthy)
	reader.APIAddress = "127.0.0.1:1"
	writer := NewNode("new-writer", "W", RoleWriter, "c")
	writer.UpdateState(StateHealthy)
	writer.APIAddress = "127.0.0.1:2"
	for _, n := range []*Node{reader, writer} {
		if err := registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	r := NewRouter(&RouterConfig{Registry: registry, LocalNode: local, Logger: zerolog.Nop(),
		PeerReadQueries: func(id string) bool { return id == "new-writer" }})
	req, _ := http.NewRequest(http.MethodPost, "http://x/api/v1/query/arrow", nil)
	if _, err := r.RouteReadQuery(context.Background(), req); !errors.Is(err, ErrNoCapablePeer) {
		t.Fatalf("err = %v, want ErrNoCapablePeer (no fallback to the writer)", err)
	}
}

// With no node that could serve the read at all, the answer is 503 (as
// /api/v1/query), not a local fallback: the compactor must not take reads
// because its readers are down.
func TestRouteReadQueryWithNoPeersIsUnavailable(t *testing.T) {
	local := NewNode("local", "Local", RoleCompactor, "c")
	registry := NewRegistry(&RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
	r := NewRouter(&RouterConfig{Registry: registry, LocalNode: local, Logger: zerolog.Nop(), PeerReadQueries: func(string) bool { return true }})
	req, _ := http.NewRequest(http.MethodGet, "http://x/api/v1/measurements", nil)
	if _, err := r.RouteReadQuery(context.Background(), req); !errors.Is(err, ErrNoReaderAvailable) {
		t.Fatalf("err = %v, want ErrNoReaderAvailable", err)
	}
}
