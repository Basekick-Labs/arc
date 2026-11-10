package cluster

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/rs/zerolog"
)

// #1567: every build that forwards Arrow queries also accepts them, and says
// so at join.
func TestForwardArrowQueryCapabilityIsAdvertised(t *testing.T) {
	c := originRig(t, "writer-1", RoleWriter)
	if !hasCapability(c.localCapabilities(), CapabilityForwardArrowQuery) {
		t.Fatalf("capabilities %v lack %s", c.localCapabilities(), CapabilityForwardArrowQuery)
	}
}

// Without Raft there is no record of a peer's capabilities: never assume one,
// the query runs locally instead.
func TestPeerArrowQueryWithoutRaftIsFalse(t *testing.T) {
	c := &Coordinator{}
	if c.peerArrowQuery("anyone") {
		t.Fatal("a peer was assumed to accept a forwarded Arrow query with no Raft state")
	}
}

// RouteArrowQuery forwards only to capable peers, and a node that can serve
// queries never forwards.
func TestRouteArrowQueryNeedsACapablePeer(t *testing.T) {
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
		r := NewRouter(&RouterConfig{Registry: registry, LocalNode: local, Logger: zerolog.Nop(), PeerArrowQuery: peer})
		if _, err := r.RouteArrowQuery(context.Background(), req); !errors.Is(err, ErrNoArrowQueryPeer) {
			t.Fatalf("err = %v, want ErrNoArrowQueryPeer", err)
		}
	}
	readerLocal := NewNode("rl", "RL", RoleReader, "c")
	r := NewRouter(&RouterConfig{Registry: registry, LocalNode: readerLocal, Logger: zerolog.Nop(), PeerArrowQuery: func(string) bool { return true }})
	if _, err := r.RouteArrowQuery(context.Background(), req); !errors.Is(err, ErrLocalNodeCanHandle) {
		t.Fatalf("a reader: err = %v, want ErrLocalNodeCanHandle", err)
	}
}
