package api

// /ready/write answers whether a load balancer should route WRITES here,
// which /ready cannot: every healthy node answers /ready, readers included,
// so a write pool pointed at it sprays writes at nodes that only proxy them
// on (#857).

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/rs/zerolog"
)

func readyWriteRig(t *testing.T, ready bool, mayIngest *bool) *Server {
	t.Helper()
	s := NewServer(DefaultServerConfig(), zerolog.Nop())
	s.RegisterRoutes()
	s.ready.Store(ready)
	if mayIngest != nil {
		v := *mayIngest
		s.SetWriteReadiness(func() bool { return v })
	}
	return s
}

func getStatus(t *testing.T, s *Server, path string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest("GET", path, nil)
	resp, err := s.app.Test(req, 5_000)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(body, &out)
	return resp.StatusCode, out
}

// A node with no cluster is the whole deployment: it takes writes whenever it
// is ready, and nothing needs wiring.
func TestReadyWrite_UnwiredNodeAcceptsWrites(t *testing.T) {
	s := readyWriteRig(t, true, nil)
	if code, body := getStatus(t, s, "/ready/write"); code != 200 {
		t.Errorf("status = %d, body = %v; want 200 for an unclustered node", code, body)
	}
}

func TestReadyWrite_FollowsThePredicate(t *testing.T) {
	yes, no := true, false

	s := readyWriteRig(t, true, &yes)
	if code, _ := getStatus(t, s, "/ready/write"); code != 200 {
		t.Errorf("a write target answered %d; want 200", code)
	}

	s = readyWriteRig(t, true, &no)
	code, body := getStatus(t, s, "/ready/write")
	if code != 503 {
		t.Errorf("a non-write-target answered %d; want 503", code)
	}
	if body["status"] != "not_write_target" {
		t.Errorf("status = %v; want not_write_target", body["status"])
	}
	// /ready must still say the node is fine: it serves queries.
	if code, _ := getStatus(t, s, "/ready"); code != 200 {
		t.Errorf("/ready answered %d for a healthy non-writer; want 200", code)
	}
}

// Anything that makes a node unready makes it unready for writes, so the
// write endpoint can only ever be narrower.
func TestReadyWrite_IsNarrowerThanReady(t *testing.T) {
	yes := true
	s := readyWriteRig(t, false, &yes)

	if code, _ := getStatus(t, s, "/ready"); code != 503 {
		t.Errorf("/ready answered %d while not ready; want 503", code)
	}
	code, body := getStatus(t, s, "/ready/write")
	if code != 503 {
		t.Errorf("/ready/write answered %d while the node is not ready; want 503", code)
	}
	if body["status"] != "not_ready" {
		t.Errorf("status = %v; want not_ready, the readiness reason rather than the role one", body["status"])
	}
}

// The route has to be public in every list that gates authentication, or a
// load balancer cannot call it.
func TestReadyWrite_IsUnauthenticated(t *testing.T) {
	if !isUnauthenticatedRoute("/ready/write") {
		t.Error("/ready/write is not in the client-identity public list")
	}
}
