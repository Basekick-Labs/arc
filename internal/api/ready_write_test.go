package api

// /ready/write answers whether a load balancer should route WRITES here,
// which /ready cannot: every healthy node answers /ready, readers included,
// so a write pool pointed at it sprays writes at nodes that only proxy them
// on (#857).

import (
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"io"
	"net/http/httptest"
	"strings"
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

// /ready/read (#1852): a node with no read lease is ready and not a read
// target; its reason is a fixed sentence, never the lease's own (it can carry
// addresses from a failed sync, and the probe is unauthenticated).
func TestReadyRead1852(t *testing.T) {
	s := readyWriteRig(t, true, nil)
	if code, body := getStatus(t, s, "/ready/read"); code != 200 || body["status"] != "ready" {
		t.Fatalf("unwired: %d %v", code, body)
	}
	ok, why := false, "could not sync: dial tcp 10.0.0.7:7946: connection refused"
	s.SetReadReadiness(func() (bool, string) { return ok, why })
	code, body := getStatus(t, s, "/ready/read")
	if code != 503 || body["status"] != "not_read_target" {
		t.Fatalf("no lease: %d %v", code, body)
	}
	if r := fmt.Sprint(body["reason"]); strings.Contains(r, "10.0.0.7") {
		t.Fatalf("the probe gave out the lease's own reason: %q", r)
	}
	if code, _ := getStatus(t, s, "/ready"); code != 200 {
		t.Fatalf("/ready answered %d: a node with no read lease is still ready", code)
	}
	ok = true
	if code, _ := getStatus(t, s, "/ready/read"); code != 200 {
		t.Fatalf("lease held: %d", code)
	}
	s.ready.Store(false)
	if code, body := getStatus(t, s, "/ready/read"); code != 503 || body["status"] != "not_ready" {
		t.Fatalf("not ready at all: %d %v", code, body)
	}
}

// The probe's 503 is an expected state: it does not add an Error line per
// poll.
func TestReadyReadRefusalIsQuiet1852(t *testing.T) {
	s := readyWriteRig(t, true, nil)
	s.SetReadReadiness(func() (bool, string) { return false, "x" })
	var quiet bool
	app := fiber.New()
	app.Use(func(c *fiber.Ctx) error {
		err := c.Next()
		quiet, _ = c.Locals(ingestRefusedLocal).(bool)
		return err
	})
	app.Get("/ready/read", s.readyReadHandler)
	resp, err := app.Test(httptest.NewRequest("GET", "/ready/read", nil))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 || !quiet {
		t.Fatalf("%d, quiet %v", resp.StatusCode, quiet)
	}
}
