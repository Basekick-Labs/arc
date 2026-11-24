package api

import (
	"encoding/json"
	"github.com/basekick-labs/arc/internal/cluster"
	"github.com/rs/zerolog"
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

// fakeReplicationReadiness is a test double for the replicationReadinessProvider
// interface consumed by checkReplicationReady. Lets the gate tests drive the
// "ready" / "catching up" states without spinning up a real cluster coordinator.
type fakeReplicationReadiness struct {
	ready  bool
	status map[string]int64
}

func (f *fakeReplicationReadiness) ReplicationReady() bool {
	return f.ready
}

func (f *fakeReplicationReadiness) ReplicationCatchUpStatus() map[string]int64 {
	return f.status
}

// TestCheckReplicationReady_GatesWhen_FlagOn_AndCatchingUp covers the issue's
// core ask: with cluster.query_gate_on_catchup=true and the coordinator
// reporting peer replication is still draining, query entry points return 503
// with a structured body that includes catchup_status so clients can implement
// bounded retry without scraping logs.
func TestCheckReplicationReady_GatesWhen_FlagOn_AndCatchingUp(t *testing.T) {
	h := &QueryHandler{
		queryGateOnCatchup: true,
		coordinator: &fakeReplicationReadiness{
			ready: false,
			status: map[string]int64{
				"queue_depth":    7,
				"inflight_count": 2,
				"completed_at":   0,
			},
		},
	}

	app := fiber.New()
	app.Get("/test", h.checkReplicationReady, func(c *fiber.Ctx) error {
		return c.SendString("should not reach")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("io.ReadAll: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v (body=%s)", err, body)
	}

	if got["success"] != false {
		t.Errorf("success: got %v, want false", got["success"])
	}
	if got["error"] != "replication_catch_up_in_progress" {
		t.Errorf("error: got %q, want %q", got["error"], "replication_catch_up_in_progress")
	}
	if got["message"] == nil {
		t.Errorf("message: missing, want operator-actionable text")
	}
	cs, ok := got["catchup_status"].(map[string]any)
	if !ok {
		t.Fatalf("catchup_status: got %T, want object (body=%s)", got["catchup_status"], body)
	}
	if cs["queue_depth"] != float64(7) {
		t.Errorf("catchup_status.queue_depth: got %v, want 7", cs["queue_depth"])
	}
	if cs["inflight_count"] != float64(2) {
		t.Errorf("catchup_status.inflight_count: got %v, want 2", cs["inflight_count"])
	}
	if got := resp.Header.Get("Retry-After"); got != gate503RetryAfterSeconds {
		t.Errorf("Retry-After: got %q, want %q", got, gate503RetryAfterSeconds)
	}
	if got := h.QueryGate503Total(); got != 1 {
		t.Errorf("QueryGate503Total after one fire: got %d, want 1", got)
	}
}

// TestCheckReplicationReady_CounterMonotonic verifies that every gated 503
// increments the counter, so operators can alert on rate from a Prometheus
// scrape. The sampled Warn fires at most once per second; the counter must
// not be subject to the same rate limit.
func TestCheckReplicationReady_CounterMonotonic(t *testing.T) {
	h := &QueryHandler{
		queryGateOnCatchup: true,
		coordinator: &fakeReplicationReadiness{
			ready: false,
		},
	}

	app := fiber.New()
	app.Get("/test", h.checkReplicationReady, func(c *fiber.Ctx) error {
		return c.SendString("should not reach")
	})

	const fires = 5
	for i := 0; i < fires; i++ {
		resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
		if err != nil {
			t.Fatalf("app.Test iter %d: %v", i, err)
		}
		resp.Body.Close()
	}

	if got := h.QueryGate503Total(); got != fires {
		t.Errorf("QueryGate503Total after %d fires: got %d, want %d", fires, got, fires)
	}
}

// TestCheckReplicationReady_PassesThrough_WhenFlagOff covers the default
// behavior: gate disabled, query proceeds even though the coordinator says
// the puller is still draining. Preserves the existing pre-#392 behavior for
// any deployment that hasn't opted in.
func TestCheckReplicationReady_PassesThrough_WhenFlagOff(t *testing.T) {
	h := &QueryHandler{
		queryGateOnCatchup: false,
		coordinator: &fakeReplicationReadiness{
			ready: false, // not ready, but flag off — should pass through
		},
	}

	app := fiber.New()
	app.Get("/test", h.checkReplicationReady, func(c *fiber.Ctx) error {
		return c.SendString("downstream-handler")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Errorf("status: got %d, want 200 (gate is off)", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("io.ReadAll: %v", err)
	}
	if string(body) != "downstream-handler" {
		t.Errorf("body: got %q, want %q (gate should not have fired)", body, "downstream-handler")
	}
}

// TestCheckReplicationReady_PassesThrough_WhenCoordinatorNil covers OSS /
// standalone deployments where there is no coordinator at all. The gate must
// be a no-op so non-cluster builds aren't accidentally blocked.
func TestCheckReplicationReady_PassesThrough_WhenCoordinatorNil(t *testing.T) {
	h := &QueryHandler{
		queryGateOnCatchup: true, // flag on but no coordinator
		coordinator:        nil,
	}

	app := fiber.New()
	app.Get("/test", h.checkReplicationReady, func(c *fiber.Ctx) error {
		return c.SendString("downstream-handler")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Errorf("status: got %d, want 200 (no coordinator, gate is no-op)", resp.StatusCode)
	}
}

// TestCheckReplicationReady_PassesThrough_WhenReady covers the steady-state
// path: gate enabled, coordinator says replication is fully drained, query
// proceeds normally. This is the path operators care about most — once
// catch-up completes, queries unblock without restart.
func TestCheckReplicationReady_PassesThrough_WhenReady(t *testing.T) {
	h := &QueryHandler{
		queryGateOnCatchup: true,
		coordinator: &fakeReplicationReadiness{
			ready: true,
		},
	}

	app := fiber.New()
	app.Get("/test", h.checkReplicationReady, func(c *fiber.Ctx) error {
		return c.SendString("downstream-handler")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusOK {
		t.Errorf("status: got %d, want 200 (replication ready)", resp.StatusCode)
	}
}

// TestCheckReplicationReady_GatesWithoutStatus covers the corner case where
// ReplicationCatchUpStatus returns nil (e.g. puller is nil but the readiness
// fake still says false — defensive). The gate should still 503 with the
// error/message fields, just without catchup_status.
func TestCheckReplicationReady_GatesWithoutStatus(t *testing.T) {
	h := &QueryHandler{
		queryGateOnCatchup: true,
		coordinator: &fakeReplicationReadiness{
			ready:  false,
			status: nil, // no status payload available
		},
	}

	app := fiber.New()
	app.Get("/test", h.checkReplicationReady, func(c *fiber.Ctx) error {
		return c.SendString("should not reach")
	})

	resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != fiber.StatusServiceUnavailable {
		t.Errorf("status: got %d, want 503", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("io.ReadAll: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v (body=%s)", err, body)
	}
	if got["error"] != "replication_catch_up_in_progress" {
		t.Errorf("error: got %q, want %q", got["error"], "replication_catch_up_in_progress")
	}
	if _, hasStatus := got["catchup_status"]; hasStatus {
		t.Errorf("catchup_status: present, want absent when ReplicationCatchUpStatus returns nil")
	}
}

// fakeReadLease is a coordinator with a read lease (cluster/read_lease.go).
type fakeReadLease struct {
	fakeReplicationReadiness
	readReady bool
	why       string
}

func (f *fakeReadLease) ReadReady() (bool, string) { return f.readReady, f.why }

// #1825, #1852: on a cluster whose nodes share one object store, a node whose
// cluster state may be behind the leader's serves no reads. Not opt-in: the
// catch-up gate's flag does not apply.
func TestCheckReplicationReady_ReadLease1825(t *testing.T) {
	for _, flag := range []bool{false, true} {
		lease := &fakeReadLease{fakeReplicationReadiness: fakeReplicationReadiness{ready: true}, why: "this node has not yet synced"}
		h := &QueryHandler{queryGateOnCatchup: flag, coordinator: lease}
		app := fiber.New()
		var quiet bool
		app.Use(func(c *fiber.Ctx) error {
			err := c.Next()
			quiet, _ = c.Locals(ingestRefusedLocal).(bool)
			return err
		})
		app.Get("/test", h.checkReplicationReady, func(c *fiber.Ctx) error { return c.SendString("served") })

		resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != fiber.StatusServiceUnavailable || resp.Header.Get("Retry-After") == "" {
			t.Fatalf("flag %v, no lease: %d %s", flag, resp.StatusCode, body)
		}
		var out map[string]interface{}
		if err := json.Unmarshal(body, &out); err != nil || out["error"] != "cluster_state_not_synced" {
			t.Fatalf("flag %v: body %s", flag, body)
		}
		if h.readLease503Total.Load() != 1 {
			t.Fatalf("refusals counted: %d", h.readLease503Total.Load())
		}
		if !quiet {
			t.Fatal("a refused read is logged once per request at Error by the request logger")
		}

		lease.readReady = true
		resp, err = app.Test(httptest.NewRequest("GET", "/test", nil))
		if err != nil {
			t.Fatal(err)
		}
		body, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 || string(body) != "served" {
			t.Fatalf("flag %v, lease held: %d %s", flag, resp.StatusCode, body)
		}
	}
}

// A compactor forwards queries to a reader or writer: it is not refused for
// a lease it does not need, the peer answers for itself. A node that reads
// locally is refused.
func TestCheckReplicationReady_ForwardingNodeIsNotRefused1852(t *testing.T) {
	for _, tc := range []struct {
		role cluster.NodeRole
		want int
	}{{cluster.RoleCompactor, 200}, {cluster.RoleReader, 503}} {
		local := cluster.NewNode("local", "Local", tc.role, "test-cluster")
		registry := cluster.NewRegistry(&cluster.RegistryConfig{LocalNode: local, Logger: zerolog.Nop()})
		router := cluster.NewRouter(&cluster.RouterConfig{Timeout: time.Second, Registry: registry, LocalNode: local, Logger: zerolog.Nop()})
		h := &QueryHandler{router: router, coordinator: &fakeReadLease{fakeReplicationReadiness: fakeReplicationReadiness{ready: true}, why: "not synced"}}
		app := fiber.New()
		app.Get("/test", h.checkReplicationReady, func(c *fiber.Ctx) error { return c.SendString("passed on") })
		resp, err := app.Test(httptest.NewRequest("GET", "/test", nil))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("%s with no read lease: %d, want %d", tc.role, resp.StatusCode, tc.want)
		}
	}
}
