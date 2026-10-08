package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// fakeCompactionGate is a CompactionGate whose answer can change between
// requests. atomic.Bool rather than a plain field: the handler reads it from a
// Fiber request goroutine while the test writes it, which -race would
// otherwise flag on the test's own fake.
type fakeCompactionGate struct {
	canCompact atomic.Bool
	role       string
	// leaseHolder is "" when the cluster manages no lease at all, which is
	// the default (cluster.failover_enabled is false). The rejection wording
	// depends on it, so the tests set it deliberately in both shapes.
	leaseHolder atomic.Value
}

func (g *fakeCompactionGate) CanCompact() bool { return g.canCompact.Load() }
func (g *fakeCompactionGate) Role() string     { return g.role }
func (g *fakeCompactionGate) LeaseHolder() string {
	if v, ok := g.leaseHolder.Load().(string); ok {
		return v
	}
	return ""
}

// triggerCompactionFor posts a trigger and decodes the response.
func triggerCompactionFor(t *testing.T, app *fiber.App) (int, map[string]interface{}) {
	t.Helper()
	resp, err := app.Test(httptest.NewRequest(http.MethodPost, "/api/v1/compaction/trigger", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]interface{}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("body %s: %v", body, err)
	}
	return resp.StatusCode, out
}

func newGateTestHandler(t *testing.T, gate CompactionGate) (*compaction.Manager, *fiber.App) {
	t.Helper()
	manager := compaction.NewManager(&compaction.ManagerConfig{CycleTimeout: time.Minute, Logger: zerolog.Nop()})
	handler := NewCompactionHandler(manager, nil, nil, nil, gate, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)
	return manager, app
}

// A nil gate is OSS and standalone mode: the trigger behaves exactly as it did
// before #1152. This is the configuration every non-clustered deployment runs,
// so it is the row that must not change.
func TestCompactionTriggerNilGateAllowsIssue1152(t *testing.T) {
	_, app := newGateTestHandler(t, nil)

	if status, body := triggerCompactionFor(t, app); status != http.StatusOK {
		t.Fatalf("status = %d (%v), want 200 with no gate", status, body)
	}
}

// A node the gate excludes answers 503 and starts no cycle.
//
// The no-cycle assertion sleeps first and is paired with a positive control.
// The trigger runs the cycle in a goroutine, so immediately after a 200 the
// cycle has not necessarily been claimed either — without the sleep the
// assertion would pass even if the gate did nothing, and without the control
// it would pass even if no configuration could ever start a cycle here.
func TestCompactionTriggerRejectsWhenGateDeniesIssue1152(t *testing.T) {
	gate := &fakeCompactionGate{role: "writer"}
	gate.leaseHolder.Store("arc-compactor1")
	manager, app := newGateTestHandler(t, gate)

	status, body := triggerCompactionFor(t, app)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d (%v), want 503", status, body)
	}
	if body["role"] != "writer" {
		t.Fatalf("role = %v, want writer so an operator can see which node refused", body["role"])
	}
	if body["can_compact"] != false {
		t.Fatalf("can_compact = %v, want false", body["can_compact"])
	}
	if body["lease_holder"] != "arc-compactor1" {
		t.Fatalf("lease_holder = %v, want the holder so the operator knows where to send it", body["lease_holder"])
	}
	// arcli renders only the error field, so the way out must live there.
	if errText, _ := body["error"].(string); !strings.Contains(errText, "arc-compactor1") {
		t.Fatalf("error = %q, want it to name the lease holder (arcli discards message)", errText)
	}

	time.Sleep(50 * time.Millisecond)
	if manager.GetCurrentCycleID() != 0 || manager.IsCycleRunning() {
		t.Fatalf("a cycle was started on a node the gate excludes: cycle_id=%d running=%v",
			manager.GetCurrentCycleID(), manager.IsCycleRunning())
	}

	// Positive control: the same manager does advance its cycle id once the
	// gate allows, so the assertion above is about the gate and not about a
	// manager that can never start a cycle in this fixture.
	gate.canCompact.Store(true)
	if status, body := triggerCompactionFor(t, app); status != http.StatusOK {
		t.Fatalf("control status = %d (%v), want 200", status, body)
	}
	deadline := time.Now().Add(2 * time.Second)
	for manager.GetCurrentCycleID() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if manager.GetCurrentCycleID() == 0 {
		t.Fatal("control: the allowed trigger never claimed a cycle, so the negative assertion proves nothing")
	}
}

// The gate is consulted per request, not captured at route registration, so a
// lease hand-over or a demotion takes effect without restarting the node. This
// is the only test that distinguishes the two, and it is the actual
// requirement: the scheduler re-checks its gate at every tick for the same
// reason.
func TestCompactionTriggerGateIsPerRequestIssue1152(t *testing.T) {
	gate := &fakeCompactionGate{role: "compactor"}
	_, app := newGateTestHandler(t, gate)

	// Standby compactor: right role, lease held elsewhere.
	if status, body := triggerCompactionFor(t, app); status != http.StatusServiceUnavailable {
		t.Fatalf("status before the hand-over = %d (%v), want 503", status, body)
	}

	// The lease moves here. No restart, no re-registration.
	gate.canCompact.Store(true)
	if status, body := triggerCompactionFor(t, app); status != http.StatusOK {
		t.Fatalf("status after the hand-over = %d (%v), want 200", status, body)
	}

	// And back: a demotion takes effect just as immediately.
	gate.canCompact.Store(false)
	if status, body := triggerCompactionFor(t, app); status != http.StatusServiceUnavailable {
		t.Fatalf("status after the demotion = %d (%v), want 503", status, body)
	}
}

// The gate gates the mutating route only. The read-only routes report this
// node's own manager state and storage listing, which is accurate on a node
// that may not compact, so they stay reachable.
func TestCompactionReadRoutesIgnoreTheGateIssue1152(t *testing.T) {
	gate := &fakeCompactionGate{role: "reader"}
	_, app := newGateTestHandler(t, gate)

	for _, path := range []string{
		"/api/v1/compaction/status",
		"/api/v1/compaction/stats",
		"/api/v1/compaction/jobs",
		"/api/v1/compaction/history",
		"/api/v1/compaction/candidates",
	} {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, path, nil))
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200 (the gate must not cover read routes)", path, resp.StatusCode)
		}
	}
}

// The role rejection precedes the pause refusal. A node that may not compact
// gets the same answer whether or not a restore happens to hold the pause
// here, so an operator is never told to "retry when the restore ends" on a
// node where the trigger would never run.
func TestCompactionTriggerGatePrecedesPauseIssue1152(t *testing.T) {
	manager := compaction.NewManager(&compaction.ManagerConfig{CycleTimeout: time.Minute, Logger: zerolog.Nop()})
	var paused atomic.Bool
	paused.Store(true)
	manager.SetPauseGate(paused.Load)

	gate := &fakeCompactionGate{role: "writer"}
	handler := NewCompactionHandler(manager, nil, nil, nil, gate, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)

	status, body := triggerCompactionFor(t, app)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d (%v), want the lease 503 rather than the pause 409", status, body)
	}
	if body["paused"] == true {
		t.Fatalf("body = %v, want the lease rejection, not the pause rejection", body)
	}
}

// The no-lease refusal must not claim a lease exists, and must not send the
// operator to the assign endpoint.
//
// This is the modal case, not an edge: cluster.failover_enabled defaults to
// false, so a licensed cluster with explicit roles manages no compactor lease
// at all and a writer or reader is refused by its static role capability.
// POST /api/v1/cluster/compactor/assign answers "this cluster does not manage
// a compactor lease" in exactly this state, so recommending it here would hand
// the operator a remedy that refuses.
func TestCompactionTriggerNoLeaseWordingIssue1152(t *testing.T) {
	gate := &fakeCompactionGate{role: "writer"} // leaseHolder left unset = ""
	_, app := newGateTestHandler(t, gate)

	status, body := triggerCompactionFor(t, app)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d (%v), want 503", status, body)
	}
	if body["lease_holder"] != "" {
		t.Fatalf("lease_holder = %v, want empty when no lease is managed", body["lease_holder"])
	}
	errText, _ := body["error"].(string)
	// The endpoint path, not the word: "can be assigned" is legitimate prose
	// here, recommending POST /api/v1/cluster/compactor/assign is not.
	if strings.Contains(errText, "compactor/assign") {
		t.Fatalf("error = %q, must not recommend the assign endpoint: it refuses while no lease is managed", errText)
	}
	if strings.Contains(errText, "does not hold") {
		t.Fatalf("error = %q, must not imply a lease exists and is held elsewhere", errText)
	}
	if !strings.Contains(errText, "failover_enabled") || !strings.Contains(errText, "compactor") {
		t.Fatalf("error = %q, want the two real remedies: a compactor-role node, or enabling failover", errText)
	}
}

// blockingTierIssue1152 holds a cycle open until the test releases it, so the
// already-running state is deterministic. Without this the cycle over an empty
// manager finishes before the next request lands and the ordering assertion
// skips instead of running. Same settable-manager.Tiers seam as the #915 test.
type blockingTierIssue1152 struct {
	compaction.Tier
	entered chan struct{}
	release chan struct{}
}

func (blockingTierIssue1152) GetTierName() string              { return "hourly" }
func (blockingTierIssue1152) IsEnabled() bool                  { return true }
func (blockingTierIssue1152) GetStats() map[string]interface{} { return map[string]interface{}{} }
func (tier blockingTierIssue1152) FindCandidates(ctx context.Context, database, measurement string) ([]compaction.Candidate, error) {
	select {
	case tier.entered <- struct{}{}:
	default:
	}
	select {
	case <-tier.release:
	case <-ctx.Done():
	}
	return nil, nil
}

// The role rejection also precedes the already-running 409. The two
// state-dependent refusals are symmetric: a node that may not compact must get
// the same answer regardless of what this process happens to be doing.
func TestCompactionTriggerGatePrecedesCycleRunningIssue1152(t *testing.T) {
	gate := &fakeCompactionGate{role: "compactor"}
	gate.canCompact.Store(true)
	// A real backend is required once a tier is present: the cycle reaches
	// Manager.listDatabases, which the other tests never hit because a
	// tier-less cycle short-circuits before it.
	root := t.TempDir()
	// One partition on disk, or listDatabases returns nothing and the cycle
	// never calls FindCandidates at all.
	partition := filepath.Join(root, "rig", "cpu", "2026", "10", "08", "12")
	if err := os.MkdirAll(partition, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partition, "a.parquet"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	backend, err := storage.NewLocalBackend(root, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: backend, CycleTimeout: time.Minute, Logger: zerolog.Nop(),
	})
	tier := blockingTierIssue1152{entered: make(chan struct{}, 1), release: make(chan struct{})}
	manager.Tiers = []compaction.Tier{tier}
	t.Cleanup(func() { close(tier.release) })

	handler := NewCompactionHandler(manager, nil, nil, nil, gate, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)

	if status, body := triggerCompactionFor(t, app); status != http.StatusOK {
		t.Fatalf("setup trigger = %d (%v), want 200", status, body)
	}
	select {
	case <-tier.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the cycle never reached the tier, so there is no running state to order against")
	}
	if !manager.IsCycleRunning() {
		t.Fatal("cycle not running after the tier was entered")
	}

	// Demote mid-cycle: the answer must be the role 503, not the running 409.
	gate.canCompact.Store(false)
	status, body := triggerCompactionFor(t, app)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d (%v), want the role 503 ahead of the already-running 409", status, body)
	}
	if body["is_running"] == true {
		t.Fatalf("body = %v, want the role rejection rather than the running-cycle rejection", body)
	}
}

// The OSS gate must be a nil INTERFACE, which is what makes that row safe.
//
// main declares compactionAPIGate as an interface variable and assigns it only
// inside the cluster branch, so the OSS value is nil-nil rather than an
// interface holding a nil pointer. Pinned here because the difference is
// invisible at the call site and the typed-nil version of this mistake was a
// real bug once (#713): a non-nil interface wrapping a nil pointer passes
// h.gate == nil and then panics inside CanCompact.
func TestCompactionGateNilInterfaceConversionIssue1152(t *testing.T) {
	// Exactly main's OSS shape: an interface variable declared and never
	// assigned, then passed to the constructor.
	var apiGate CompactionGate
	if apiGate != nil {
		t.Fatal("an unassigned CompactionGate is not nil; the OSS row would panic in CanCompact")
	}
	// And the typed-nil counter-example, to show the distinction is real and
	// that main must keep assigning through an interface variable.
	var typedNil *fakeCompactionGate
	var trap CompactionGate = typedNil
	if trap == nil {
		t.Fatal("a typed nil compared equal to nil; the guard below would not be meaningful")
	}

	manager := compaction.NewManager(&compaction.ManagerConfig{CycleTimeout: time.Minute, Logger: zerolog.Nop()})
	handler := NewCompactionHandler(manager, nil, nil, nil, apiGate, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)
	if status, body := triggerCompactionFor(t, app); status != http.StatusOK {
		t.Fatalf("status = %d (%v), want 200 through the converted nil gate", status, body)
	}
}
