package api

// /status advertised active_jobs and emitted null; /jobs asserted .(int) off a
// key Stats() never set and so answered 0 for the life of the route (#1168).
// Both now read Manager.ActiveJobs(). The zero case passes identically under
// the old and new code, so these tests drive a genuinely non-zero count.

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/compaction"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
)

// parkingBackend blocks in ConfigJSON, which a compaction attempt calls after
// taking the partition lock and marking itself in flight, and before spawning
// a subprocess. With an already-cancelled context the spawn fails instantly,
// so an attempt can be held mid-flight with no DuckDB involved.
type parkingBackend struct {
	storage.Backend
	entered chan struct{}
	release chan struct{}
}

func (b *parkingBackend) ConfigJSON() string {
	close(b.entered)
	<-b.release
	return b.Backend.ConfigJSON()
}

// parkInFlightAttempt leaves exactly one compaction attempt in flight and
// returns a release func that drains it.
func parkInFlightAttempt(t *testing.T) (*compaction.Manager, *fiber.App, func()) {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = local.Close() })

	parked := &parkingBackend{
		Backend: local,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := compaction.NewManager(&compaction.ManagerConfig{
		StorageBackend: parked,
		LockManager:    compaction.NewLockManager(),
		TempDirectory:  t.TempDir(),
		CycleTimeout:   time.Minute,
		Logger:         zerolog.Nop(),
	})
	handler := NewCompactionHandler(manager, nil, nil, nil, nil, zerolog.Nop())
	app := fiber.New()
	handler.RegisterRoutes(app)

	candidate := compaction.Candidate{
		Database:      "bench",
		Measurement:   "cpu",
		PartitionPath: "bench/cpu/2026/10/08/12",
		Files:         []string{"bench/cpu/2026/10/08/12/a.parquet"},
		FileCount:     1,
		Tier:          "hourly",
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- manager.CompactPartition(ctx, candidate) }()

	select {
	case <-parked.entered:
	case err := <-done:
		t.Fatalf("attempt returned (%v) without reaching the seam", err)
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for the attempt to reach the seam")
	}

	// Precondition: the attempt really is mid-flight, holding its lock.
	lockKey := filepath.Join(candidate.Database, candidate.PartitionPath)
	if !manager.LockManager.IsLocked(lockKey) {
		t.Fatalf("parked attempt does not hold the lock for %q", lockKey)
	}
	if got := manager.ActiveJobs(); got != 1 {
		t.Fatalf("manager reports %d attempts in flight, want 1", got)
	}

	released := false
	return manager, app, func() {
		if released {
			return
		}
		released = true
		close(parked.release)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("timed out draining the parked attempt")
		}
	}
}

// TestActiveJobsEndpointReportsAnInFlightAttemptIssue1168 is the test the
// int-vs-int64 assertion bug could not survive: /jobs must report 1, not 0.
func TestActiveJobsEndpointReportsAnInFlightAttemptIssue1168(t *testing.T) {
	_, app, release := parkInFlightAttempt(t)
	defer release()

	status, body := getJSON(t, app, "/api/v1/compaction/jobs")
	if status != 200 {
		t.Fatalf("GET /jobs = %d, want 200 (%v)", status, body)
	}
	active, ok := body["active_jobs"].(float64)
	if !ok {
		t.Fatalf("/jobs active_jobs is %T (%v), want a number", body["active_jobs"], body["active_jobs"])
	}
	if active != 1 {
		t.Fatalf("/jobs active_jobs = %v with one attempt in flight, want 1", active)
	}
	if unit, _ := body["unit"].(string); unit != "attempts" {
		t.Fatalf("/jobs unit = %q, want \"attempts\" -- the number is attempts, not batches", unit)
	}
	jobs, ok := body["jobs"].([]interface{})
	if !ok || len(jobs) != 0 {
		t.Fatalf("/jobs jobs = %v, want an empty array (no in-flight identities are retained)", body["jobs"])
	}
}

// TestStatusEndpointReportsAnInFlightAttemptIssue1168 covers the other
// surface, which emitted a literal null rather than a wrong number.
func TestStatusEndpointReportsAnInFlightAttemptIssue1168(t *testing.T) {
	_, app, release := parkInFlightAttempt(t)
	defer release()

	status, body := getJSON(t, app, "/api/v1/compaction/status")
	if status != 200 {
		t.Fatalf("GET /status = %d, want 200 (%v)", status, body)
	}
	manager, ok := body["manager"].(map[string]interface{})
	if !ok {
		t.Fatalf("/status has no manager object: %v", body)
	}
	raw, present := manager["active_jobs"]
	if !present || raw == nil {
		t.Fatalf("/status manager.active_jobs = %v, want a number (it emitted null before #1168)", raw)
	}
	active, ok := raw.(float64)
	if !ok {
		t.Fatalf("/status manager.active_jobs is %T (%v), want a number", raw, raw)
	}
	if active != 1 {
		t.Fatalf("/status manager.active_jobs = %v with one attempt in flight, want 1", active)
	}
}

// TestActiveJobsEndpointsReturnToZeroIssue1168 pins the decrement at both
// surfaces: a flipped sign leaves them negative instead of zero.
func TestActiveJobsEndpointsReturnToZeroIssue1168(t *testing.T) {
	_, app, release := parkInFlightAttempt(t)
	release()

	if _, body := getJSON(t, app, "/api/v1/compaction/jobs"); body["active_jobs"] != float64(0) {
		t.Fatalf("/jobs active_jobs after the attempt drained = %v, want 0", body["active_jobs"])
	}
	_, body := getJSON(t, app, "/api/v1/compaction/status")
	manager, _ := body["manager"].(map[string]interface{})
	if manager["active_jobs"] != float64(0) {
		t.Fatalf("/status manager.active_jobs after the attempt drained = %v, want 0", manager["active_jobs"])
	}
}

// TestStatsEndpointCarriesActiveJobsIssue1168 covers the third surface:
// /stats returns the whole Stats() map, so it gains the key too.
func TestStatsEndpointCarriesActiveJobsIssue1168(t *testing.T) {
	_, app, release := parkInFlightAttempt(t)
	defer release()

	status, body := getJSON(t, app, "/api/v1/compaction/stats")
	if status != 200 {
		t.Fatalf("GET /stats = %d, want 200 (%v)", status, body)
	}
	if body["active_jobs"] != float64(1) {
		t.Fatalf("/stats active_jobs = %v with one attempt in flight, want 1", body["active_jobs"])
	}
}
