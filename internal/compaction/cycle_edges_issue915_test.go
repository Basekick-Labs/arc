package compaction

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// Intercept only manifest listing. Ordinary discovery still uses the
// manager's real local storage backend.
type manifestBackendIssue915 struct {
	storage.Backend
	calls  int
	cancel context.CancelFunc
	fail   error
}

func (b *manifestBackendIssue915) List(
	ctx context.Context, prefix string,
) ([]string, error) {
	if prefix != ManifestBasePath+"/" {
		return b.Backend.List(ctx, prefix)
	}

	b.calls++

	// The initial call belongs to crash-recovery discovery.
	if b.calls == 1 {
		return nil, nil
	}

	// The next call belongs to candidate manifest filtering.
	if b.cancel != nil {
		b.cancel()
		return nil, ctx.Err()
	}

	if b.fail != nil {
		return nil, b.fail
	}

	return nil, nil
}

func TestCycleManifestIndependentFailureIssue915(t *testing.T) {
	manager, backend, cleanup := setupTestManager(t)
	defer cleanup()

	failure := errors.New("independent manifest storage failure")
	interceptor := &manifestBackendIssue915{
		Backend: backend,
		fail:    failure,
	}

	manager.ManifestManager = NewManifestManager(
		interceptor, zerolog.Nop(),
	)

	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			return []Candidate{candidateIssue915("partition")}, nil
		},
	}}

	launched := false
	manager.compactBatchForTest = func(
		context.Context, Candidate,
	) error {
		launched = true
		return nil
	}

	_, err := manager.RunCompactionCycleForMeasurement(
		context.Background(), "db", "cpu", []string{"hourly"},
	)

	if err == nil {
		t.Fatal("want independent manifest failure, got successful cycle")
	}

	if launched {
		t.Fatal("manifest listing failure bypassed fail-closed filtering")
	}

	if interceptor.calls != 2 {
		t.Fatalf("manifest lists = %d, want 2", interceptor.calls)
	}

	outcome := cycleOutcomeIssue915(t, manager)

	if outcome["status"] != "failed" ||
		outcome["discovery_errors"] != int64(1) ||
		outcome["failed_batches"] != int64(0) ||
		outcome["interrupted_batches"] != int64(0) {
		t.Fatalf("manifest failure misclassified: %#v", outcome)
	}
}

func TestCycleManifestCancellationIssue915(t *testing.T) {
	manager, backend, cleanup := setupTestManager(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	interceptor := &manifestBackendIssue915{
		Backend: backend,
		cancel:  cancel,
	}

	manager.ManifestManager = NewManifestManager(
		interceptor, zerolog.Nop(),
	)

	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			return []Candidate{candidateIssue915("partition")}, nil
		},
	}}

	launched := false
	manager.compactBatchForTest = func(
		context.Context, Candidate,
	) error {
		launched = true
		return nil
	}

	_, err := manager.RunCompactionCycleForMeasurement(
		ctx, "db", "cpu", []string{"hourly"},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	if launched {
		t.Fatal("cancelled manifest filtering launched work")
	}

	if interceptor.calls != 2 {
		t.Fatalf("manifest lists = %d, want 2", interceptor.calls)
	}

	outcome := cycleOutcomeIssue915(t, manager)

	if outcome["status"] != "cancelled" ||
		outcome["discovery_errors"] != int64(0) ||
		outcome["failed_batches"] != int64(0) {
		t.Fatalf("cancellation misclassified: %#v", outcome)
	}
}

func TestCycleDiscoveryCancellationIssue915(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	manager.ManifestManager = nil

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(
			ctx context.Context, _, _ string,
		) ([]Candidate, error) {
			cancel()
			return nil, ctx.Err()
		},
	}}

	_, err := manager.RunCompactionCycleForMeasurement(
		ctx, "db", "cpu", []string{"hourly"},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("discovery error = %v, want cancellation", err)
	}

	outcome := cycleOutcomeIssue915(t, manager)

	if outcome["status"] != "cancelled" ||
		outcome["discovered_batches"] != int64(0) ||
		outcome["discovery_errors"] != int64(0) {
		t.Fatalf("discovery cancellation misclassified: %#v", outcome)
	}
}

func TestCycleCancellationBetweenBatchesIssue915(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	manager.ManifestManager = nil
	manager.MaxFilesPerBatch = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			candidate := candidateIssue915("partition")
			candidate.Files = []string{
				"a.parquet",
				"b.parquet",
				"c.parquet",
				"d.parquet",
			}
			candidate.FileCount = len(candidate.Files)
			return []Candidate{candidate}, nil
		},
	}}

	calls := 0
	manager.compactBatchForTest = func(
		context.Context, Candidate,
	) error {
		calls++
		cancel()
		return nil
	}

	_, err := manager.RunCompactionCycleForMeasurement(
		ctx, "db", "cpu", []string{"hourly"},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cycle error = %v, want cancellation", err)
	}

	if calls != 1 {
		t.Fatalf("executed batches = %d, want 1", calls)
	}

	outcome := cycleOutcomeIssue915(t, manager)

	want := map[string]int64{
		"discovered_batches":  2,
		"started_batches":     1,
		"succeeded_batches":   1,
		"failed_batches":      0,
		"interrupted_batches": 0,
		"unstarted_batches":   1,
	}

	for name, value := range want {
		if got := outcome[name]; got != value {
			t.Errorf("%s = %v, want %d", name, got, value)
		}
	}

	if outcome["status"] != "cancelled" {
		t.Fatalf("status = %v, want cancelled", outcome["status"])
	}
}

func TestCycleOverlapIssue915(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	manager.ManifestManager = nil

	started := make(chan struct{})
	release := make(chan struct{})

	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			return []Candidate{candidateIssue915("partition")}, nil
		},
	}}

	manager.compactBatchForTest = func(
		context.Context, Candidate,
	) error {
		close(started)
		<-release
		return nil
	}

	done := make(chan error, 1)

	go func() {
		_, err := manager.RunCompactionCycleForMeasurement(
			context.Background(), "db", "cpu", []string{"hourly"},
		)
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("first cycle did not start")
	}

	_, overlapErr := manager.RunCompactionCycleForMeasurement(
		context.Background(), "db", "cpu", []string{"hourly"},
	)

	close(release)

	if !errors.Is(overlapErr, ErrCycleAlreadyRunning) {
		t.Fatalf("overlap error = %v", overlapErr)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first cycle failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("first cycle did not finish")
	}

	if manager.IsCycleRunning() {
		t.Fatal("cycleRunning remained set")
	}

	manager.Tiers = nil
	if _, err := manager.RunCompactionCycleForTiers(
		context.Background(), []string{"hourly"},
	); err != nil {
		t.Fatalf("subsequent cycle failed: %v", err)
	}
}

func TestScheduledCycleUsesConfiguredDeadlineIssue915(t *testing.T) {
	manager, backend, cleanup := setupTestManager(t)
	defer cleanup()

	manager.ManifestManager = nil
	manager.CycleTimeout = 90 * time.Minute

	err := backend.Write(
		context.Background(),
		"db/cpu/2026/09/19/04/example.parquet",
		[]byte("test"),
	)
	if err != nil {
		t.Fatal(err)
	}

	observed := false

	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(
			ctx context.Context, _, _ string,
		) ([]Candidate, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("scheduled cycle has no deadline")
			}

			remaining := time.Until(deadline)
			if remaining < 89*time.Minute ||
				remaining > 90*time.Minute {
				t.Errorf(
					"remaining budget = %s, want approximately 90m",
					remaining,
				)
			}

			observed = true
			return nil, nil
		},
	}}

	scheduler := &Scheduler{
		manager:   manager,
		tierNames: []string{"hourly"},
		logger:    zerolog.Nop(),
	}

	scheduler.runCompaction()

	if !observed {
		t.Fatal("scheduled cycle did not reach candidate discovery")
	}
}
