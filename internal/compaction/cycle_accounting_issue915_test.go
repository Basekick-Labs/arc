package compaction

import (
	"context"
	"errors"
	"testing"
	"time"
)

type cycleTierIssue915 struct {
	Tier
	find func(context.Context, string, string) ([]Candidate, error)
}

func (cycleTierIssue915) GetTierName() string { return "hourly" }
func (cycleTierIssue915) IsEnabled() bool     { return true }
func (cycleTierIssue915) GetMinFiles() int    { return 2 }
func (cycleTierIssue915) GetStats() map[string]interface{} {
	return map[string]interface{}{}
}

func (tier cycleTierIssue915) FindCandidates(
	ctx context.Context, database, measurement string,
) ([]Candidate, error) {
	return tier.find(ctx, database, measurement)
}

func candidateIssue915(partition string) Candidate {
	return Candidate{
		Database:      "db",
		Measurement:   "cpu",
		PartitionPath: partition,
		Tier:          "hourly",
		Files:         []string{"one.parquet", "two.parquet"},
		FileCount:     2,
	}
}

func cycleOutcomeIssue915(t *testing.T, manager *Manager) map[string]interface{} {
	t.Helper()
	value, ok := manager.Stats()["last_cycle"].(map[string]interface{})
	if !ok {
		t.Fatal("last_cycle statistics missing")
	}
	return value
}

func TestCycleWorkerWaitCancellationAccountingIssue915(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	manager.ManifestManager = nil
	manager.MaxConcurrent = 1

	started := make(chan struct{})
	secondFiltered := make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	manager.compactBatchForTest = func(ctx context.Context, _ Candidate) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}

	filterCalls := 0
	manager.SetSyncEligibility(func(
		_ context.Context, paths []string,
	) (map[string]bool, error) {
		filterCalls++
		if filterCalls == 2 {
			close(secondFiltered)
		}
		eligible := make(map[string]bool, len(paths))
		for _, path := range paths {
			eligible[path] = true
		}
		return eligible, nil
	})

	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			return []Candidate{
				candidateIssue915("partition-one"),
				candidateIssue915("partition-two"),
			}, nil
		},
	}}

	done := make(chan error, 1)
	go func() {
		_, err := manager.RunCompactionCycleForMeasurement(
			ctx, "db", "cpu", []string{"hourly"},
		)
		done <- err
	}()

	for _, signal := range []<-chan struct{}{started, secondFiltered} {
		select {
		case <-signal:
		case <-time.After(10 * time.Second):
			t.Fatal("cycle did not reach expected worker/capacity state")
		}
	}

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cycle error = %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cycle did not exit after cancellation")
	}

	outcome := cycleOutcomeIssue915(t, manager)
	if outcome["status"] != "cancelled" {
		t.Fatalf("status = %v, want cancelled", outcome["status"])
	}

	want := map[string]int64{
		"discovered_batches":  2,
		"started_batches":     1,
		"succeeded_batches":   0,
		"failed_batches":      0,
		"interrupted_batches": 1,
		"unstarted_batches":   1,
	}

	for key, expected := range want {
		if got, ok := outcome[key].(int64); !ok || got != expected {
			t.Errorf("%s = %v, want %d", key, outcome[key], expected)
		}
	}

	if manager.IsCycleRunning() {
		t.Fatal("cycle remained locked after active worker exited")
	}

	manager.Tiers = nil
	if _, err := manager.RunCompactionCycleForTiers(
		context.Background(), []string{"hourly"},
	); err != nil {
		t.Fatalf("next cycle failed: %v", err)
	}
}

func TestCycleGenuineFailureAccountingIssue915(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	manager.ManifestManager = nil
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(context.Context, string, string) ([]Candidate, error) {
			return []Candidate{candidateIssue915("partition")}, nil
		},
	}}

	storageErr := errors.New("independent storage failure")
	manager.compactBatchForTest = func(context.Context, Candidate) error {
		return storageErr
	}

	_, err := manager.RunCompactionCycleForMeasurement(
		context.Background(), "db", "cpu", []string{"hourly"},
	)
	if err == nil {
		t.Fatal("a failed batch must not yield a successful cycle")
	}

	outcome := cycleOutcomeIssue915(t, manager)
	if outcome["status"] != "failed" ||
		outcome["failed_batches"] != int64(1) ||
		outcome["interrupted_batches"] != int64(0) {
		t.Fatalf("genuine failure misclassified: %#v", outcome)
	}
}

func TestCycleMeasurementFilterIssue915(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	manager.ManifestManager = nil

	var calls []string
	manager.Tiers = []Tier{cycleTierIssue915{
		find: func(
			_ context.Context, database, measurement string,
		) ([]Candidate, error) {
			calls = append(calls, database+"/"+measurement)
			return nil, nil
		},
	}}

	_, err := manager.RunCompactionCycleForMeasurement(
		context.Background(), "db", "cpu", []string{"hourly"},
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(calls) != 1 || calls[0] != "db/cpu" {
		t.Fatalf("measurement discovery = %v, want [db/cpu]", calls)
	}
}
