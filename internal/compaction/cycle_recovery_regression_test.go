package compaction

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

type recoveryListFailureBackend struct{ storage.Backend }

func (b recoveryListFailureBackend) List(ctx context.Context, prefix string) ([]string, error) {
	if strings.HasPrefix(prefix, ManifestBasePath+"/") {
		return nil, errors.New("manifest storage unavailable")
	}
	return b.Backend.List(ctx, prefix)
}

func TestCycleRecoveryFailureReported(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	m.ManifestManager = NewManifestManager(recoveryListFailureBackend{b}, zerolog.Nop())
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return nil, nil }}}
	_, err := m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"})
	if err == nil {
		t.Fatalf("recovery failed but cycle returned nil; outcome=%v", cycleOutcomeIssue915(t, m))
	}
}

func TestCycleEligibilityFailureReported(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	m.ManifestManager = nil
	m.SetSyncEligibility(func(context.Context, []string) (map[string]bool, error) {
		return nil, errors.New("sync ledger unavailable")
	})
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		return []Candidate{candidateIssue915("partition")}, nil
	}}}
	_, err := m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"})
	if err == nil {
		t.Fatalf("eligibility lookup failed but cycle returned nil; outcome=%v", cycleOutcomeIssue915(t, m))
	}
}

func TestCycleMeasurementScopesRecovery(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx := context.Background()
	input := "otherdb/othermeasurement/2026/01/01/00/input.parquet"
	output := "otherdb/othermeasurement/2026/01/01/00/output_compacted.parquet"
	for _, path := range []string{input, output} {
		if err := b.Write(ctx, path, []byte("synthetic payload")); err != nil {
			t.Fatal(err)
		}
	}
	_, err := m.ManifestManager.WriteManifest(ctx, &Manifest{Database: "otherdb", Measurement: "othermeasurement", Tier: "hourly", JobID: "scope-test", PartitionPath: "otherdb/othermeasurement/2026/01/01/00", OutputPath: output, OutputSize: int64(len("synthetic payload")), InputFiles: []string{input}, CreatedAt: time.Now(), Status: ManifestStatusPending})
	if err != nil {
		t.Fatal(err)
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return nil, nil }}}
	if _, err = m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"}); err != nil {
		t.Fatal(err)
	}
	exists, err := b.Exists(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("targeted db/cpu cycle deleted an input belonging to otherdb/othermeasurement during global recovery")
	}
}

func TestCycleIndependentBatchTimeoutFails(t *testing.T) {
	m, _, cleanup := setupTestManager(t)
	defer cleanup()
	m.ManifestManager = nil
	m.MaxFilesPerBatch = 2
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) {
		c := candidateIssue915("partition")
		c.Files = []string{"a", "b", "c", "d"}
		c.FileCount = 4
		return []Candidate{c}, nil
	}}}
	m.compactBatchForTest = func(context.Context, Candidate) error { return context.DeadlineExceeded }
	_, err := m.RunCompactionCycleForMeasurement(context.Background(), "db", "cpu", []string{"hourly"})
	if err == nil {
		t.Fatalf("independent timeout dropped remainder and returned success; outcome=%v", cycleOutcomeIssue915(t, m))
	}
	outcome := cycleOutcomeIssue915(t, m)
	if outcome["status"] != "failed" || outcome["failed_batches"] != int64(2) || outcome["interrupted_batches"] != int64(0) || outcome["unstarted_batches"] != int64(0) {
		t.Fatalf("operation timeout must fail batches, not abandon the partition: %v", outcome)
	}
}

func TestCyclePartialRecoveryAccountingOnCancel(t *testing.T) {
	m, b, cleanup := setupTestManager(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, id := range []string{"first", "second"} {
		input := "db/cpu/2026/01/01/00/" + id + ".parquet"
		output := "db/cpu/2026/01/01/00/" + id + "_compacted.parquet"
		for _, p := range []string{input, output} {
			if err := b.Write(ctx, p, []byte("row")); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := m.ManifestManager.WriteManifest(ctx, &Manifest{Database: "db", Measurement: "cpu", Tier: "hourly", JobID: id, PartitionPath: "db/cpu/2026/01/01/00", OutputPath: output, OutputSize: 3, InputFiles: []string{input}, CreatedAt: time.Now(), Status: ManifestStatusPending}); err != nil {
			t.Fatal(err)
		}
	}
	m.Tiers = []Tier{cycleTierIssue915{find: func(context.Context, string, string) ([]Candidate, error) { return nil, nil }}}
	m.SetOnConsumedInputs(func([]string) error { cancel(); return nil })
	_, err := m.RunCompactionCycleForMeasurement(ctx, "db", "cpu", []string{"hourly"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	pending, err := m.ManifestManager.ListManifests(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected exactly one completed recovery, pending=%v", pending)
	}
	if got := m.Stats()["total_manifests_recover"]; got != 1 {
		t.Fatalf("one manifest successfully recovered before cancellation, total_manifests_recover=%v", got)
	}
}
