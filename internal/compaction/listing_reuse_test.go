package compaction

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// countingBackend records every List prefix so a test can prove how many
// times one database/measurement was listed. It stays on the
// DirectoryLister path the manager takes with the real local backend, so
// database and measurement discovery never show up as object listings.
type countingBackend struct {
	storage.Backend
	mu    sync.Mutex
	lists map[string]int
}

func newCountingBackend(t *testing.T) *countingBackend {
	t.Helper()
	local, err := storage.NewLocalBackend(t.TempDir(), zerolog.Nop())
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { local.Close() })
	return &countingBackend{Backend: local, lists: map[string]int{}}
}

func (b *countingBackend) List(ctx context.Context, prefix string) ([]string, error) {
	b.mu.Lock()
	b.lists[prefix]++
	b.mu.Unlock()
	return b.Backend.List(ctx, prefix)
}

func (b *countingBackend) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	return b.Backend.(storage.DirectoryLister).ListDirectories(ctx, prefix)
}

func (b *countingBackend) listCount(prefix string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lists[prefix]
}

// writeHourFiles writes n raw files into one hour partition far enough in
// the past that every tier's age cutoff has passed and the daily tier's
// file-age check is bypassed (SkipFileAgeCheckDays).
func writeHourFiles(t *testing.T, backend storage.Backend, database, measurement string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("%s/%s/2025/01/05/10/%s_20250105_1000%02d_00000000%d.parquet",
			database, measurement, measurement, i, i)
		if err := backend.Write(ctx, path, []byte("parquet")); err != nil {
			t.Fatalf("Write %s: %v", path, err)
		}
	}
}

// newTwoTierManager builds a manager with hourly and daily both enabled and
// the same MinFiles, so a partition either triggers both tiers or neither.
func newTwoTierManager(t *testing.T, backend storage.Backend, minFiles int) *Manager {
	t.Helper()
	logger := zerolog.Nop()
	hourly := NewHourlyTier(&HourlyTierConfig{
		StorageBackend: backend, MinAgeHours: 1, MinFiles: minFiles, Enabled: true, Logger: logger,
	})
	daily := NewDailyTier(&DailyTierConfig{
		StorageBackend: backend, MinAgeHours: 24, MinFiles: minFiles, Enabled: true, Logger: logger,
	})
	return NewManager(&ManagerConfig{
		StorageBackend:   backend,
		LockManager:      NewLockManager(),
		MinFiles:         minFiles,
		MaxFilesPerBatch: 10,
		MaxConcurrent:    1,
		TempDirectory:    t.TempDir(),
		Tiers:            []Tier{hourly, daily},
		Logger:           logger,
	})
}

// The candidates preview runs every tier against one measurement with no
// compaction in between, so one listing must serve all of them (#316).
func TestManager_FindCandidates_ListsMeasurementOnceAcrossTiers(t *testing.T) {
	backend := newCountingBackend(t)
	writeHourFiles(t, backend, "db1", "m1", 6)
	m := newTwoTierManager(t, backend, 5)

	candidates, err := m.FindCandidates(context.Background())
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected one hourly and one daily candidate, got %d", len(candidates))
	}
	if got := backend.listCount("db1/m1/"); got != 1 {
		t.Fatalf("db1/m1/ listed %d times across two tiers, want 1", got)
	}
}

// A spoke-namespace pseudo-database carries a slash ("rocket-01/telemetry",
// #619). The shared listing must be taken under the full pseudo-database
// prefix and both tiers must still parse partitions relative to it.
func TestManager_FindCandidates_ListsPseudoDatabaseMeasurementOnce(t *testing.T) {
	backend := newCountingBackend(t)
	writeHourFiles(t, backend, "rocket-01/telemetry", "m1", 6)
	m := newTwoTierManager(t, backend, 5)
	m.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) {
		return map[string]struct{}{"rocket-01": {}}, nil
	})

	candidates, err := m.FindCandidates(context.Background())
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("expected one hourly and one daily candidate, got %d: %+v", len(candidates), candidates)
	}
	for _, c := range candidates {
		if c.Database != "rocket-01/telemetry" || c.Measurement != "m1" || len(c.Files) != 6 {
			t.Fatalf("candidate not parsed relative to the pseudo-database prefix: %+v", c)
		}
	}
	if got := backend.listCount("rocket-01/telemetry/m1/"); got != 1 {
		t.Fatalf("rocket-01/telemetry/m1/ listed %d times across two tiers, want 1", got)
	}
}

// A carried listing must be scoped to its own database/measurement:
// objects under another prefix are ignored, as List would never have
// returned them.
func TestHourlyTier_FindCandidatesFromListing_IgnoresForeignObjects(t *testing.T) {
	tier := NewHourlyTier(&HourlyTierConfig{MinAgeHours: 1, MinFiles: 2, Enabled: true, Logger: zerolog.Nop()})
	objects := []string{
		"db1/m1/2025/01/05/10/m1_20250105_100000_000000001.parquet",
		"db1/m1/2025/01/05/10/m1_20250105_100100_000000002.parquet",
		"db1/other/2025/01/05/10/other_20250105_100000_000000001.parquet",
		"db1/other/2025/01/05/10/other_20250105_100100_000000002.parquet",
	}

	got := tier.FindCandidatesFromListing("db1", "m1", objects)
	if len(got) != 1 || got[0].PartitionPath != "db1/m1/2025/01/05/10" || len(got[0].Files) != 2 {
		t.Fatalf("unexpected candidates: %+v", got)
	}
}

func TestDailyTier_FindCandidatesFromListing_IgnoresForeignObjects(t *testing.T) {
	tier := NewDailyTier(&DailyTierConfig{MinAgeHours: 24, MinFiles: 2, Enabled: true, Logger: zerolog.Nop()})
	objects := []string{
		"db1/m1/2025/01/05/10/m1_20250105_100000_000000001.parquet",
		"db1/m1/2025/01/05/11/m1_20250105_110000_000000002.parquet",
		"db1/other/2025/01/05/10/other_20250105_100000_000000001.parquet",
		"db1/other/2025/01/05/11/other_20250105_110000_000000002.parquet",
	}

	got := tier.FindCandidatesFromListing("db1", "m1", objects)
	if len(got) != 1 || got[0].PartitionPath != "db1/m1/2025/01/05" || len(got[0].Files) != 2 {
		t.Fatalf("unexpected candidates: %+v", got)
	}
}

// A disabled tier yields nothing from a carried listing, exactly as
// FindCandidates does.
func TestFindCandidatesFromListing_DisabledTierReturnsNothing(t *testing.T) {
	objects := []string{"db1/m1/2025/01/05/10/m1_20250105_100000_000000001.parquet"}
	hourly := NewHourlyTier(&HourlyTierConfig{MinFiles: 1, Enabled: false, Logger: zerolog.Nop()})
	daily := NewDailyTier(&DailyTierConfig{MinFiles: 1, Enabled: false, Logger: zerolog.Nop()})
	for _, tier := range []Tier{hourly, daily} {
		if got := tier.FindCandidatesFromListing("db1", "m1", objects); got != nil {
			t.Fatalf("%s: disabled tier returned %v", tier.GetTierName(), got)
		}
	}
}
