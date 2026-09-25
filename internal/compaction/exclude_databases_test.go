package compaction

import (
	"context"
	"os"
	"sort"
	"sync"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// excludeTier is a minimal hourly tier whose discovery is observable: it
// records every (database, measurement) pair FindCandidates is asked about
// and returns one two-file candidate per pair.
type excludeTier struct {
	Tier
	mu    sync.Mutex
	asked []string
}

func (*excludeTier) GetTierName() string { return "hourly" }
func (*excludeTier) IsEnabled() bool     { return true }
func (*excludeTier) GetMinFiles() int    { return 2 }
func (*excludeTier) GetStats() map[string]interface{} {
	return map[string]interface{}{}
}

func (tier *excludeTier) FindCandidates(
	_ context.Context, database, measurement string,
) ([]Candidate, error) {
	tier.mu.Lock()
	tier.asked = append(tier.asked, database)
	tier.mu.Unlock()
	return []Candidate{{
		Database:      database,
		Measurement:   measurement,
		PartitionPath: database + "/" + measurement + "/2026/08/19/10",
		Tier:          "hourly",
		Files: []string{
			database + "/" + measurement + "/2026/08/19/10/a.parquet",
			database + "/" + measurement + "/2026/08/19/10/b.parquet",
		},
		FileCount: 2,
	}}, nil
}

// FindCandidatesFromListing mirrors FindCandidates for the listing-reuse
// path Manager.FindCandidates (the /candidates endpoint) takes.
func (tier *excludeTier) FindCandidatesFromListing(
	database, measurement string, _ []string,
) []Candidate {
	tier.mu.Lock()
	tier.asked = append(tier.asked, database)
	tier.mu.Unlock()
	return []Candidate{{
		Database:    database,
		Measurement: measurement,
		Tier:        "hourly",
	}}
}

func (tier *excludeTier) askedDatabases() []string {
	tier.mu.Lock()
	defer tier.mu.Unlock()
	out := append([]string(nil), tier.asked...)
	sort.Strings(out)
	return out
}

// newExcludeRig builds a manager over a real local backend holding one
// measurement per database, with an observable tier and a batch hook that
// records what actually gets compacted. Manifest recovery is disabled the
// same way the issue-915 cycle tests disable it.
func newExcludeRig(
	t *testing.T, exclude []string, databases ...string,
) (*Manager, *excludeTier, *[]string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "arc-exclude-test-*")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	backend, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { backend.Close() })

	ctx := context.Background()
	for _, db := range databases {
		for _, key := range []string{
			db + "/cpu/2026/08/19/10/a.parquet",
			db + "/cpu/2026/08/19/10/b.parquet",
		} {
			if err := backend.Write(ctx, key, []byte("x")); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
	}

	tier := &excludeTier{}
	manager := NewManager(&ManagerConfig{
		StorageBackend:   backend,
		LockManager:      NewLockManager(),
		MaxConcurrent:    2,
		ExcludeDatabases: exclude,
		TempDirectory:    dir + "/temp",
		Tiers:            []Tier{tier},
		Logger:           zerolog.Nop(),
	})
	manager.ManifestManager = nil

	compacted := &[]string{}
	var mu sync.Mutex
	manager.compactBatchForTest = func(_ context.Context, batch Candidate) error {
		mu.Lock()
		*compacted = append(*compacted, batch.Database)
		mu.Unlock()
		return nil
	}

	return manager, tier, compacted
}

func TestNormalizeExcludeDatabases(t *testing.T) {
	// No separator splitting: "fleet,eu" is a legal spoke namespace ID and
	// must survive verbatim — splitting it would fail to exclude the spoke
	// AND silently exclude real databases named "fleet" and "eu".
	got := normalizeExcludeDatabases([]string{
		" staging ", "", "staging", "  ", "fleet,eu", "spoke1/telemetry",
	})
	want := []string{"staging", "fleet,eu", "spoke1/telemetry"}
	if len(got) != len(want) {
		t.Fatalf("normalized = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("normalized = %v, want %v", got, want)
		}
	}

	if out := normalizeExcludeDatabases(nil); out != nil {
		t.Fatalf("nil input normalized to %v, want nil", out)
	}
}

// The startup typo warning must fire only for entries discovery can never
// produce, and never for the loose-but-legal spoke namespace forms.
func TestExcludeEntryCanNeverMatch(t *testing.T) {
	never := []string{
		"_schema", ".hidden", "compaction", "a..b", `back\slash`,
		"ctrl\x01char", "staging/", "/staging", "a/b/c", "a//b",
	}
	for _, name := range never {
		if !excludeEntryCanNeverMatch(name) {
			t.Errorf("expected %q to be flagged as never-matchable", name)
		}
	}
	legal := []string{"staging", "wh-other", "fleet,eu", "9spoke.eu", "spoke1/telemetry"}
	for _, name := range legal {
		if excludeEntryCanNeverMatch(name) {
			t.Errorf("legal entry %q wrongly flagged", name)
		}
	}
}

// Exact match only: an excluded "wh" must not bleed onto the sibling
// database "wh-other" (the #534 prefix-matching class).
func TestFilterExcludedDatabasesExactMatch(t *testing.T) {
	manager, _, _ := newExcludeRig(t, []string{"wh"}, "wh", "wh-other")

	got := manager.filterExcludedDatabases([]string{"wh", "wh-other"})
	if len(got) != 1 || got[0] != "wh-other" {
		t.Fatalf("filtered = %v, want [wh-other]", got)
	}

	// Empty exclusion set: passthrough of the identical slice.
	none, _, _ := newExcludeRig(t, nil, "wh")
	in := []string{"wh", "wh-other"}
	if out := none.filterExcludedDatabases(in); len(out) != 2 {
		t.Fatalf("empty set filtered = %v, want passthrough", out)
	}
}

// A scheduled (unscoped) cycle skips excluded databases entirely: their
// measurements are never scanned for candidates and nothing is compacted.
func TestScheduledCycleSkipsExcludedDatabase(t *testing.T) {
	manager, tier, compacted := newExcludeRig(
		t, []string{"dbskip"}, "dbkeep", "dbskip",
	)

	if _, err := manager.RunCompactionCycleForTiers(
		context.Background(), []string{"hourly"},
	); err != nil {
		t.Fatalf("cycle: %v", err)
	}

	if asked := tier.askedDatabases(); len(asked) != 1 || asked[0] != "dbkeep" {
		t.Fatalf("tier asked about %v, want [dbkeep]", asked)
	}
	if len(*compacted) != 1 || (*compacted)[0] != "dbkeep" {
		t.Fatalf("compacted %v, want [dbkeep]", *compacted)
	}
}

// An operator naming the database bypasses the exclusion list: the scoped
// cycle compacts an excluded database. This is also the negative control
// proving the assertions in the scheduled test detect compaction when it
// happens on the same rig.
func TestScopedCycleBypassesExclusion(t *testing.T) {
	manager, tier, compacted := newExcludeRig(
		t, []string{"dbskip"}, "dbkeep", "dbskip",
	)

	if _, err := manager.RunCompactionCycleForDatabase(
		context.Background(), "dbskip", []string{"hourly"},
	); err != nil {
		t.Fatalf("scoped cycle: %v", err)
	}

	if asked := tier.askedDatabases(); len(asked) != 1 || asked[0] != "dbskip" {
		t.Fatalf("tier asked about %v, want [dbskip]", asked)
	}
	if len(*compacted) != 1 || (*compacted)[0] != "dbskip" {
		t.Fatalf("compacted %v, want [dbskip]", *compacted)
	}
}

// Spoke namespaces: excluding the parent removes the whole namespace before
// its children are listed; excluding one pseudo-database removes just that
// child after expansion.
func TestExcludeSpokeNamespaces(t *testing.T) {
	t.Run("parent pre-expansion", func(t *testing.T) {
		manager, tier, _ := newExcludeRig(
			t, []string{"rocket-01"}, "telemetry",
		)
		// Give the spoke two children on the backend.
		ctx := context.Background()
		for _, key := range []string{
			"rocket-01/factory/temps/2026/08/19/10/a.parquet",
			"rocket-01/factory/temps/2026/08/19/10/b.parquet",
			"rocket-01/lab/humid/2026/08/19/10/a.parquet",
			"rocket-01/lab/humid/2026/08/19/10/b.parquet",
		} {
			if err := manager.StorageBackend.Write(ctx, key, []byte("x")); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		manager.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{"rocket-01": {}}, nil
		})

		if _, err := manager.RunCompactionCycleForTiers(
			ctx, []string{"hourly"},
		); err != nil {
			t.Fatalf("cycle: %v", err)
		}
		if asked := tier.askedDatabases(); len(asked) != 1 || asked[0] != "telemetry" {
			t.Fatalf("tier asked about %v, want [telemetry]", asked)
		}
	})

	t.Run("child post-expansion", func(t *testing.T) {
		manager, tier, _ := newExcludeRig(
			t, []string{"rocket-01/lab"}, "telemetry",
		)
		ctx := context.Background()
		for _, key := range []string{
			"rocket-01/factory/temps/2026/08/19/10/a.parquet",
			"rocket-01/factory/temps/2026/08/19/10/b.parquet",
			"rocket-01/lab/humid/2026/08/19/10/a.parquet",
			"rocket-01/lab/humid/2026/08/19/10/b.parquet",
		} {
			if err := manager.StorageBackend.Write(ctx, key, []byte("x")); err != nil {
				t.Fatalf("write: %v", err)
			}
		}
		manager.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) {
			return map[string]struct{}{"rocket-01": {}}, nil
		})

		if _, err := manager.RunCompactionCycleForTiers(
			ctx, []string{"hourly"},
		); err != nil {
			t.Fatalf("cycle: %v", err)
		}
		asked := tier.askedDatabases()
		want := []string{"rocket-01/factory", "telemetry"}
		if len(asked) != len(want) || asked[0] != want[0] || asked[1] != want[1] {
			t.Fatalf("tier asked about %v, want %v", asked, want)
		}
	})
}

// The /candidates preview applies the same exclusion a scheduled cycle
// would (#619 F4 invariant): an excluded database never appears in the
// candidate listing.
func TestFindCandidatesAppliesExclusion(t *testing.T) {
	manager, tier, _ := newExcludeRig(
		t, []string{"dbskip"}, "dbkeep", "dbskip",
	)

	candidates, err := manager.FindCandidates(context.Background())
	if err != nil {
		t.Fatalf("FindCandidates: %v", err)
	}
	for _, cand := range candidates {
		if cand.Database == "dbskip" {
			t.Fatalf("excluded database in candidates: %+v", cand)
		}
	}
	if asked := tier.askedDatabases(); len(asked) != 1 || asked[0] != "dbkeep" {
		t.Fatalf("tier asked about %v, want [dbkeep]", asked)
	}
}

// Stats exposes the normalized exclusion list so operators can see what a
// running node is configured to skip, and ExcludedDatabases returns a copy.
func TestStatsExposesExcludeList(t *testing.T) {
	manager, _, _ := newExcludeRig(t, []string{" b ", "a", "b"}, "dbkeep")
	got, ok := manager.Stats()["exclude_databases"].([]string)
	if !ok {
		t.Fatal("exclude_databases missing from stats")
	}
	if len(got) != 2 || got[0] != "b" || got[1] != "a" {
		t.Fatalf("stats exclude_databases = %v, want [b a]", got)
	}

	copied := manager.ExcludedDatabases()
	copied[0] = "mutated"
	if again := manager.ExcludedDatabases(); again[0] != "b" {
		t.Fatalf("ExcludedDatabases returned shared storage: %v", again)
	}

	empty, _, _ := newExcludeRig(t, nil, "dbkeep")
	if out := empty.ExcludedDatabases(); out != nil {
		t.Fatalf("empty config ExcludedDatabases = %v, want nil", out)
	}
	// Stats always hands JSON consumers an array, never null.
	emptyStats, ok := empty.Stats()["exclude_databases"].([]string)
	if !ok || emptyStats == nil || len(emptyStats) != 0 {
		t.Fatalf("empty config stats exclude_databases = %#v, want []", empty.Stats()["exclude_databases"])
	}
}
