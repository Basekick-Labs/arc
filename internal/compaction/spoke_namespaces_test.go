package compaction

// Regression tests for #619 part 1: hub compaction of received spoke
// namespaces — pseudo-database expansion, prefix-relative partition parsing,
// name sanitization, and the delivery-gate exemption.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

func nsTestManager(t *testing.T, backend storage.Backend) *Manager {
	t.Helper()
	return &Manager{StorageBackend: backend, logger: zerolog.Nop()}
}

func nsTestBackend(t *testing.T) storage.Backend {
	t.Helper()
	dir, err := os.MkdirTemp("", "ns-compact-*")
	if err != nil {
		t.Fatalf("temp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	b, err := storage.NewLocalBackend(dir, zerolog.Nop())
	if err != nil {
		t.Fatalf("backend: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

// Registered spoke namespaces expand into {spoke}/{child} pseudo-databases;
// plain databases pass through; an expander error skips spoke dirs entirely
// rather than compacting them unexpanded (fail-safe).
func TestExpandNamespaces(t *testing.T) {
	ctx := context.Background()
	backend := nsTestBackend(t)
	for _, p := range []string{
		"telemetry/cpu/2026/08/19/10/a.parquet",
		"rocket-01/factory/temps/2026/08/19/10/b.parquet",
		"rocket-01/lab/humid/2026/08/19/10/c.parquet",
	} {
		if err := backend.Write(ctx, p, []byte("x")); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	m := nsTestManager(t, backend)

	// No hook: passthrough.
	got := m.expandNamespaces(ctx, []string{"telemetry", "rocket-01"})
	if len(got) != 2 {
		t.Fatalf("nil hook altered the list: %v", got)
	}

	m.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) {
		return map[string]struct{}{"rocket-01": {}}, nil
	})
	got = m.expandNamespaces(ctx, []string{"telemetry", "rocket-01"})
	want := map[string]bool{"telemetry": true, "rocket-01/factory": true, "rocket-01/lab": true}
	if len(got) != len(want) {
		t.Fatalf("expanded = %v, want keys %v", got, want)
	}
	for _, db := range got {
		if !want[db] {
			t.Errorf("unexpected database %q", db)
		}
	}

	// Error: spoke dirs skipped, others kept.
	m.SetNamespaceExpander(func(context.Context) (map[string]struct{}, error) {
		return nil, errors.New("registry down")
	})
	got = m.expandNamespaces(ctx, []string{"telemetry", "rocket-01"})
	if len(got) != 2 {
		// Fail-safe keeps the ORIGINAL list when the set is unknown — a bare
		// spoke dir yields no candidates anyway (its "years" are databases).
		t.Fatalf("error path = %v, want passthrough of 2", got)
	}
}

// The hourly scanner finds partitions under a slash-carrying pseudo-database
// — the fixed-index parsing this replaces found zero (#619 review F1).
func TestHourlyTier_FindsPartitionsUnderPseudoDatabase(t *testing.T) {
	ctx := context.Background()
	backend := nsTestBackend(t)
	for i := 0; i < 3; i++ {
		p := "rocket-01/factory/temps/2026/08/19/10/f" + string(rune('a'+i)) + ".parquet"
		if err := backend.Write(ctx, p, []byte("x")); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	tier := NewHourlyTier(&HourlyTierConfig{
		StorageBackend: backend,
		MinFiles:       3,
		MinAgeHours:    1,
		Enabled:        true,
		Logger:         zerolog.Nop(),
	})
	// Partition hour 2026/08/19/10 is far in the past relative to test time.
	cands, err := tier.FindCandidates(ctx, "rocket-01/factory", "temps")
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(cands) != 1 {
		t.Fatalf("candidates = %d, want 1", len(cands))
	}
	c := cands[0]
	if c.PartitionPath != "rocket-01/factory/temps/2026/08/19/10" {
		t.Errorf("partition = %q", c.PartitionPath)
	}
	if len(c.Files) != 3 {
		t.Errorf("files = %d, want 3", len(c.Files))
	}
	wantPT := time.Date(2026, 8, 19, 10, 0, 0, 0, time.UTC)
	if !c.PartitionTime.Equal(wantPT) {
		t.Errorf("partition time = %v, want %v", c.PartitionTime, wantPT)
	}
}

// Tail-shape input detection: hour-level files (any database depth) are daily
// input; day-level outputs are not.
func TestIsHourLevelFile(t *testing.T) {
	cases := map[string]bool{
		"db/meas/2026/08/19/10/f.parquet":           true,  // plain, hour level
		"rocket-01/db/meas/2026/08/19/10/f.parquet": true,  // pseudo, hour level
		"db/meas/2026/08/19/f_daily.parquet":        false, // day level
		"rocket-01/db/meas/2026/08/19/f.parquet":    false, // pseudo, day level
		"db/meas/f.parquet":                         false,
	}
	for path, want := range cases {
		if got := isHourLevelFile(path); got != want {
			t.Errorf("%s = %v, want %v", path, got, want)
		}
	}
}

// Sanitized names are collision-free against legal database names and carry
// no path separators (cluster completion manifests REJECT them).
func TestSanitizeDBForName(t *testing.T) {
	if got := sanitizeDBForName("telemetry"); got != "telemetry" {
		t.Errorf("plain = %q", got)
	}
	got := sanitizeDBForName("rocket-01/telemetry")
	if got != "rocket-01.telemetry" {
		t.Errorf("pseudo = %q", got)
	}
	// "." is illegal in database names, so no legal database can collide.
	if got == sanitizeDBForName("rocket-01_telemetry") {
		t.Error("sanitized pseudo collides with a legal database name")
	}
}

// Expander-produced candidates bypass the delivery gate — without this a
// dual-role node defers every received partition forever (#619 review F2) —
// and the exemption survives batch splitting.
func TestSyncExemptBypassesDeliveryGate(t *testing.T) {
	m := nsTestManager(t, nsTestBackend(t))
	m.SetSyncEligibility(func(ctx context.Context, paths []string) (map[string]bool, error) {
		return nil, errors.New("gate must not be consulted for exempt candidates")
	})

	c := Candidate{
		Database:   "rocket-01/factory",
		Files:      []string{"a.parquet", "b.parquet"},
		FileCount:  2,
		SyncExempt: true,
	}
	got, ok := m.filterSyncEligibility(context.Background(), c, stubTier{minFiles: 2})
	if !ok || len(got.Files) != 2 {
		t.Fatalf("exempt candidate was gated: ok=%v files=%d", ok, len(got.Files))
	}

	for _, b := range SplitCandidateIntoBatches(c, 1) {
		if !b.SyncExempt {
			t.Fatal("batch splitting dropped SyncExempt")
		}
	}
}

// Deep-review B1: a failed receipt mark must KEEP the crash-recovery
// manifest — recovery re-fires the marks idempotently — while a successful
// mark lets recovery delete it.
func TestRecovery_MarkFailureKeepsManifest(t *testing.T) {
	ctx := context.Background()
	backend := nsTestBackend(t)
	mm := NewManifestManager(backend, zerolog.Nop())

	// Kept-output shape: output exists, inputs already gone.
	if err := backend.Write(ctx, "rocket-01/db/meas/2026/08/19/10/out_compacted.parquet", []byte("data")); err != nil {
		t.Fatalf("write output: %v", err)
	}
	manifest := &Manifest{
		OutputPath:    "rocket-01/db/meas/2026/08/19/10/out_compacted.parquet",
		OutputSize:    4,
		InputFiles:    []string{"rocket-01/db/meas/2026/08/19/10/raw1.parquet"},
		Database:      "rocket-01/db",
		Measurement:   "meas",
		PartitionPath: "rocket-01/db/meas/2026/08/19/10",
		Tier:          "hourly",
		Status:        ManifestStatusPending,
		CreatedAt:     time.Now().UTC(),
		JobID:         "j1",
	}
	mpath, err := mm.WriteManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	// Marks fail: the manifest must survive.
	failing := func([]string) error { return errors.New("sqlite busy") }
	if _, err := mm.RecoverOrphanedManifests(ctx, nil, failing); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if present, _ := backend.Exists(ctx, mpath); !present {
		t.Fatal("manifest deleted despite a failed receipt mark; the marks are lost forever")
	}

	// Marks succeed: recovery re-fires them and finalizes.
	var got []string
	ok := func(inputs []string) error { got = append(got, inputs...); return nil }
	if _, err := mm.RecoverOrphanedManifests(ctx, nil, ok); err != nil {
		t.Fatalf("recover 2: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("marks re-fired %d inputs, want 1", len(got))
	}
	if present, _ := backend.Exists(ctx, mpath); present {
		t.Fatal("manifest not finalized after successful marks")
	}
}

// Deep-review B2: the parent may finalize the manifest ONLY when source
// deletion fully succeeded — on a partial failure the manifest is the
// surviving raws' sole recovery path.
func TestRetainedManifestPath_GatedOnDeletionSuccess(t *testing.T) {
	j := &Job{ParentFinalizesManifest: true, manifestPath: "_compaction_state/h/x.json"}
	if got := j.RetainedManifestPath(); got != "" {
		t.Fatalf("path handed to the parent despite unfinished source deletion: %q", got)
	}
	j.sourcesDeleted = true
	if got := j.RetainedManifestPath(); got != "_compaction_state/h/x.json" {
		t.Fatalf("path withheld after full deletion success: %q", got)
	}
	j.ParentFinalizesManifest = false
	if got := j.RetainedManifestPath(); got != "" {
		t.Fatalf("path handed out without the flag: %q", got)
	}
}
