package pruning

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// tierMock embeds mockS3Backend and adds error injection, day-file listings,
// and recording of every requested prefix — the recording is the regression
// harness for the prefix double-application bug (#662 review finding): the
// pruner must hand the backend keys relative to the backend's own root, never
// keys that still carry the configured bucket prefix.
type tierMock struct {
	*mockS3Backend
	files       map[string][]string // List(prefix) -> object keys
	errPrefixes map[string]bool     // prefixes whose listings fail
	requested   []string
}

func (m *tierMock) ListDirectories(ctx context.Context, prefix string) ([]string, error) {
	m.requested = append(m.requested, prefix)
	if m.errPrefixes[prefix] {
		return nil, fmt.Errorf("injected listing error for %s", prefix)
	}
	return m.mockS3Backend.ListDirectories(ctx, prefix)
}

func (m *tierMock) List(ctx context.Context, prefix string) ([]string, error) {
	m.requested = append(m.requested, prefix)
	if m.errPrefixes[prefix] {
		return nil, fmt.Errorf("injected listing error for %s", prefix)
	}
	return m.files[prefix], nil
}

func tierRange(t *testing.T, start, end string) *TimeRange {
	t.Helper()
	s, err := time.Parse(time.RFC3339, start)
	if err != nil {
		t.Fatal(err)
	}
	e, err := time.Parse(time.RFC3339, end)
	if err != nil {
		t.Fatal(err)
	}
	return &TimeRange{Start: s, End: e}
}

// The cold glob embeds the backend's configured prefix; listings must go out
// backend-relative so the backend's own prefixing applies exactly once.
func TestPruneTierPaths_RemoteUsesBackendRelativeKeys(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &tierMock{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/2024/03/15/": {"14"},
		}},
		files: map[string][]string{},
	}
	glob := "s3://bucket/tenant1/db/cpu/**/*.parquet"

	paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), backend, false)

	if outcome != TierPrunePruned {
		t.Fatalf("outcome = %v, want TierPrunePruned", outcome)
	}
	want := []string{"s3://bucket/tenant1/db/cpu/2024/03/15/14/*.parquet"}
	if len(paths) != 1 || paths[0] != want[0] {
		t.Fatalf("paths = %v, want %v", paths, want)
	}
	for _, prefix := range backend.requested {
		if strings.HasPrefix(prefix, "tenant1/") || strings.Contains(prefix, "bucket") {
			t.Fatalf("backend asked to list %q — key must be relative to the backend root (double-prefix bug)", prefix)
		}
	}
}

func TestPruneTierPaths_VerifiedEmptyDropsTier(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &tierMock{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{}},
		files:         map[string][]string{},
	}
	glob := "s3://bucket/db/cpu/**/*.parquet"

	paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), backend, false)

	if outcome != TierPruneEmpty {
		t.Fatalf("outcome = %v, want TierPruneEmpty", outcome)
	}
	if len(paths) != 0 {
		t.Fatalf("paths = %v, want none", paths)
	}
}

// A listing failure must fail OPEN: keep the unverifiable paths, and never
// let an all-error result read as "verified empty".
func TestPruneTierPaths_ListingErrorFailsOpen(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &tierMock{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{}},
		files:         map[string][]string{},
		errPrefixes:   map[string]bool{"db/cpu/2024/03/15/": true},
	}
	glob := "s3://bucket/db/cpu/**/*.parquet"

	paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), backend, false)

	if outcome != TierPrunePruned {
		t.Fatalf("outcome = %v, want TierPrunePruned (fail-open keeps unverifiable paths)", outcome)
	}
	// The hour paths under the erroring parent AND the day-level path (same
	// erroring prefix) must all survive.
	if len(paths) != 3 {
		t.Fatalf("paths = %v, want the 2 hour paths and 1 day path kept", paths)
	}
}

func TestPruneTierPaths_LocalTier(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	base := t.TempDir()
	hourDir := filepath.Join(base, "db", "cpu", "2024", "03", "15", "14")
	if err := os.MkdirAll(hourDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hourDir, "x.parquet"), []byte("p"), 0o644); err != nil {
		t.Fatal(err)
	}
	glob := base + "/db/cpu/**/*.parquet"

	paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), nil, false)

	if outcome != TierPrunePruned {
		t.Fatalf("outcome = %v, want TierPrunePruned", outcome)
	}
	if len(paths) != 1 || paths[0] != filepath.Join(base, "db", "cpu", "2024", "03", "15", "14", "*.parquet") {
		t.Fatalf("paths = %v, want the one existing hour glob", paths)
	}

	// A range with no local data at all is a verified-empty tier.
	_, outcome = p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2030-01-01T00:00:00Z", "2030-01-01T02:00:00Z"), nil, false)
	if outcome != TierPruneEmpty {
		t.Fatalf("future-range outcome = %v, want TierPruneEmpty", outcome)
	}
}

func TestPruneTierPaths_FallbackCases(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	r := tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z")

	if _, outcome := p.PruneTierPaths(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu", nil, nil, false); outcome != TierPruneFallback {
		t.Fatalf("nil range: outcome = %v, want TierPruneFallback", outcome)
	}
	if _, outcome := p.PruneTierPaths(context.Background(), "s3://bucket/other/shape.parquet", "db", "cpu", r, nil, false); outcome != TierPruneFallback {
		t.Fatalf("bad glob shape: outcome = %v, want TierPruneFallback", outcome)
	}
	// A range wide enough to trip the path cap must fall back, not drop.
	wide := &TimeRange{Start: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), End: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	if _, outcome := p.PruneTierPaths(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu", wide, nil, false); outcome != TierPruneFallback {
		t.Fatalf("capped range: outcome = %v, want TierPruneFallback", outcome)
	}
}

// End-only predicates get an assumed 2020 start; the range must say so, so
// the query layer can refuse to prune a cold archive with it.
func TestExtractTimeRange_MarksAssumedStart(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())

	endOnly := p.ExtractTimeRange("SELECT * FROM cpu WHERE time < '2024-01-01 00:00:00'")
	if endOnly == nil || !endOnly.StartAssumed {
		t.Fatalf("end-only range = %+v, want StartAssumed=true", endOnly)
	}
	twoSided := p.ExtractTimeRange("SELECT * FROM cpu WHERE time >= '2023-01-01' AND time < '2024-01-01'")
	if twoSided == nil || twoSided.StartAssumed {
		t.Fatalf("two-sided range = %+v, want StartAssumed=false", twoSided)
	}
}

// A day with only a daily-compacted file (no hour subdirectories) must keep
// its day-level glob and drop the hour globs, and vice versa.
func TestPruneTierPaths_MixedHourAndDayExistence(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &tierMock{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{}},
		files: map[string][]string{
			"db/cpu/2024/03/15/": {"db/cpu/2024/03/15/cpu_daily.parquet"},
		},
	}
	glob := "s3://bucket/db/cpu/**/*.parquet"

	paths, outcome := p.PruneTierPaths(context.Background(), glob, "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), backend, false)

	if outcome != TierPrunePruned {
		t.Fatalf("outcome = %v, want TierPrunePruned", outcome)
	}
	if len(paths) != 1 || paths[0] != "s3://bucket/db/cpu/2024/03/15/*.parquet" {
		t.Fatalf("paths = %v, want only the day-level glob", paths)
	}
}

// A spoke-namespace query reaches the pruner with the query layer's
// (spoke, spoke-db) split, so generated partition paths sit one level too
// shallow and existence-filter to verified-empty. Pin that outcome: the
// query layer's combine rules rely on it (an Empty without any Pruned tier
// keeps the full glob, so spoke queries stay correct, just unpruned).
func TestPruneTierPaths_SpokeShapedLayoutIsEmptyNotPruned(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	base := t.TempDir()
	deep := filepath.Join(base, "rocket-01", "telemetry", "engine_temp", "2024", "03", "15", "14")
	if err := os.MkdirAll(deep, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(deep, "f.parquet"), []byte("p"), 0o644); err != nil {
		t.Fatal(err)
	}
	glob := base + "/rocket-01/telemetry/**/*.parquet"

	paths, outcome := p.PruneTierPaths(context.Background(), glob, "rocket-01", "telemetry",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T15:00:00Z"), nil, false)
	if outcome != TierPruneEmpty {
		t.Fatalf("spoke-shaped outcome = %v (paths=%v), want TierPruneEmpty — pruned paths would miss the extra namespace level", outcome, paths)
	}
}
