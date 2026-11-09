package pruning

import (
	"context"
	"fmt"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// #1550: PruneTierPathsDetail tells the paths a failed listing kept apart
// from the paths a listing confirmed, so the query path can decide about the
// former alone. Its outcome and paths stay exactly PruneTierPaths'.

// listErrMock fails only the object listing (day-level compacted files) for
// the prefixes in listErr; directory listings answer from tierMock.
type listErrMock struct {
	*tierMock
	listErr map[string]bool
}

func (m *listErrMock) List(ctx context.Context, prefix string) ([]string, error) {
	if m.listErr[prefix] {
		m.requested = append(m.requested, prefix)
		return nil, fmt.Errorf("injected object listing error for %s", prefix)
	}
	return m.tierMock.List(ctx, prefix)
}

func TestPruneTierPathsDetail_AllListingsFailedAreUnverified(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &tierMock{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{}},
		files:         map[string][]string{},
		errPrefixes:   map[string]bool{"db/cpu/2024/03/15/": true},
	}
	d := p.PruneTierPathsDetail(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), backend, false)

	if d.Outcome != TierPrunePruned {
		t.Fatalf("outcome = %v, want TierPrunePruned (fail-open is unchanged)", d.Outcome)
	}
	if len(d.Paths) != 3 || !sameSet(d.Paths, d.Unverified) {
		t.Fatalf("paths %v, unverified %v: every kept path rests on a failed listing", d.Paths, d.Unverified)
	}
}

// A listing that answered keeps its paths verified: only the path whose
// listing failed is unverified, and the confirmed hour is not.
func TestPruneTierPathsDetail_ConfirmedPathIsNeverUnverified(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &listErrMock{
		tierMock: &tierMock{
			mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{
				"db/cpu/2024/03/15/": {"db/cpu/2024/03/15/14/"},
			}},
			files: map[string][]string{},
		},
		listErr: map[string]bool{"db/cpu/2024/03/15/": true},
	}
	d := p.PruneTierPathsDetail(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), backend, false)

	hour := "s3://bucket/db/cpu/2024/03/15/14/*.parquet"
	day := "s3://bucket/db/cpu/2024/03/15/*.parquet"
	if !sameSet(d.Paths, []string{hour, day}) {
		t.Fatalf("paths = %v, want the confirmed hour and the unverified day", d.Paths)
	}
	if !sameSet(d.Unverified, []string{day}) {
		t.Fatalf("unverified = %v, want only the day path whose listing failed", d.Unverified)
	}
}

// After the first failed listing the call lists no more: an unreachable store
// costs one timeout per query, not one per covered day.
func TestPruneTierPathsDetail_StopsListingAfterAFailure(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &tierMock{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{}},
		files:         map[string][]string{},
		storeMissing:  true,
	}
	d := p.PruneTierPathsDetail(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu",
		tierRange(t, "2024-03-10T00:00:00Z", "2024-03-15T23:00:00Z"), backend, false)

	if len(backend.requested) != 1 {
		t.Fatalf("issued %d listings %v over six days, want 1", len(backend.requested), backend.requested)
	}
	if len(d.Paths) == 0 || !sameSet(d.Paths, d.Unverified) {
		t.Fatalf("paths %d, unverified %d: the skipped listings must keep their paths unverified", len(d.Paths), len(d.Unverified))
	}
}

// Open-end discovery lists first; its failure must stop the filter too.
func TestPruneTierPathsDetail_OpenEndFailureStopsTheFilter(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &tierMock{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{}},
		files:         map[string][]string{},
		storeMissing:  true,
	}
	tr := tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z")
	tr.EndAssumed = true
	d := p.PruneTierPathsDetail(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu", tr, backend, false)

	if len(backend.requested) != 1 {
		t.Fatalf("issued %d listings %v, want only open-end discovery's one", len(backend.requested), backend.requested)
	}
	if len(d.Paths) == 0 || !sameSet(d.Paths, d.Unverified) {
		t.Fatalf("paths %v, unverified %v", d.Paths, d.Unverified)
	}
}

// A backend that cannot list directories keeps its paths unverified as
// before, but that is a layout limit, not a failed listing: nothing is
// reported unverified, so the query path never drops them.
func TestPruneTierPathsDetail_NonListerIsNotUnverified(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	var backend storage.Backend = struct{ storage.Backend }{&mockS3Backend{}}
	d := p.PruneTierPathsDetail(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu",
		tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z"), backend, false)

	if len(d.Paths) == 0 {
		t.Fatalf("outcome %v with no paths", d.Outcome)
	}
	if len(d.Unverified) != 0 {
		t.Fatalf("unverified = %v, want none for a backend that cannot list", d.Unverified)
	}
}

// Healthy listings report nothing unverified, and PruneTierPaths returns the
// same paths and outcome as the detail.
func TestPruneTierPathsDetail_HealthyListingMatchesPruneTierPaths(t *testing.T) {
	p := NewPartitionPruner(zerolog.Nop())
	backend := &tierMock{
		mockS3Backend: &mockS3Backend{existingDirs: map[string][]string{
			"db/cpu/2024/03/15/": {"db/cpu/2024/03/15/15/"},
		}},
		files: map[string][]string{},
	}
	tr := tierRange(t, "2024-03-15T14:00:00Z", "2024-03-15T16:00:00Z")
	d := p.PruneTierPathsDetail(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu", tr, backend, false)
	paths, outcome := p.PruneTierPaths(context.Background(), "s3://bucket/db/cpu/**/*.parquet", "db", "cpu", tr, backend, false)

	if len(d.Unverified) != 0 {
		t.Fatalf("unverified = %v on a healthy listing", d.Unverified)
	}
	if outcome != d.Outcome || !sameSet(paths, d.Paths) {
		t.Fatalf("PruneTierPaths (%v, %v) != detail (%v, %v)", paths, outcome, d.Paths, d.Outcome)
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, s := range a {
		seen[s]++
	}
	for _, s := range b {
		if seen[s] == 0 {
			return false
		}
		seen[s]--
	}
	return true
}
