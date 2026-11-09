package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/fieldschema"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/basekick-labs/arc/internal/tiering"
	"github.com/rs/zerolog"
)

// #1550: a hot-only read must not fail because the cold tier cannot be
// listed, when this node's tier metadata (a complete record of the cold
// store) holds no cold row for the range. Every other case keeps the cold
// partitions and fails as before.

// unlistedColdGuard is a node whose cold backend started (not missing) and
// whose tier metadata answers range lookups as configured.
type unlistedColdGuard struct {
	authoritative bool
	hasCold       bool
	err           error
	calls         int
	start, end    *time.Time
}

func (g *unlistedColdGuard) ColdTierMissing() bool           { return false }
func (g *unlistedColdGuard) ColdMetadataAuthoritative() bool { return g.authoritative }
func (g *unlistedColdGuard) HasColdFilesInRange(_ context.Context, _, _ string, start, end *time.Time) (bool, error) {
	g.calls++
	g.start, g.end = start, end
	return g.hasCold, g.err
}

// unreachableCold is an S3 cold tier whose endpoint refuses connections: every
// listing fails, as in an outage.
func unreachableCold(t *testing.T) storage.Backend {
	t.Helper()
	b, err := storage.NewS3Backend(&storage.S3Config{
		Bucket: "cold", Region: "us-east-1", Endpoint: "http://127.0.0.1:1",
		AccessKey: "k", SecretKey: "s", PathStyle: true,
	}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

const unlistedAt = "2026-01-01T10:00:00Z"

func unlistedEnv(t *testing.T) *fieldSchemaEnv {
	t.Helper()
	e := newFieldSchemaEnv(t, fieldschema.Options{}, false)
	at, _ := time.Parse(time.RFC3339, unlistedAt)
	e.write("sch", "m", at, map[string]interface{}{"v": []int64{1, 2, 3}})
	return e
}

// unlistedRead builds the multi-tier read for sql against hot + the
// unreachable cold tier, returning the FROM clause and whether the transform
// was marked volatile.
func unlistedRead(t *testing.T, e *fieldSchemaEnv, guard *unlistedColdGuard, sql string) (string, bool) {
	t.Helper()
	cold := unreachableCold(t)
	e.h.coldGuard = guard
	e.h.tierView = fakeTierView{tiers: map[tiering.Tier]bool{tiering.TierHot: true, tiering.TierCold: true}, cold: cold}
	ctx, vr := pruning.WithVolatileResult(context.Background())
	paths := map[tiering.Tier]string{
		tiering.TierHot:  e.h.getStoragePath(ctx, "sch", "m"),
		tiering.TierCold: e.h.storagePathForBackend(ctx, cold, "sch", "m"),
	}
	from := e.h.buildMultiTierReadParquet(ctx, "sch", "m", sql, paths, "FROM")
	return from, vr.Volatile
}

const hotRangeSQL = "SELECT count(*) FROM sch.m WHERE time >= '2026-01-01T09:00:00Z' AND time < '2026-01-01T11:00:00Z'"

func TestHotRangeReadSkipsUnlistableColdWithoutColdRows(t *testing.T) {
	e := unlistedEnv(t)
	guard := &unlistedColdGuard{authoritative: true}
	before := metrics.Get().Snapshot()["query_cold_unlisted_skipped_total"]

	from, volatile := unlistedRead(t, e, guard, hotRangeSQL)

	if strings.Contains(from, "s3://") {
		t.Fatalf("read %s still names the cold tier it could not list", from)
	}
	if guard.calls != 1 || guard.start == nil || guard.end == nil {
		t.Fatalf("metadata asked %d times with start %v end %v, want once with both stated bounds", guard.calls, guard.start, guard.end)
	}
	if !volatile {
		t.Fatal("a transform built without the cold tier was not marked volatile")
	}
	var n int64
	if err := e.duck.DB().QueryRow("SELECT count(*) " + from).Scan(&n); err != nil {
		t.Fatalf("hot read %s: %v", from, err)
	}
	if n != 3 {
		t.Fatalf("count = %d, want the 3 hot rows", n)
	}
	if after := metrics.Get().Snapshot()["query_cold_unlisted_skipped_total"]; after.(int64) != before.(int64)+1 {
		t.Fatalf("skip counter %v -> %v, want +1", before, after)
	}
}

func TestUnlistableColdIsKeptWhenAnyDoubt(t *testing.T) {
	// Each read waits out one listing timeout against the dead endpoint.
	t.Parallel()
	cases := []struct {
		name  string
		guard *unlistedColdGuard
	}{
		{"cold rows in range", &unlistedColdGuard{authoritative: true, hasCold: true}},
		{"metadata not authoritative (cluster node, restored metadata)", &unlistedColdGuard{authoritative: false}},
		{"metadata lookup failed", &unlistedColdGuard{authoritative: true, err: errors.New("database is locked")}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e := unlistedEnv(t)
			from, _ := unlistedRead(t, e, tc.guard, hotRangeSQL)
			if !strings.Contains(from, "s3://cold/sch/m/") {
				t.Fatalf("read %s dropped the cold tier it could not list", from)
			}
			if !tc.guard.authoritative && tc.guard.calls != 0 {
				t.Fatalf("non-authoritative metadata was asked %d times", tc.guard.calls)
			}
		})
	}
}

// An end-only range leaves the start open in the lookup: the assumed 1970
// floor must not exclude old cold rows. Without cold rows before the end the
// cold tier is left out; with them it is kept.
func TestEndOnlyRangeAsksMetadataWithAnOpenStart(t *testing.T) {
	// Each read waits out one listing timeout against the dead endpoint.
	t.Parallel()
	sql := "SELECT count(*) FROM sch.m WHERE time < '2026-01-01T11:00:00Z'"
	e := unlistedEnv(t)
	guard := &unlistedColdGuard{authoritative: true, hasCold: true}
	from, _ := unlistedRead(t, e, guard, sql)
	if guard.start != nil || guard.end == nil {
		t.Fatalf("lookup bounds start %v end %v, want an open start and the stated end", guard.start, guard.end)
	}
	if !strings.Contains(from, "s3://cold/") {
		t.Fatalf("read %s dropped a cold tier with rows before the end", from)
	}

	e2 := unlistedEnv(t)
	from2, _ := unlistedRead(t, e2, &unlistedColdGuard{authoritative: true}, sql)
	if strings.Contains(from2, "s3://") {
		t.Fatalf("read %s kept an unlistable cold tier with no cold rows", from2)
	}
}

// No time range: every cold row counts. A measurement with cold rows keeps the
// tier; one whose only cold rows are quarantined (the lookup says none) drops it.
func TestUnboundedReadAsksMetadataWithOpenBounds(t *testing.T) {
	// Each read waits out one listing timeout against the dead endpoint.
	t.Parallel()
	sql := "SELECT count(*) FROM sch.m"
	e := unlistedEnv(t)
	guard := &unlistedColdGuard{authoritative: true, hasCold: true}
	from, _ := unlistedRead(t, e, guard, sql)
	if guard.calls != 1 || guard.start != nil || guard.end != nil {
		t.Fatalf("lookup calls %d bounds %v %v, want one with both open", guard.calls, guard.start, guard.end)
	}
	if !strings.Contains(from, "s3://cold/") {
		t.Fatalf("read %s dropped a cold tier that has cold rows", from)
	}

	e2 := unlistedEnv(t)
	from2, volatile := unlistedRead(t, e2, &unlistedColdGuard{authoritative: true}, sql)
	if strings.Contains(from2, "s3://") {
		t.Fatalf("read %s kept an unlistable cold tier with no cold rows", from2)
	}
	if !volatile {
		t.Fatal("a transform built without the cold tier was not marked volatile")
	}
}

// A range the hot tier verifies empty, with the cold tier left out: hot keeps
// its full glob (an Empty tier is only dropped beside a pruned one), so the
// read is correct, scanning hot unpruned while the cold tier is down.
func TestHotEmptyRangeWithColdLeftOutReadsHotUnpruned(t *testing.T) {
	// Each read waits out one listing timeout against the dead endpoint.
	t.Parallel()
	sql := "SELECT count(*) FROM sch.m WHERE time >= '2025-06-01T00:00:00Z' AND time < '2025-06-01T02:00:00Z'"
	e := unlistedEnv(t)
	from, _ := unlistedRead(t, e, &unlistedColdGuard{authoritative: true}, sql)
	if strings.Contains(from, "s3://") {
		t.Fatalf("read %s kept the unlistable cold tier", from)
	}
	if !strings.Contains(from, "**/*.parquet") {
		t.Fatalf("read %s: want the hot tier's full glob", from)
	}
	var n int64
	if err := e.duck.DB().QueryRow("SELECT count(*) " + from + " WHERE time >= '2025-06-01T00:00:00Z' AND time < '2025-06-01T02:00:00Z'").Scan(&n); err != nil {
		t.Fatalf("read %s: %v", from, err)
	}
	if n != 0 {
		t.Fatalf("count = %d, want 0", n)
	}
}

// A cold path a listing confirmed is evidence the metadata is incomplete: the
// tier is only left out when every kept path rests on a failed listing.
func TestAllColdPathsUnlisted(t *testing.T) {
	cases := []struct {
		d    pruning.TierPruneDetail
		want bool
	}{
		{pruning.TierPruneDetail{Paths: []string{"a", "b"}, Unverified: []string{"a", "b"}}, true},
		{pruning.TierPruneDetail{Paths: []string{"a", "b"}, Unverified: []string{"b"}}, false},
		{pruning.TierPruneDetail{Paths: []string{"a"}}, false},
		{pruning.TierPruneDetail{}, false},
	}
	for i, tc := range cases {
		if got := allColdPathsUnlisted(tc.d); got != tc.want {
			t.Errorf("case %d: allColdPathsUnlisted(%+v) = %v, want %v", i, tc.d, got, tc.want)
		}
	}
}

// A multi-day range against an unreachable cold tier pays one listing
// timeout, not one per day: the cold tier's prune stops after its first
// failed listing (#1550).
func TestMultiDayReadPaysOneColdListingTimeout(t *testing.T) {
	t.Parallel()
	sql := "SELECT count(*) FROM sch.m WHERE time >= '2025-12-28T00:00:00Z' AND time < '2026-01-01T11:00:00Z'"
	e := unlistedEnv(t)
	started := time.Now()
	from, _ := unlistedRead(t, e, &unlistedColdGuard{authoritative: true}, sql)
	elapsed := time.Since(started)
	if strings.Contains(from, "s3://") {
		t.Fatalf("read %s kept the unlistable cold tier", from)
	}
	// Five covered days at 5 s each would be 25 s; one timeout is ~5 s.
	if elapsed > 12*time.Second {
		t.Fatalf("building the read took %v; the cold tier was listed once per day", elapsed)
	}
}
