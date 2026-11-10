package api

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
	"github.com/basekick-labs/arc/internal/tiering"
)

// #1552: on a cluster node, a read whose cold listing failed leaves its cold
// partitions out only on the trusted cold mark's word, and a read of a
// measurement this node has no cold rows for still reads cold when the mark
// says the range may hold cold data.

type fakeColdMark struct {
	trusted    bool
	why        string
	identity   string
	gotID      string
	marks      map[string]int32
	barrierErr error
	barriers   int
	trustCalls int
}

func (f *fakeColdMark) ColdMarkTrusted(_ time.Time, _ time.Duration, id string) (bool, string) {
	f.trustCalls++
	f.gotID = id
	return f.trusted && id != "" && id == f.identity, f.why
}
func (f *fakeColdMark) ColdMarkIdentity() string { return f.identity }
func (f *fakeColdMark) ColdMarkDay(scope string) (int32, bool) {
	d, ok := f.marks[scope]
	return d, ok
}
func (f *fakeColdMark) ColdMarkReadBarrier(context.Context) error {
	f.barriers++
	return f.barrierErr
}

// 2026-01-01 is day 20454.
const markJan1 = int32(20454)

// clusterRead is unlistedRead on a cluster node: tier metadata not trusted
// (clustered), the cold mark answering instead.
func clusterRead(t *testing.T, mark clusterColdMark, tiers map[tiering.Tier]bool, sql string) (string, bool) {
	t.Helper()
	e := unlistedEnv(t)
	cold := unreachableCold(t)
	e.h.coldGuard = &unlistedColdGuard{authoritative: false}
	e.h.coldMark = mark
	e.h.tierView = fakeTierView{tiers: tiers, cold: cold}
	ctx, vr := pruning.WithVolatileResult(context.Background())
	paths := map[tiering.Tier]string{
		tiering.TierHot:  e.h.getStoragePath(ctx, "sch", "m"),
		tiering.TierCold: e.h.storagePathForBackend(ctx, cold, "sch", "m"),
	}
	return e.h.buildMultiTierReadParquet(ctx, "sch", "m", sql, paths, "FROM"), vr.Volatile
}

var bothTiers = map[tiering.Tier]bool{tiering.TierHot: true, tiering.TierCold: true}

func trustedMark(day int32) *fakeColdMark {
	return &fakeColdMark{trusted: true, identity: "s3://x", marks: map[string]int32{"sch/m": day}}
}

// Trusted mark before the read's start day: the hot range answers.
func TestClusterHotRangeAnswersOnTheColdMark(t *testing.T) {
	before := metrics.Get().Snapshot()["query_cold_mark_skipped_total"].(int64)
	mark := trustedMark(markJan1 - 1)
	from, volatile := clusterRead(t, mark, bothTiers, hotRangeSQL)
	if strings.Contains(from, "s3://") {
		t.Fatalf("read %s still names the cold tier", from)
	}
	if !volatile || mark.barriers != 1 || mark.gotID != "s3://x" {
		t.Fatalf("volatile %v, barriers %d, identity %q", volatile, mark.barriers, mark.gotID)
	}
	if after := metrics.Get().Snapshot()["query_cold_mark_skipped_total"].(int64); after != before+1 {
		t.Fatalf("metric %d -> %d", before, after)
	}
}

// Every reason to doubt keeps the cold partitions: the read fails as today.
func TestClusterColdKeptOnAnyDoubt(t *testing.T) {
	cases := map[string]*fakeColdMark{
		"mark on the start day": trustedMark(markJan1),
		"mark after the start":  trustedMark(markJan1 + 3),
		"unknown scope":         trustedMark(raft.MarkUnknownDay),
		"not trusted":           {trusted: false, why: "stale", identity: "s3://x", marks: map[string]int32{"sch/m": 1}},
		"barrier fails":         {trusted: true, identity: "s3://x", marks: map[string]int32{"sch/m": 1}, barrierErr: errors.New("no leader")},
		"no identity":           {trusted: true, identity: "", marks: map[string]int32{"sch/m": 1}},
	}
	for name, mark := range cases {
		t.Run(name, func(t *testing.T) {
			from, _ := clusterRead(t, mark, bothTiers, hotRangeSQL)
			if !strings.Contains(from, "s3://") {
				t.Fatalf("read %s left the cold tier out", from)
			}
		})
	}
	// An untrusted mark costs no read barrier.
	if m := cases["not trusted"]; m.barriers != 0 {
		t.Fatalf("untrusted mark paid %d read barriers", m.barriers)
	}
}

// A scope never marked holds no cold object: the read answers from hot.
func TestClusterUnmarkedScopeAnswers(t *testing.T) {
	mark := &fakeColdMark{trusted: true, identity: "s3://x", marks: map[string]int32{"sch/other": markJan1 + 9}}
	if from, _ := clusterRead(t, mark, bothTiers, hotRangeSQL); strings.Contains(from, "s3://") {
		t.Fatalf("read %s kept cold for an unmarked scope", from)
	}
}

// Open end: only the start matters. An end-only or unbounded read has no
// stated start and keeps cold.
func TestClusterColdMarkRangeSemantics(t *testing.T) {
	mark := trustedMark(markJan1 - 1)
	if from, _ := clusterRead(t, mark, bothTiers, "SELECT count(*) FROM sch.m WHERE time >= '2026-01-01T09:00:00Z'"); strings.Contains(from, "s3://") {
		t.Fatalf("open-end read after the mark kept cold: %s", from)
	}
	for _, sql := range []string{
		"SELECT count(*) FROM sch.m WHERE time < '2026-01-01T11:00:00Z'",
		"SELECT count(*) FROM sch.m",
	} {
		if from, _ := clusterRead(t, trustedMark(markJan1-1), bothTiers, sql); !strings.Contains(from, "s3://") {
			t.Fatalf("%q left cold out with no stated start: %s", sql, from)
		}
	}
}

// Standalone (no cold mark) keeps #1550 unchanged: rows decide.
func TestStandaloneIgnoresTheColdMark(t *testing.T) {
	e := unlistedEnv(t)
	guard := &unlistedColdGuard{authoritative: true, hasCold: true}
	from, _ := unlistedRead(t, e, guard, hotRangeSQL)
	if !strings.Contains(from, "s3://") || guard.calls != 1 {
		t.Fatalf("standalone read %s, metadata calls %d", from, guard.calls)
	}
}

// The tier map TiersForMeasurement returns (the tier cache's own) is never
// changed by a read.
func TestClusterColdMarkLeavesTheTierMapAlone(t *testing.T) {
	hotOnly := map[tiering.Tier]bool{tiering.TierHot: true}
	clusterRead(t, trustedMark(markJan1), hotOnly, hotRangeSQL)
	if len(hotOnly) != 1 || hotOnly[tiering.TierCold] {
		t.Fatalf("tier map changed: %v", hotOnly)
	}
}

// C8: no cold row on this node, but the trusted mark says the range may hold
// cold data: the cold tier is read (here it fails, as a real outage would,
// instead of answering without it). Not trusted, or the range after the mark:
// the rows decide, as before.
func TestClusterReadsColdTheRowsDoNotKnow(t *testing.T) {
	hotOnly := map[tiering.Tier]bool{tiering.TierHot: true}
	from, volatile := clusterRead(t, trustedMark(markJan1), hotOnly, hotRangeSQL)
	if !strings.Contains(from, "s3://") || !volatile {
		t.Fatalf("mark covers the range but the read %s left cold out (volatile %v)", from, volatile)
	}
	if from, _ := clusterRead(t, trustedMark(markJan1-1), hotOnly, hotRangeSQL); strings.Contains(from, "s3://") {
		t.Fatalf("mark before the range, yet cold read: %s", from)
	}
	untrusted := &fakeColdMark{identity: "s3://x", marks: map[string]int32{"sch/m": markJan1}}
	if from, _ := clusterRead(t, untrusted, hotOnly, hotRangeSQL); strings.Contains(from, "s3://") {
		t.Fatalf("untrusted mark changed the read: %s", from)
	}
	// No tier rows at all: same rule.
	if from, _ := clusterRead(t, trustedMark(markJan1), map[tiering.Tier]bool{}, hotRangeSQL); !strings.Contains(from, "s3://") {
		t.Fatalf("no rows, mark covers the range, cold left out: %s", from)
	}
}

// clusterColdMayHaveData's boundaries: a start on the mark's day may need
// cold, the day after may not; unknown and unstated starts always may.
func TestClusterColdMayHaveDataBoundaries(t *testing.T) {
	e := unlistedEnv(t)
	at, _ := time.Parse(time.RFC3339, unlistedAt)
	tr := &pruning.TimeRange{Start: at, End: at.Add(time.Hour)}
	for _, c := range []struct {
		day  int32
		tr   *pruning.TimeRange
		want bool
	}{
		{markJan1, tr, true},
		{markJan1 - 1, tr, false},
		{raft.MarkUnknownDay, tr, true},
		{markJan1 - 1, &pruning.TimeRange{Start: at, End: at, StartAssumed: true}, true},
	} {
		e.h.coldMark = trustedMark(c.day)
		e.h.coldMarkTrust.Store(nil)
		if got := e.h.clusterColdMayHaveData("sch", "m", c.tr); got != c.want {
			t.Fatalf("mark %d range %+v: %v, want %v", c.day, c.tr, got, c.want)
		}
	}
}

// The trust check is cached across reads.
func TestClusterColdMarkTrustIsCached(t *testing.T) {
	e := unlistedEnv(t)
	mark := trustedMark(1)
	e.h.coldMark = mark
	for i := 0; i < 5; i++ {
		e.h.clusterColdMarkUsable()
	}
	if mark.trustCalls != 1 {
		t.Fatalf("trust checked %d times, want 1", mark.trustCalls)
	}
}

func TestUTCDay(t *testing.T) {
	at, _ := time.Parse(time.RFC3339, "2026-01-01T23:59:59-03:00") // Jan 2 UTC
	if utcDay(at) != markJan1+1 {
		t.Fatalf("utcDay = %d", utcDay(at))
	}
}

// On a cluster node, listing stops after the first failure only while the
// cold mark the read would fall back on is trusted.
func TestClusterColdListingStopsOnlyWhenTheMarkIsTrusted(t *testing.T) {
	sql := "SELECT count(*) FROM sch.m WHERE time >= '2025-12-28T00:00:00Z' AND time < '2026-01-01T11:00:00Z'"
	for _, c := range []struct {
		name string
		mark *fakeColdMark
		want func(int64) bool
	}{
		// The mark covers the range, so cold is kept either way: this pins
		// the listing count alone.
		{"trusted", trustedMark(markJan1 + 5), func(n int64) bool { return n == 1 }},
		{"not trusted", &fakeColdMark{identity: "s3://x", marks: map[string]int32{"sch/m": markJan1 + 5}}, func(n int64) bool { return n >= 5 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := unlistedEnv(t)
			cold, requests := deniedCold(t)
			e.h.coldGuard = &unlistedColdGuard{authoritative: false}
			e.h.coldMark = c.mark
			e.h.tierView = fakeTierView{tiers: bothTiers, cold: cold}
			ctx, _ := pruning.WithVolatileResult(context.Background())
			paths := map[tiering.Tier]string{
				tiering.TierHot:  e.h.getStoragePath(ctx, "sch", "m"),
				tiering.TierCold: e.h.storagePathForBackend(ctx, cold, "sch", "m"),
			}
			if from := e.h.buildMultiTierReadParquet(ctx, "sch", "m", sql, paths, "FROM"); !strings.Contains(from, "s3://") {
				t.Fatalf("read %s left cold out; the mark covers the range", from)
			}
			if n := requests.Load(); !c.want(n) {
				t.Fatalf("cold listing requests = %d", n)
			}
		})
	}
}

// flipOnBarrier stops being trusted while the read barrier runs, as when the
// barrier applies a seed reset or a stale member's Alive ages out.
type flipOnBarrier struct{ *fakeColdMark }

func (f flipOnBarrier) ColdMarkReadBarrier(ctx context.Context) error {
	f.trusted = false
	return f.fakeColdMark.ColdMarkReadBarrier(ctx)
}

// Trust is checked again after the barrier, not only from the cache.
func TestClusterColdMarkRecheckedAfterTheBarrier(t *testing.T) {
	mark := trustedMark(markJan1 - 1)
	if from, _ := clusterRead(t, flipOnBarrier{mark}, bothTiers, hotRangeSQL); !strings.Contains(from, "s3://") {
		t.Fatalf("left cold out on a mark no longer trusted after the barrier: %s", from)
	}
}
