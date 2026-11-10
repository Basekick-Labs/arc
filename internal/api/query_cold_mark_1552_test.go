package api

import (
	"context"
	"errors"
	"strings"
	"sync"
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
	raised     time.Time // zero: now
	barrierErr error
	barriers   int
	trustCalls int
	onBarrier  func(*fakeColdMark)
}

func (f *fakeColdMark) ColdMarkTrusted(_ time.Time, _ time.Duration, id string) (bool, string) {
	f.trustCalls++
	f.gotID = id
	return f.trusted && id != "" && id == f.identity, f.why
}
func (f *fakeColdMark) ColdMarkIdentity() string { return f.identity }
func (f *fakeColdMark) ColdMarkDay(scope string) (int32, time.Time, bool) {
	d, ok := f.marks[scope]
	r := f.raised
	if r.IsZero() {
		r = time.Now()
	}
	return d, r, ok
}
func (f *fakeColdMark) ColdMarkReadBarrier(context.Context) error {
	f.barriers++
	if f.onBarrier != nil {
		f.onBarrier(f)
	}
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
	// A read the mark cannot drop pays no read barrier.
	for _, name := range []string{"not trusted", "mark on the start day", "mark after the start", "unknown scope"} {
		if m := cases[name]; m.barriers != 0 {
			t.Fatalf("%s: paid %d read barriers", name, m.barriers)
		}
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
		e.h.coldMarkState.trust.Store(nil)
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
		e.h.clusterColdMarkTrusted(time.Time{})
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
		// Droppable by the mark (start after it): stop after one failure.
		{"trusted, droppable", trustedMark(markJan1 - 10), func(n int64) bool { return n == 1 }},
		// The mark covers the range, so the read cannot be dropped: keep
		// listing each day, as before.
		{"trusted, not droppable", trustedMark(markJan1 + 5), func(n int64) bool { return n >= 5 }},
		{"not trusted", &fakeColdMark{identity: "s3://x", marks: map[string]int32{"sch/m": markJan1 - 10}}, func(n int64) bool { return n >= 5 }},
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
			e.h.buildMultiTierReadParquet(ctx, "sch", "m", sql, paths, "FROM")
			if n := requests.Load(); !c.want(n) {
				t.Fatalf("cold listing requests = %d", n)
			}
		})
	}
}

// Trust is checked again after the barrier, not only from the cache.
func TestClusterColdMarkRecheckedAfterTheBarrier(t *testing.T) {
	mark := trustedMark(markJan1 - 1)
	mark.onBarrier = func(f *fakeColdMark) { f.trusted = false }
	if from, _ := clusterRead(t, mark, bothTiers, hotRangeSQL); !strings.Contains(from, "s3://") {
		t.Fatalf("left cold out on a mark no longer trusted after the barrier: %s", from)
	}
}

// The mark is read again after the barrier: a raise the barrier applied
// (a cold write just committed) keeps the cold partitions.
func TestClusterColdMarkDayReadAfterTheBarrier(t *testing.T) {
	mark := trustedMark(markJan1 - 1)
	mark.onBarrier = func(f *fakeColdMark) { f.marks["sch/m"] = markJan1 }
	if from, _ := clusterRead(t, mark, bothTiers, hotRangeSQL); !strings.Contains(from, "s3://") {
		t.Fatalf("left cold out on a mark raised by the barrier: %s", from)
	}
}

// C8 applies only to a recent raise: an old mark (cold data perhaps all
// deleted since) leaves healthy reads as before.
func TestClusterReadsColdOnlyForARecentRaise(t *testing.T) {
	hotOnly := map[tiering.Tier]bool{tiering.TierHot: true}
	old := trustedMark(markJan1)
	old.raised = time.Now().Add(-coldMarkRecentRaise - time.Minute)
	if from, volatile := clusterRead(t, old, hotOnly, hotRangeSQL); strings.Contains(from, "s3://") || volatile {
		t.Fatalf("an old raise changed a healthy read: %s volatile %v", from, volatile)
	}
	recent := trustedMark(markJan1)
	recent.raised = time.Now().Add(-coldMarkRecentRaise + time.Minute)
	if from, _ := clusterRead(t, recent, hotOnly, hotRangeSQL); !strings.Contains(from, "s3://") {
		t.Fatalf("a recent raise did not add cold: %s", from)
	}
}

// The fallback site (no pruned partitions: here a range past the pruner's
// path cap) never drops on the mark.
func TestClusterFallbackReadNeverDropsOnTheMark(t *testing.T) {
	mark := trustedMark(utcDay(time.Date(2018, 12, 1, 0, 0, 0, 0, time.UTC)))
	sql := "SELECT count(*) FROM sch.m WHERE time >= '2019-01-01T00:00:00Z' AND time < '2026-01-01T11:00:00Z'"
	if from, _ := clusterRead(t, mark, bothTiers, sql); !strings.Contains(from, "s3://") {
		t.Fatalf("a fallback read dropped cold on the mark: %s", from)
	}
}

// One barrier serves every caller that arrived while none ran; a caller that
// arrives while one runs joins the next, which starts after it, never the
// running one.
func TestBarrierCoalescer(t *testing.T) {
	var b barrierCoalescer
	release := make(chan struct{})
	calls := 0
	var mu sync.Mutex
	fn := func(context.Context) error {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			<-release
			return errors.New("first")
		}
		return nil
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- b.do(context.Background(), fn) }()
	for {
		mu.Lock()
		n := calls
		mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	late := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() { late <- b.do(context.Background(), fn) }()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-firstDone; err == nil || err.Error() != "first" {
		t.Fatalf("first caller got %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := <-late; err != nil {
			t.Fatalf("a caller that arrived during the first barrier got its result: %v", err)
		}
	}
	if calls != 2 {
		t.Fatalf("%d barriers for 4 callers, want 2", calls)
	}
}

func TestUTCDayBeforeTheEpoch(t *testing.T) {
	if d := utcDay(time.Date(1969, 12, 31, 23, 0, 0, 0, time.UTC)); d != -1 {
		t.Fatalf("utcDay before the epoch = %d, want -1", d)
	}
	if d := utcDay(time.Unix(0, 0)); d != 0 {
		t.Fatalf("utcDay(epoch) = %d", d)
	}
}
