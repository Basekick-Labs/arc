package api

import (
	"context"
	"testing"

	"github.com/basekick-labs/arc/internal/tiering"
)

// #1765: a transform built while a measurement had only cold rows (the hot
// tier left out) goes stale once the measurement has a hot row, so its first
// hot file is not left out of a repeated query for the cache TTL.
func TestColdOnlyTransformGoesStaleOnTheFirstHotRow(t *testing.T) {
	e := unlistedEnv(t)
	cold := unreachableCold(t)
	tiers := map[tiering.Tier]bool{tiering.TierCold: true}
	e.h.tierView = fakeTierView{tiers: tiers, cold: cold}
	ctx, f := withStoragePathFailure(context.Background())
	paths := map[tiering.Tier]string{
		tiering.TierHot:  e.h.getStoragePath(ctx, "sch", "m"),
		tiering.TierCold: e.h.storagePathForBackend(ctx, cold, "sch", "m"),
	}
	_ = e.h.buildMultiTierReadParquet(ctx, "sch", "m", hotRangeSQL, paths, "FROM")
	if len(f.coldOnly) != 1 || f.coldOnly[0] != (TableReference{Database: "sch", Measurement: "m"}) {
		t.Fatalf("cold-only measurements %v, want sch.m", f.coldOnly)
	}
	if e.h.gainedHotTier(f.coldOnly) {
		t.Fatal("stale while the measurement is still cold-only")
	}
	tiers[tiering.TierHot] = true // the first hot file is recorded
	if !e.h.gainedHotTier(f.coldOnly) {
		t.Fatal("not stale once the measurement has a hot row")
	}
}

// A measurement read with its hot tier is not tracked.
func TestHotTransformIsNotTrackedAsColdOnly(t *testing.T) {
	e := unlistedEnv(t)
	cold := unreachableCold(t)
	e.h.tierView = fakeTierView{tiers: map[tiering.Tier]bool{tiering.TierHot: true}, cold: cold}
	ctx, f := withStoragePathFailure(context.Background())
	paths := map[tiering.Tier]string{tiering.TierHot: e.h.getStoragePath(ctx, "sch", "m")}
	_ = e.h.buildMultiTierReadParquet(ctx, "sch", "m", hotRangeSQL, paths, "FROM")
	if len(f.coldOnly) != 0 {
		t.Fatalf("tracked %v", f.coldOnly)
	}
}
