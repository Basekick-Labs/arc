package api

// Cluster reads and the cluster's cold mark (#1552).
//
// On a cluster node this node's tier rows are not a complete record of the
// cold store (#1550 trusts them on standalone nodes only). The cluster's cold
// mark is: per "database/measurement", the latest UTC day that may hold a
// cold object, raised through Raft before every cold write and seeded from a
// complete cold listing (internal/cluster/raft/cold_mark.go). Two uses here:
//
//  1. A read whose cold listing failed leaves its cold partitions out when the
//     mark is trusted and its stated start is on a day after the mark — the
//     read cannot need cold data — instead of failing on the outage.
//  2. A read of a measurement this node holds no cold row for still reads the
//     cold tier when the trusted mark says the range may hold cold data: the
//     rows lag the cold store on a reader, and before this such a read left
//     the cold tier out silently.
//
// The mark is trusted only when enabled, fenced, seeded from this node's own
// cold store, and every member confirmed it maintains it recently
// (Coordinator.ColdMarkTrusted). Use 1 also confirms this node's Raft state
// is caught up with the leader first (a lagging reader could hold a mark
// below a committed write). Any doubt keeps today's behaviour.

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/basekick-labs/arc/internal/cluster/raft"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/basekick-labs/arc/internal/pruning"
)

// clusterColdMark is what the query path needs of the cold mark.
// *cluster.Coordinator implements it.
type clusterColdMark interface {
	ColdMarkTrusted(now time.Time, maxAge time.Duration, localIdentity string) (bool, string)
	ColdMarkIdentity() string
	ColdMarkDay(scope string) (day int32, has bool)
	ColdMarkReadBarrier(ctx context.Context) error
}

// coldMarkTrustTTL bounds how long a read reuses another's trust check: the
// check reads the Raft configuration, and every tiered read on a cluster node
// asks whether it may stop listing after a failure.
const coldMarkTrustTTL = 5 * time.Second

type coldMarkTrust struct {
	at      time.Time
	trusted bool
	why     string
}

// clusterColdMarkTrusted is the cached trust check; false off a cluster.
func (h *QueryHandler) clusterColdMarkTrusted() (bool, string) {
	if h.coldMark == nil {
		return false, "not a cluster node"
	}
	now := time.Now()
	if c := h.coldMarkTrust.Load(); c != nil && now.Sub(c.at) < coldMarkTrustTTL {
		return c.trusted, c.why
	}
	ok, why := h.coldMark.ColdMarkTrusted(now, raft.ColdMarkAliveMaxAge, h.coldMark.ColdMarkIdentity())
	h.coldMarkTrust.Store(&coldMarkTrust{at: now, trusted: ok, why: why})
	return ok, why
}

// utcDay is t's UTC day as days since the epoch, the mark's unit (the
// storage layout's day directories are UTC).
func utcDay(t time.Time) int32 {
	return int32(t.UTC().Unix() / 86400)
}

// clusterColdRangeEmpty reports whether the trusted cold mark rules out cold
// data for this read: its stated start is on a day after the scope's mark, or
// the scope was never marked. An assumed start, an unknown-day scope, a mark
// not trusted, or a read barrier that does not complete answer false. The
// end is ignored: a read open at the end needs every day from its start.
//
// The cost — a trust check and one Raft read barrier — is paid only by a read
// whose cold listing already failed.
func (h *QueryHandler) clusterColdRangeEmpty(ctx context.Context, database, measurement string, timeRange *pruning.TimeRange) bool {
	if h.coldMark == nil {
		return false
	}
	start, _ := statedBounds(timeRange)
	if start == nil {
		return false
	}
	if ok, _ := h.clusterColdMarkTrusted(); !ok {
		return false
	}
	if err := h.coldMark.ColdMarkReadBarrier(ctx); err != nil {
		h.logger.Warn().Err(err).Str("database", database).Str("measurement", measurement).
			Msg("Could not confirm this node is caught up with the cluster; keeping the cold partitions the cold tier could not list")
		return false
	}
	// Read after the barrier, uncached: the barrier may have applied a change.
	if ok, _ := h.coldMark.ColdMarkTrusted(time.Now(), raft.ColdMarkAliveMaxAge, h.coldMark.ColdMarkIdentity()); !ok {
		return false
	}
	day, has := h.coldMark.ColdMarkDay(database + "/" + measurement)
	if !has {
		return true
	}
	// raft.MarkUnknownDay is MaxInt32: no start is after it.
	return utcDay(*start) > day
}

// clusterColdMayHaveData reports whether a cluster node must read the cold
// tier for a measurement its tier rows hold no cold row for: the trusted mark
// says the read's range may hold cold data (a marked scope, and no stated
// start or one on or before the mark's day). Untrusted, or off a cluster: the
// rows decide, as before. No barrier: a lagging mark can only answer as the
// rows did before this change.
func (h *QueryHandler) clusterColdMayHaveData(database, measurement string, timeRange *pruning.TimeRange) bool {
	if h.coldMark == nil {
		return false
	}
	if ok, _ := h.clusterColdMarkTrusted(); !ok {
		return false
	}
	day, has := h.coldMark.ColdMarkDay(database + "/" + measurement)
	if !has {
		return false
	}
	start, _ := statedBounds(timeRange)
	return day == raft.MarkUnknownDay || start == nil || utcDay(*start) <= day
}

// clusterColdMarkUsable reports whether a cluster read may stop listing cold
// after the first failure: only when the mark it would then fall back on is
// trusted. Cached check; no barrier.
func (h *QueryHandler) clusterColdMarkUsable() bool {
	ok, _ := h.clusterColdMarkTrusted()
	return ok
}

// coldRangeEmpty is the one question both drop sites ask: may this read leave
// out cold partitions the cold tier could not list? Standalone: tier rows
// (#1550). Cluster: the cold mark.
func (h *QueryHandler) coldRangeEmpty(ctx context.Context, database, measurement string, timeRange *pruning.TimeRange) (bool, bool) {
	if h.coldMetadataTrusted() {
		return h.coldRangeHasNoRows(ctx, database, measurement, timeRange), false
	}
	return h.clusterColdRangeEmpty(ctx, database, measurement, timeRange), true
}

// noteColdSkipped records a read that left cold partitions out: the #1550
// metric and log on standalone, the cold mark's on a cluster node.
func (h *QueryHandler) noteColdSkipped(database, measurement string, partitions int, viaMark bool) {
	if !viaMark {
		h.noteColdUnlistedSkipped(database, measurement, partitions)
		return
	}
	metrics.Get().IncQueryColdMarkSkipped()
	key := "mark:" + database + "/" + measurement
	now := time.Now()
	if last, ok := coldUnlistedLog.Load(key); ok && now.Sub(last.(time.Time)) < coldUnlistedLogEvery {
		return
	}
	coldUnlistedLog.Store(key, now)
	h.logger.Warn().
		Str("database", database).
		Str("measurement", measurement).
		Int("cold_partitions_skipped", partitions).
		Msg("The cold tier could not be listed; answering this read without its cold partitions because the cluster's cold mark says its range holds no cold data (logged at most once a minute per measurement)")
}

// coldMarkTrustCache is the QueryHandler field type.
type coldMarkTrustCache = atomic.Pointer[coldMarkTrust]
