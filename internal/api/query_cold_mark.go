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
	"sync"
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
	ColdMarkDay(scope string) (day int32, raisedAt time.Time, has bool)
	ColdMarkReadBarrier(ctx context.Context) error
}

// coldMarkTrustTTL bounds how long a read reuses another's trust check: the
// check reads the Raft configuration.
const coldMarkTrustTTL = 5 * time.Second

// coldMarkRecentRaise bounds use 2: a scope whose mark was last raised longer
// ago than this is read as before PR #1552 part 2. A reader's tier rows catch
// up with a cold write at its next cold-tier metadata sync, which runs every
// migration cycle (daily by default) and at startup; two of those is the
// window in which its rows may not know of a recent cold write. Without the
// bound, a measurement whose cold data was all deleted later (the mark never
// goes down) would make every healthy read list cold and skip the transform
// cache forever.
const coldMarkRecentRaise = 48 * time.Hour

type coldMarkTrust struct {
	at      time.Time
	trusted bool
	why     string
}

// coldMarkState is the QueryHandler's cache of the trust check, with a mutex
// so concurrent expiries refresh it once, and the read barrier coalescer.
type coldMarkState struct {
	trust     atomic.Pointer[coldMarkTrust]
	refreshMu sync.Mutex
	barrier   barrierCoalescer
}

// clusterColdMarkTrusted is the trust check, reusing a result taken at or
// after notBefore and less than coldMarkTrustTTL old; false off a cluster.
func (h *QueryHandler) clusterColdMarkTrusted(notBefore time.Time) bool {
	if h.coldMark == nil {
		return false
	}
	fresh := func(c *coldMarkTrust) bool {
		return c != nil && !c.at.Before(notBefore) && time.Since(c.at) < coldMarkTrustTTL
	}
	if c := h.coldMarkState.trust.Load(); fresh(c) {
		return c.trusted
	}
	h.coldMarkState.refreshMu.Lock()
	defer h.coldMarkState.refreshMu.Unlock()
	if c := h.coldMarkState.trust.Load(); fresh(c) {
		return c.trusted
	}
	now := time.Now()
	ok, why := h.coldMark.ColdMarkTrusted(now, raft.ColdMarkAliveMaxAge, h.coldMark.ColdMarkIdentity())
	h.coldMarkState.trust.Store(&coldMarkTrust{at: now, trusted: ok, why: why})
	return ok
}

// utcDay is t's UTC day as days since the epoch, the mark's unit (the
// storage layout's day directories are UTC). Floor division: a time before
// the epoch is on day -1, not 0.
func utcDay(t time.Time) int32 {
	s := t.UTC().Unix()
	d := s / 86400
	if s%86400 < 0 {
		d--
	}
	return int32(d)
}

// coldMarkMayDrop reports, with no barrier, whether the cold mark could let
// this read leave out cold partitions it cannot list: a cluster node, a
// trusted mark, a stated start, and the scope unmarked or its mark before the
// start's day (raft.MarkUnknownDay is MaxInt32: never before). The end is
// ignored: a read open at the end needs every day from its start.
func (h *QueryHandler) coldMarkMayDrop(database, measurement string, timeRange *pruning.TimeRange) bool {
	if h.coldMark == nil {
		return false
	}
	start, _ := statedBounds(timeRange)
	if start == nil || !h.clusterColdMarkTrusted(time.Time{}) {
		return false
	}
	day, _, has := h.coldMark.ColdMarkDay(database + "/" + measurement)
	return !has || utcDay(*start) > day
}

// clusterColdRangeEmpty reports whether the trusted cold mark rules out cold
// data for this read. coldMarkMayDrop first, at no Raft cost: a read it
// rules out pays no barrier. Then one coalesced read barrier, so the mark is
// not older than the cluster's, and the trust check and the mark again, read
// after it.
func (h *QueryHandler) clusterColdRangeEmpty(ctx context.Context, database, measurement string, timeRange *pruning.TimeRange) bool {
	if !h.coldMarkMayDrop(database, measurement, timeRange) {
		return false
	}
	barrierAt := time.Now()
	if err := h.coldMarkState.barrier.do(ctx, h.coldMark.ColdMarkReadBarrier); err != nil {
		h.logger.Warn().Err(err).Str("database", database).Str("measurement", measurement).
			Msg("Could not confirm this node is caught up with the cluster; keeping the cold partitions the cold tier could not list")
		return false
	}
	if !h.clusterColdMarkTrusted(barrierAt) {
		return false
	}
	start, _ := statedBounds(timeRange)
	day, _, has := h.coldMark.ColdMarkDay(database + "/" + measurement)
	return !has || utcDay(*start) > day
}

// clusterColdMayHaveData reports whether a cluster node must read the cold
// tier for a measurement its tier rows hold no cold row for: the trusted mark
// was raised within coldMarkRecentRaise (a cold write this node's rows may
// not know of yet) and the read's range may reach it (no stated start, an
// unknown day, or a start on or before the mark's day). Otherwise the rows
// decide, as before. No barrier: a lagging mark can only answer as the rows
// did before this change.
func (h *QueryHandler) clusterColdMayHaveData(database, measurement string, timeRange *pruning.TimeRange) bool {
	if h.coldMark == nil {
		return false
	}
	day, raisedAt, has := h.coldMark.ColdMarkDay(database + "/" + measurement)
	if !has || raisedAt.IsZero() || time.Since(raisedAt) > coldMarkRecentRaise {
		return false
	}
	if !h.clusterColdMarkTrusted(time.Time{}) {
		return false
	}
	start, _ := statedBounds(timeRange)
	return start == nil || utcDay(*start) <= day
}

// barrierCoalescer runs one read barrier for every caller that arrives while
// none is running, and one more, started after it, for callers that arrive
// while one runs: a caller only ever joins a barrier that began after it
// arrived, which is what makes sharing it correct. At most one barrier per
// node is in flight, back to back under load.
type barrierCoalescer struct {
	mu      sync.Mutex
	running bool
	next    *barrierCall
}

type barrierCall struct {
	done chan struct{}
	err  error
}

func (b *barrierCoalescer) do(ctx context.Context, fn func(context.Context) error) error {
	b.mu.Lock()
	if b.next == nil {
		b.next = &barrierCall{done: make(chan struct{})}
	}
	c := b.next
	if !b.running {
		b.running, b.next = true, nil
		go b.run(c, fn)
	}
	b.mu.Unlock()
	select {
	case <-c.done:
		return c.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *barrierCoalescer) run(c *barrierCall, fn func(context.Context) error) {
	for {
		c.err = fn(context.Background()) // bounded by the barrier's own timeout
		close(c.done)
		b.mu.Lock()
		if b.next == nil {
			b.running = false
			b.mu.Unlock()
			return
		}
		c, b.next = b.next, nil
		b.mu.Unlock()
	}
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

// coldRangeEmpty is the pruner drop site's question: may this read leave out
// cold partitions the cold tier could not list? Standalone: tier rows
// (#1550). Cluster: the cold mark. viaMark says which answered.
func (h *QueryHandler) coldRangeEmpty(ctx context.Context, database, measurement string, timeRange *pruning.TimeRange) (empty, viaMark bool) {
	if h.coldMetadataTrusted() {
		return h.coldRangeHasNoRows(ctx, database, measurement, timeRange), false
	}
	return h.clusterColdRangeEmpty(ctx, database, measurement, timeRange), true
}
