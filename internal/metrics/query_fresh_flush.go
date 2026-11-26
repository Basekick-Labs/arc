package metrics

import (
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

// queryFreshFlushLabels are the outcomes of a query that carried
// x-arc-freshness (#1760): the labels of arc_query_fresh_flush_total, each
// the X-Arc-Freshness-Reason the client got ("flushed" on success). A fixed
// set, so the series count never grows.
var queryFreshFlushLabels = [...]string{
	"flushed",
	"invalid_value", "disabled", "unsupported_route",
	"cluster", "storage_not_supported", "tiering_not_supported",
	"show_not_supported", "forbidden", "too_many_measurements", "busy", "query_error",
	"flush_failed_wal_replay", "recovery_pending", "flush_failed", "not_confirmed",
	"timeout", "shutdown", "cancelled", "fresh_flush_unavailable",
}

// QueryFreshFlushOutcome reports whether outcome is a label of
// arc_query_fresh_flush_total.
func QueryFreshFlushOutcome(outcome string) bool { return freshFlushOutcomeIndex(outcome) >= 0 }

func freshFlushOutcomeIndex(outcome string) int {
	for i, l := range queryFreshFlushLabels {
		if l == outcome {
			return i
		}
	}
	return -1
}

// queryFreshFlushBounds are the wait histogram's upper bounds in seconds. The
// first is under a millisecond, so a fresh query that had nothing to wait for
// shows as such.
var queryFreshFlushBounds = [...]float64{0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 7.5, 10, 15, 20, 30}

var queryFreshFlushBucketLabels = func() []string {
	labels := make([]string, 0, len(queryFreshFlushBounds)+1)
	for _, b := range queryFreshFlushBounds {
		labels = append(labels, strconv.FormatFloat(b, 'g', -1, 64))
	}
	return append(labels, "+Inf")
}()

type queryFreshFlush struct {
	outcomes [len(queryFreshFlushLabels)]atomic.Int64
	waiters  atomic.Int64
	buckets  [len(queryFreshFlushBounds) + 1]atomic.Int64
	sumNS    atomic.Int64
}

// IncQueryFreshFlush counts one query that carried x-arc-freshness by
// outcome (its answer's reason, or "flushed"). An unknown outcome is dropped.
func (m *Metrics) IncQueryFreshFlush(outcome string) {
	if i := freshFlushOutcomeIndex(outcome); i >= 0 {
		m.queryFreshFlush.outcomes[i].Add(1)
	}
}

// AddQueryFreshFlushWaiters moves the gauge of fresh queries waiting.
func (m *Metrics) AddQueryFreshFlushWaiters(n int64) { m.queryFreshFlush.waiters.Add(n) }

// ObserveQueryFreshFlushWait records how long a fresh query waited for its
// measurements' flushes, whatever the outcome. Negative counts as zero.
func (m *Metrics) ObserveQueryFreshFlushWait(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h := &m.queryFreshFlush
	h.sumNS.Add(int64(d)) // sum before bucket: a reader that sees the count sees its sum
	h.buckets[sort.SearchFloat64s(queryFreshFlushBounds[:], d.Seconds())].Add(1)
}

func (m *Metrics) appendQueryFreshFlush(b []byte) []byte {
	h := &m.queryFreshFlush
	b = append(b, "# HELP arc_query_fresh_flush_total Queries that carried x-arc-freshness, by outcome: the X-Arc-Freshness-Reason of the answer, or flushed (200 after the wait)\n"...)
	b = append(b, "# TYPE arc_query_fresh_flush_total counter\n"...)
	for i, label := range queryFreshFlushLabels {
		b = appendMetricWithLabel(b, "arc_query_fresh_flush_total", "outcome", label, float64(h.outcomes[i].Load()))
	}
	b = append(b, "# HELP arc_query_fresh_flush_waiters Fresh queries (x-arc-freshness: flush) waiting for their measurements' flushes\n"...)
	b = append(b, "# TYPE arc_query_fresh_flush_waiters gauge\n"...)
	b = appendMetric(b, "arc_query_fresh_flush_waiters", float64(h.waiters.Load()))
	b = append(b, "# HELP arc_query_fresh_flush_seconds Per fresh query that waited: time spent waiting for its measurements' flushes, before its SQL was built. These requests are left out of the HTTP latency metrics, and their execution time starts after the wait\n"...)
	b = append(b, "# TYPE arc_query_fresh_flush_seconds histogram\n"...)
	var cumulative int64
	for i, label := range queryFreshFlushBucketLabels {
		cumulative += h.buckets[i].Load()
		b = appendMetricWithLabel(b, "arc_query_fresh_flush_seconds_bucket", "le", label, float64(cumulative))
	}
	b = appendMetric(b, "arc_query_fresh_flush_seconds_sum", time.Duration(h.sumNS.Load()).Seconds())
	return appendMetric(b, "arc_query_fresh_flush_seconds_count", float64(cumulative))
}
