package metrics

import (
	"sort"
	"sync/atomic"
	"time"
)

// Outcomes of a write that asked to wait until its rows are visible
// (x-arc-wait: visible, #1759): the labels of arc_ingest_wait_visible_total.
const (
	WaitVisibleVisible = iota
	WaitVisibleEmpty
	WaitVisibleTimeout
	WaitVisibleShutdown
	WaitVisibleRowsAtRisk
	WaitVisibleWALReplay
	WaitVisibleNotConfirmed
	WaitVisibleStorageNotConfirmable
	WaitVisibleFutureStamped
	WaitVisibleBusy
	// WaitVisiblePartial: a partial write (400) or a write stopped part-way
	// (500); answered at once, never waited on.
	WaitVisiblePartial
	waitVisibleOutcomes
)

var waitVisibleLabels = [waitVisibleOutcomes]string{
	"visible", "empty", "timeout", "shutdown", "rows_at_risk", "wal_replay",
	"not_confirmed", "storage_not_confirmable", "future_stamped", "busy", "partial",
}

// ingestWaitVisible holds the metrics of visible writes. The histogram reuses
// arc_ingest_publish_delay_seconds' bounds: a waiting write is answered after
// its rows' flush, so the two measure the same delay from different ends.
type ingestWaitVisible struct {
	outcomes [waitVisibleOutcomes]atomic.Int64
	waiters  atomic.Int64
	buckets  [len(ingestPublishDelayBounds) + 1]atomic.Int64
	sumNS    atomic.Int64
}

// IncIngestWaitVisible counts one visible write by outcome (WaitVisible*).
func (m *Metrics) IncIngestWaitVisible(outcome int) {
	if outcome >= 0 && outcome < waitVisibleOutcomes {
		m.ingestWaitVisible.outcomes[outcome].Add(1)
	}
}

// AddIngestWaitVisibleWaiters moves the gauge of visible writes in progress.
func (m *Metrics) AddIngestWaitVisibleWaiters(n int64) {
	m.ingestWaitVisible.waiters.Add(n)
}

// ObserveIngestWaitVisible records how long a visible write waited for its
// verdict. A negative value counts as zero. It only adds to atomics.
func (m *Metrics) ObserveIngestWaitVisible(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h := &m.ingestWaitVisible
	h.sumNS.Add(int64(d)) // sum before bucket: a reader that sees the count sees its sum
	h.buckets[sort.SearchFloat64s(ingestPublishDelayBounds[:], d.Seconds())].Add(1)
}

func (m *Metrics) appendIngestWaitVisible(b []byte) []byte {
	h := &m.ingestWaitVisible
	b = append(b, "# HELP arc_ingest_wait_visible_total Writes that asked to wait until their rows are visible (x-arc-wait: visible), by outcome: visible and empty (204), timeout, shutdown, wal_replay, not_confirmed, storage_not_confirmable and future_stamped (202), rows_at_risk (500), busy (503, nothing written), partial (a partial or stopped write, not waited on)\n"...)
	b = append(b, "# TYPE arc_ingest_wait_visible_total counter\n"...)
	for i, label := range waitVisibleLabels {
		b = appendMetricWithLabel(b, "arc_ingest_wait_visible_total", "outcome", label, float64(h.outcomes[i].Load()))
	}
	b = append(b, "# HELP arc_ingest_wait_visible_waiters Writes waiting until their rows are visible, from just before admission to their answer\n"...)
	b = append(b, "# TYPE arc_ingest_wait_visible_waiters gauge\n"...)
	b = appendMetric(b, "arc_ingest_wait_visible_waiters", float64(h.waiters.Load()))
	b = append(b, "# HELP arc_ingest_wait_visible_seconds Per write that asked to wait until its rows are visible: time from the end of its write call to its verdict. These writes are left out of the HTTP latency metrics\n"...)
	b = append(b, "# TYPE arc_ingest_wait_visible_seconds histogram\n"...)
	var cumulative int64
	for i, label := range ingestPublishDelayLabels {
		cumulative += h.buckets[i].Load()
		b = appendMetricWithLabel(b, "arc_ingest_wait_visible_seconds_bucket", "le", label, float64(cumulative))
	}
	b = appendMetric(b, "arc_ingest_wait_visible_seconds_sum", time.Duration(h.sumNS.Load()).Seconds())
	return appendMetric(b, "arc_ingest_wait_visible_seconds_count", float64(cumulative))
}
