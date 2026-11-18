package metrics

import (
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

// ingestPublishDelayBounds are the histogram's upper bounds in seconds. They
// are dense around the two settings that matter: ingest.max_buffer_age_ms at
// its default (5 s: the bounds at 5.5, 6 and 7.5 keep a quantile off the 10 s
// edge) and at 100 ms (0.15, 0.2, 0.25, 0.35). The long tail (120, 300) is for
// object storage and a full flush queue.
var ingestPublishDelayBounds = [...]float64{
	0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5, 0.75,
	1, 2, 3, 5, 5.5, 6, 7.5, 10, 15, 30, 60, 120, 300,
}

// ingestPublishDelayLabels are the bounds as Prometheus "le" values, built
// from the bounds so the two cannot drift apart.
var ingestPublishDelayLabels = func() []string {
	labels := make([]string, 0, len(ingestPublishDelayBounds)+1)
	for _, b := range ingestPublishDelayBounds {
		labels = append(labels, strconv.FormatFloat(b, 'g', -1, 64))
	}
	return append(labels, "+Inf")
}()

// ingestPublishDelay is the histogram behind arc_ingest_publish_delay_seconds.
// Buckets are not cumulative; exposition adds them up.
type ingestPublishDelay struct {
	buckets [len(ingestPublishDelayBounds) + 1]atomic.Int64
	sumNS   atomic.Int64
	maxNS   atomic.Int64
}

// ObserveIngestPublishDelay records, for one Parquet file an ingest flush
// published, how long the oldest row of the flushed buffer waited: from the
// buffer being created to the file being in this node's storage. The caller
// (ingest.observePublishDelay) says which files are observed and when. A
// negative value (a wall-clock step; the start is a wall-clock time) counts
// as zero.
//
// It only adds to atomics: it cannot fail or block, so the flush that calls
// it behaves the same with or without it.
func (m *Metrics) ObserveIngestPublishDelay(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h := &m.ingestPublishDelay
	// First bound >= the value: Prometheus "le" buckets include their bound.
	// Sum before bucket: a reader that sees the count sees its sum.
	h.sumNS.Add(int64(d))
	h.buckets[sort.SearchFloat64s(ingestPublishDelayBounds[:], d.Seconds())].Add(1)
	for {
		cur := h.maxNS.Load()
		if int64(d) <= cur || h.maxNS.CompareAndSwap(cur, int64(d)) {
			return
		}
	}
}

// ingestPublishDelayCount is the number of observations.
func (m *Metrics) ingestPublishDelayCount() int64 {
	var n int64
	for i := range m.ingestPublishDelay.buckets {
		n += m.ingestPublishDelay.buckets[i].Load()
	}
	return n
}

// appendIngestPublishDelay writes the histogram and its max gauge in
// Prometheus text format. _count is the cumulative total of the buckets just
// written, so it always equals the +Inf bucket.
func (m *Metrics) appendIngestPublishDelay(b []byte) []byte {
	h := &m.ingestPublishDelay
	b = append(b, "# HELP arc_ingest_publish_delay_seconds Per Parquet file published by an ingest flush: time from the oldest row of its buffer being buffered to the file being in this node's storage\n"...)
	b = append(b, "# TYPE arc_ingest_publish_delay_seconds histogram\n"...)
	var cumulative int64
	for i, label := range ingestPublishDelayLabels {
		cumulative += h.buckets[i].Load()
		b = appendMetricWithLabel(b, "arc_ingest_publish_delay_seconds_bucket", "le", label, float64(cumulative))
	}
	b = appendMetric(b, "arc_ingest_publish_delay_seconds_sum", time.Duration(h.sumNS.Load()).Seconds())
	b = appendMetric(b, "arc_ingest_publish_delay_seconds_count", float64(cumulative))

	b = append(b, "# HELP arc_ingest_publish_delay_max_seconds Largest arc_ingest_publish_delay_seconds observation since this process started\n"...)
	b = append(b, "# TYPE arc_ingest_publish_delay_max_seconds gauge\n"...)
	return appendMetric(b, "arc_ingest_publish_delay_max_seconds", time.Duration(h.maxNS.Load()).Seconds())
}
