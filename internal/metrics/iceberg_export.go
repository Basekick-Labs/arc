package metrics

import (
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

// icebergExportLagBounds are the histogram's upper bounds in seconds. The
// lag of an ingest-triggered export is about iceberg.ingest_export_interval
// (default 60) and that of a timed one about iceberg.reconcile_interval
// (default 300), so the bounds are dense between 5 s and 10 min and reach an
// hour for an export that has fallen behind.
var icebergExportLagBounds = [...]float64{
	1, 2, 5, 10, 15, 30, 45, 60, 90, 120, 180, 300, 600, 900, 1800, 3600,
}

var icebergExportLagLabels = func() []string {
	labels := make([]string, 0, len(icebergExportLagBounds)+1)
	for _, b := range icebergExportLagBounds {
		labels = append(labels, strconv.FormatFloat(b, 'g', -1, 64))
	}
	return append(labels, "+Inf")
}()

// icebergExport holds the Iceberg export's metrics. Buckets are not
// cumulative; exposition adds them up.
type icebergExport struct {
	buckets [len(icebergExportLagBounds) + 1]atomic.Int64
	sumNS   atomic.Int64
	backlog atomic.Int64
	// oldestPendingNS is the Unix time, in nanoseconds, of the oldest ingest
	// file reported to the export and not yet covered by one; 0: none.
	oldestPendingNS atomic.Int64
}

// ObserveIcebergExportLag records, for one ingest file an export has just
// published in an Iceberg table, the time from the file being written to the
// table version that holds it being published. A negative value (clocks)
// counts as zero. It only adds to atomics.
func (m *Metrics) ObserveIcebergExportLag(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h := &m.icebergExport
	h.sumNS.Add(int64(d)) // sum before bucket: a reader that sees the count sees its sum
	h.buckets[sort.SearchFloat64s(icebergExportLagBounds[:], d.Seconds())].Add(1)
}

// AddIcebergExportBacklogFiles counts ingest files an export published that
// were written before this process started: existing data being exported for
// the first time, or after a restart. Their age is not a lag of the running
// export, so they are counted here and kept out of the histogram.
func (m *Metrics) AddIcebergExportBacklogFiles(n int) {
	if n > 0 {
		m.icebergExport.backlog.Add(int64(n))
	}
}

// SetIcebergOldestPendingExport records when the oldest ingest file that is
// waiting for an export was reported; the zero time means none is waiting.
func (m *Metrics) SetIcebergOldestPendingExport(t time.Time) {
	if t.IsZero() {
		m.icebergExport.oldestPendingNS.Store(0)
		return
	}
	m.icebergExport.oldestPendingNS.Store(t.UnixNano())
}

func (m *Metrics) icebergExportLagCount() int64 {
	var n int64
	for i := range m.icebergExport.buckets {
		n += m.icebergExport.buckets[i].Load()
	}
	return n
}

// icebergExportPending is how long the oldest waiting ingest file has waited,
// as of now; zero when none is waiting.
func (m *Metrics) icebergExportPending(now time.Time) time.Duration {
	ns := m.icebergExport.oldestPendingNS.Load()
	if ns == 0 {
		return 0
	}
	if d := now.Sub(time.Unix(0, ns)); d > 0 {
		return d
	}
	return 0
}

func (m *Metrics) appendIcebergExport(b []byte) []byte {
	h := &m.icebergExport
	b = append(b, "# HELP arc_iceberg_export_lag_seconds Per ingest file published in an Iceberg table: time from the file being written to the table version that holds it being published. Files written before this process started are not included\n"...)
	b = append(b, "# TYPE arc_iceberg_export_lag_seconds histogram\n"...)
	var cumulative int64
	for i, label := range icebergExportLagLabels {
		cumulative += h.buckets[i].Load()
		b = appendMetricWithLabel(b, "arc_iceberg_export_lag_seconds_bucket", "le", label, float64(cumulative))
	}
	b = appendMetric(b, "arc_iceberg_export_lag_seconds_sum", time.Duration(h.sumNS.Load()).Seconds())
	b = appendMetric(b, "arc_iceberg_export_lag_seconds_count", float64(cumulative))

	b = append(b, "# HELP arc_iceberg_export_pending_seconds How long the oldest ingest file reported to the Iceberg export has waited for an export that covers it; 0 when none is waiting\n"...)
	b = append(b, "# TYPE arc_iceberg_export_pending_seconds gauge\n"...)
	b = appendMetric(b, "arc_iceberg_export_pending_seconds", m.icebergExportPending(time.Now()).Seconds())

	b = append(b, "# HELP arc_iceberg_export_backlog_files_total Ingest files published in an Iceberg table that were written before this process started (not in arc_iceberg_export_lag_seconds)\n"...)
	b = append(b, "# TYPE arc_iceberg_export_backlog_files_total counter\n"...)
	return appendMetric(b, "arc_iceberg_export_backlog_files_total", float64(h.backlog.Load()))
}
