package metrics

import (
	"strings"
	"time"
)

// Iceberg export lag per table (#1772): labelled series for at most n
// tables, Prometheus text only. JSON snapshots never carry them: they hold
// database and measurement names, which no other Arc metric does.

// IcebergTableLag is one table's value in a labelled export lag series.
type IcebergTableLag struct {
	Database    string
	Measurement string
	Seconds     float64
}

// IcebergTableLagSource reports the tables for the labelled series, worst
// first, at most n of each. Called at scrape time with no metrics lock held.
type IcebergTableLagSource interface {
	// PendingTables: tables whose oldest file waiting for an export has
	// waited past the source's floor; total counts all of them.
	PendingTables(n int, now time.Time) (top []IcebergTableLag, total int)
	// LastLagTables: per table, the longest lag among the files its last
	// publish covered (recent publishes only).
	LastLagTables(n int, now time.Time) []IcebergTableLag
}

type icebergTableLagSource struct {
	src IcebergTableLagSource
	n   int
}

// SetIcebergTableLagSource enables the per-table series for at most n
// tables; n <= 0 or a nil source disables them.
func (m *Metrics) SetIcebergTableLagSource(src IcebergTableLagSource, n int) {
	if src == nil || n <= 0 {
		m.icebergTableLag.Store(nil)
		return
	}
	m.icebergTableLag.Store(&icebergTableLagSource{src: src, n: n})
}

func (m *Metrics) appendIcebergExportTables(b []byte) []byte {
	s := m.icebergTableLag.Load()
	if s == nil {
		return b
	}
	now := time.Now()
	pending, total := s.src.PendingTables(s.n, now)
	b = append(b, "# HELP arc_iceberg_export_tables_pending Tables whose oldest file waiting for an Iceberg export has waited past the floor (twice iceberg.ingest_export_interval, or twice iceberg.reconcile_interval where the ingest trigger is off, as on a cluster); the labelled series show at most iceberg.export_lag_table_series of them\n"...)
	b = append(b, "# TYPE arc_iceberg_export_tables_pending gauge\n"...)
	b = appendMetric(b, "arc_iceberg_export_tables_pending", float64(total))
	b = append(b, "# HELP arc_iceberg_export_table_pending_seconds Per table, how long its oldest file waiting for an Iceberg export has waited: since it was reported (standalone, with the ingest trigger on) or written (committed but not yet published, files written after this process started). Only the tables waiting longest\n"...)
	b = append(b, "# TYPE arc_iceberg_export_table_pending_seconds gauge\n"...)
	for _, t := range pending {
		b = appendTableMetric(b, "arc_iceberg_export_table_pending_seconds", t)
	}
	b = append(b, "# HELP arc_iceberg_export_table_last_lag_seconds Per table, the longest time from an ingest file being written to its table version being published, among the files its last publish covered (written after this process started). Only recent publishes, the longest first; a table missing here is not known to be healthy\n"...)
	b = append(b, "# TYPE arc_iceberg_export_table_last_lag_seconds gauge\n"...)
	for _, t := range s.src.LastLagTables(s.n, now) {
		b = appendTableMetric(b, "arc_iceberg_export_table_last_lag_seconds", t)
	}
	return b
}

func appendTableMetric(b []byte, name string, t IcebergTableLag) []byte {
	b = append(b, name...)
	b = append(b, `{database="`...)
	b = appendLabelValue(b, t.Database)
	b = append(b, `",measurement="`...)
	b = appendLabelValue(b, t.Measurement)
	b = append(b, '"', '}', ' ')
	b = appendFloat(b, t.Seconds)
	return append(b, '\n')
}

// appendLabelValue writes v as a Prometheus text-format label value: valid
// UTF-8 (one bad byte would make the whole scrape unparseable), with
// backslash, double quote and newline escaped. Names come from data.
func appendLabelValue(b []byte, v string) []byte {
	v = strings.ToValidUTF8(v, "�")
	for i := 0; i < len(v); i++ {
		switch c := v[i]; c {
		case '\\':
			b = append(b, '\\', '\\')
		case '"':
			b = append(b, '\\', '"')
		case '\n':
			b = append(b, '\\', 'n')
		default:
			b = append(b, c)
		}
	}
	return b
}
