package api

import (
	"database/sql"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/metrics"
)

func poolMetricInt(t *testing.T, key string) int64 {
	t.Helper()
	v, ok := metrics.Get().Snapshot()[key]
	if !ok {
		t.Fatalf("metric %q missing from snapshot", key)
	}
	n, ok := v.(int64)
	if !ok {
		t.Fatalf("metric %q is %T, want int64", key, v)
	}
	return n
}

// TestSampleDBStats_PublishesPoolGauges pins that the pool gauges are sampled
// from the wired source.
//
// Regression test for #809: arc_db_connections_open, _in_use and _queries_total
// were exported and never set, so /metrics and the `pool` block of
// /api/v1/metrics/query-pool reported zeros forever. Any "is DuckDB saturated?"
// dashboard built on connections_in_use / connections_open was 0/0.
func TestSampleDBStats_PublishesPoolGauges(t *testing.T) {
	s := &Server{}
	s.SetDBStats(func() sql.DBStats {
		return sql.DBStats{
			MaxOpenConnections: 16,
			OpenConnections:    7,
			InUse:              5,
			Idle:               2,
			WaitCount:          3,
			WaitDuration:       250 * time.Millisecond,
		}
	})

	s.sampleDBStats()

	for _, tc := range []struct {
		key  string
		want int64
	}{
		{"db_connections_max", 16},
		{"db_connections_open", 7},
		{"db_connections_in_use", 5},
		{"db_connections_idle", 2},
		{"db_wait_count", 3},
		{"db_wait_micros", 250_000},
	} {
		if got := poolMetricInt(t, tc.key); got != tc.want {
			t.Errorf("%s = %d, want %d: the DuckDB pool gauges are not sampled from the wired source (#809)", tc.key, got, tc.want)
		}
	}
}

// TestSampleDBStats_NoSourceIsSafe pins that a server with no database wired —
// a test server, or any code path that never calls SetDBStats — does not panic
// when metrics are read.
func TestSampleDBStats_NoSourceIsSafe(t *testing.T) {
	s := &Server{}
	// Must not panic: dbStats is nil.
	s.sampleDBStats()
}

// TestSampleDBStats_SampleIsCoherent pins that one sample lands atomically.
//
// The gauges are set from a single sql.DBStats value rather than through a
// setter per field, so a reader can never observe OpenConnections from one
// instant against InUse from another and conclude in_use > open.
func TestSampleDBStats_SampleIsCoherent(t *testing.T) {
	s := &Server{}
	s.SetDBStats(func() sql.DBStats {
		return sql.DBStats{OpenConnections: 4, InUse: 3, Idle: 1}
	})
	s.sampleDBStats()

	open := poolMetricInt(t, "db_connections_open")
	inUse := poolMetricInt(t, "db_connections_in_use")
	idle := poolMetricInt(t, "db_connections_idle")

	if inUse > open {
		t.Fatalf("in_use (%d) > open (%d): the sample is not coherent", inUse, open)
	}
	if inUse+idle != open {
		t.Fatalf("in_use (%d) + idle (%d) != open (%d): the sample is not coherent", inUse, idle, open)
	}
}
