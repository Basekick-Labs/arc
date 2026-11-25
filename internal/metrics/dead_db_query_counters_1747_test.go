package metrics

import (
	"testing"
	"time"
)

// deadDBQueryKeys were DuckDB-level query counters that nothing ever
// incremented. #802 took them off /metrics; #1747 removed them from the JSON
// surfaces, where they were still served as a permanent 0 next to the real
// query_requests_total / query_errors_total.
var deadDBQueryKeys = []string{"db_queries_total", "db_query_errors_total"}

func TestSnapshotHasNoDeadDBQueryCounters(t *testing.T) {
	snap := Get().Snapshot()
	for _, k := range deadDBQueryKeys {
		if _, ok := snap[k]; ok {
			t.Errorf("Snapshot() serves %q, a counter nothing increments (#1747)", k)
		}
	}
	// The real API-level counters stay.
	for _, k := range []string{"query_requests_total", "query_success_total", "query_errors_total"} {
		if _, ok := snap[k]; !ok {
			t.Errorf("Snapshot() is missing %q", k)
		}
	}
}

func TestAPITimeSeriesHasNoDeadDBQueryCounters(t *testing.T) {
	c := NewTimeSeriesCollector(10, time.Hour)
	c.collect()
	pts := c.GetAPI(1)
	if len(pts) != 1 {
		t.Fatalf("collect() produced %d api points, want 1", len(pts))
	}
	for _, k := range deadDBQueryKeys {
		if _, ok := pts[0].Values[k]; ok {
			t.Errorf("api timeseries serves %q, a counter nothing increments (#1747)", k)
		}
	}
}
