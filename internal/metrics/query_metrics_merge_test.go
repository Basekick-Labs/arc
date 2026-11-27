package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestColdSkipAndQueryEngineMetricsCoexist(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.IncQueryColdUnlistedSkipped()
	m.IncQueryColdMarkSkipped()
	m.RecordQueryEngineExecution(QueryEngineDuckDB, true, 1_000)

	snapshot := m.Snapshot()
	if got := snapshot["query_cold_unlisted_skipped_total"]; got != int64(1) {
		t.Fatalf("cold skips = %v, want 1", got)
	}
	if got := snapshot["query_cold_mark_skipped_total"]; got != int64(1) {
		t.Fatalf("cold mark skips = %v, want 1", got)
	}
	engines := snapshot["query_engine_executions"].(map[string]map[string]int64)
	if got := engines[QueryEngineDuckDB]["success"]; got != 1 {
		t.Fatalf("DuckDB successes = %d, want 1", got)
	}
	for _, want := range []string{
		"arc_query_cold_unlisted_skipped_total 1",
		"arc_query_cold_mark_skipped_total 1",
		`arc_query_engine_executions_total{engine="duckdb",outcome="success"} 1`,
	} {
		if !strings.Contains("\n"+m.PrometheusFormat(), "\n"+want+"\n") {
			t.Errorf("Prometheus output missing %q", want)
		}
	}
}
