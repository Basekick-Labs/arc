package metrics

import (
	"strings"
	"testing"
	"time"
)

func TestRecordQueryEngineExecution(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.RecordQueryEngineExecution(QueryEngineDuckDB, true, 20_000)
	m.RecordQueryEngineExecution(QueryEngineArcx, false, 75_000)
	// Unknown engines must not create unbounded label values.
	m.RecordQueryEngineExecution("user-supplied", true, 1)

	out := m.PrometheusFormat()
	for _, want := range []string{
		`arc_query_engine_executions_total{engine="duckdb",outcome="success"} 1`,
		`arc_query_engine_executions_total{engine="duckdb",outcome="error"} 0`,
		`arc_query_engine_executions_total{engine="arcx",outcome="success"} 0`,
		`arc_query_engine_executions_total{engine="arcx",outcome="error"} 1`,
		`arc_query_latency_seconds_bucket{engine="duckdb",le="0.05"} 1`,
		`arc_query_latency_seconds_bucket{engine="arcx",le="0.1"} 1`,
		`arc_query_latency_seconds_bucket{engine="arcx",le="+Inf"} 1`,
		`arc_query_latency_seconds_sum{engine="duckdb"} 0.02`,
		`arc_query_latency_seconds_count{engine="arcx"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("PrometheusFormat missing %q", want)
		}
	}
	if strings.Contains(out, `engine="user-supplied"`) {
		t.Fatal("unknown engine escaped the closed label set")
	}

	snapshot := m.Snapshot()
	byEngine := snapshot["query_engine_executions"].(map[string]map[string]int64)
	if got := byEngine[QueryEngineArcx]["error"]; got != 1 {
		t.Errorf("arcx errors in JSON snapshot = %d, want 1", got)
	}
}

func TestRecordQueryLatencyRecordsDuckDBHistogram(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.RecordQueryLatency(3_000_000)

	out := m.PrometheusFormat()
	for _, want := range []string{
		`arc_query_latency_seconds_bucket{engine="duckdb",le="5"} 1`,
		`arc_query_latency_seconds_sum{engine="duckdb"} 3`,
		`arc_query_latency_seconds_count{engine="duckdb"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("PrometheusFormat missing %q", want)
		}
	}
}

func TestRecordQueryLatencyForEngineRecordsArcxSuccess(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.RecordQueryLatencyForEngine(QueryEngineArcx, 150_000)

	snapshot := m.Snapshot()
	byEngine := snapshot["query_engine_executions"].(map[string]map[string]int64)
	if got := byEngine[QueryEngineArcx]["success"]; got != 1 {
		t.Errorf("arcx successes = %d, want 1", got)
	}
	if got := snapshot["query_latency_sum_us"].(int64); got != 150_000 {
		t.Errorf("aggregate latency sum = %d, want 150000", got)
	}
	if got := snapshot["query_latency_count"].(int64); got != 1 {
		t.Errorf("aggregate latency count = %d, want 1", got)
	}

	out := m.PrometheusFormat()
	for _, want := range []string{
		`arc_query_latency_seconds_bucket{engine="arcx",le="0.25"} 1`,
		`arc_query_latency_seconds_sum{engine="arcx"} 0.15`,
		`arc_query_latency_seconds_count{engine="arcx"} 1`,
		`arc_query_engine_executions_total{engine="arcx",outcome="success"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("PrometheusFormat missing %q", want)
		}
	}
}

// Regression test for #823. Before query latency was exported as a histogram,
// the latency was available only as a lifetime average in the JSON snapshot.
func TestRecordQueryLatencyExportsHistogram(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.RecordQueryLatency(75_000)

	out := m.PrometheusFormat()
	for _, want := range []string{
		`arc_query_latency_seconds_bucket{engine="duckdb",le="0.1"} 1`,
		`arc_query_latency_seconds_sum{engine="duckdb"} 0.075`,
		`arc_query_latency_seconds_count{engine="duckdb"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("PrometheusFormat missing %q", want)
		}
	}
}
