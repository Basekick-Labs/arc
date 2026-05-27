package metrics

import (
	"strings"
	"testing"
)

// TestIncQueryClientDisconnect covers the three valid paths plus the
// silent-drop branch for an unknown label. The "silent drop" semantics
// matter for #426: if a typo at a call site (e.g. "arrow-json" instead
// of "arrow_json") were ever to land, we'd rather emit no count than
// pollute the labelled time series with a malformed value.
func TestIncQueryClientDisconnect(t *testing.T) {
	m := &Metrics{}

	m.IncQueryClientDisconnect("arrow_ipc")
	m.IncQueryClientDisconnect("arrow_ipc")
	m.IncQueryClientDisconnect("arrow_json")
	m.IncQueryClientDisconnect("sql_json")
	m.IncQueryClientDisconnect("sql_json")
	m.IncQueryClientDisconnect("sql_json")

	// Invalid label: must NOT increment any counter.
	m.IncQueryClientDisconnect("arrow-json")
	m.IncQueryClientDisconnect("")

	if got, want := m.queryDisconnectsArrowIPC.Load(), int64(2); got != want {
		t.Errorf("arrow_ipc counter = %d, want %d", got, want)
	}
	if got, want := m.queryDisconnectsArrowJSON.Load(), int64(1); got != want {
		t.Errorf("arrow_json counter = %d, want %d", got, want)
	}
	if got, want := m.queryDisconnectsSQLJSON.Load(), int64(3); got != want {
		t.Errorf("sql_json counter = %d, want %d", got, want)
	}
}

// TestPrometheusFormat_DisconnectMetric verifies the three labelled
// time series are emitted in the Prometheus text exposition format
// even when the counters are zero (Prometheus convention: counters
// must be present so rate() over a fresh process doesn't produce
// gaps). Also locks in the metric name + label shape against
// accidental rename.
func TestPrometheusFormat_DisconnectMetric(t *testing.T) {
	m := &Metrics{}
	m.IncQueryClientDisconnect("arrow_ipc")
	m.IncQueryClientDisconnect("sql_json")
	m.IncQueryClientDisconnect("sql_json")

	out := m.PrometheusFormat()

	for _, want := range []string{
		`# TYPE arc_query_client_disconnects_total counter`,
		`arc_query_client_disconnects_total{path="arrow_ipc"} 1`,
		`arc_query_client_disconnects_total{path="arrow_json"} 0`,
		`arc_query_client_disconnects_total{path="sql_json"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("PrometheusFormat missing %q\n--- full output ---\n%s", want, out)
		}
	}
}
