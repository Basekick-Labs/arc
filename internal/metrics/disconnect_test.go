package metrics

import (
	"strings"
	"testing"
	"time"
)

// TestIncQueryClientDisconnect covers the three valid paths plus the
// silent-drop branch for an unknown label. The "silent drop" semantics
// matter for #426: if a typo at a call site (e.g. "arrow-json" instead
// of "arrow_json") were ever to land, we'd rather emit no count than
// pollute the labelled time series with a malformed value.
//
// Note on string-literal usage: the VALID-label calls use the typed
// constants (matching production call-site discipline); the INVALID
// calls use raw literals on purpose — typed constants would defeat
// the test (you can't typo a constant, so you can't test the silent-
// drop guard with one).
func TestIncQueryClientDisconnect(t *testing.T) {
	m := &Metrics{startTime: time.Now().UTC()}

	m.IncQueryClientDisconnect(DisconnectPathArrowIPC)
	m.IncQueryClientDisconnect(DisconnectPathArrowIPC)
	m.IncQueryClientDisconnect(DisconnectPathArrowJSON)
	m.IncQueryClientDisconnect(DisconnectPathSQLJSON)
	m.IncQueryClientDisconnect(DisconnectPathSQLJSON)
	m.IncQueryClientDisconnect(DisconnectPathSQLJSON)

	// Invalid labels: must NOT increment any counter. Raw literals
	// here on purpose — see comment above.
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
//
// startTime: time.Now().UTC() is required because PrometheusFormat
// emits arc_uptime_seconds derived from time.Since(m.startTime). A
// zero startTime yields ~9.2e9 seconds (~292 years) which is
// observationally harmless but misleading if a test ever asserts on
// uptime shape.
func TestPrometheusFormat_DisconnectMetric(t *testing.T) {
	m := &Metrics{startTime: time.Now().UTC()}
	m.IncQueryClientDisconnect(DisconnectPathArrowIPC)
	m.IncQueryClientDisconnect(DisconnectPathSQLJSON)
	m.IncQueryClientDisconnect(DisconnectPathSQLJSON)

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

// TestPrometheusFormat_StorageMetrics locks all five storage counters into
// the Prometheus text exposition format. Regression test for #349:
// arc_storage_reads_total and arc_storage_read_bytes_total were tracked
// (and present in the JSON snapshot) but missing from PrometheusFormat.
func TestPrometheusFormat_StorageMetrics(t *testing.T) {
	m := &Metrics{startTime: time.Now().UTC()}
	m.IncStorageWrites()
	m.IncStorageWriteBytes(100)
	m.IncStorageReads()
	m.IncStorageReads()
	m.IncStorageReadBytes(250)

	out := m.PrometheusFormat()

	for _, want := range []string{
		`# TYPE arc_storage_writes_total counter`,
		`arc_storage_writes_total 1`,
		`# TYPE arc_storage_write_bytes_total counter`,
		`arc_storage_write_bytes_total 100`,
		`# TYPE arc_storage_reads_total counter`,
		`arc_storage_reads_total 2`,
		`# TYPE arc_storage_read_bytes_total counter`,
		`arc_storage_read_bytes_total 250`,
		`# TYPE arc_storage_errors_total counter`,
		`arc_storage_errors_total 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("PrometheusFormat missing %q\n--- full output ---\n%s", want, out)
		}
	}
}
