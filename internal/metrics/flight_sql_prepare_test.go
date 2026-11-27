package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestFlightSQLPrepareMetrics(t *testing.T) {
	m := &Metrics{startTime: time.Now()}
	m.ObserveFlightSQLBind(FlightSQLBindOK, 2*time.Millisecond)
	m.ObserveFlightSQLBind(FlightSQLBindOK, 10*time.Millisecond) // on a bound
	m.ObserveFlightSQLBind(FlightSQLBindUnimplemented, 3*time.Second)
	m.ObserveFlightSQLBind(FlightSQLBindTimeout, time.Minute)  // past every bound
	m.ObserveFlightSQLBind(FlightSQLBindInvalid, -time.Second) // a clock step counts as zero
	m.ObserveFlightSQLBind("typo", time.Second)                // dropped

	out := m.PrometheusFormat()
	for _, want := range []string{
		"# TYPE arc_flight_sql_prepare_binds_total counter\n",
		`arc_flight_sql_prepare_binds_total{result="ok"} 2` + "\n",
		`arc_flight_sql_prepare_binds_total{result="invalid"} 1` + "\n",
		`arc_flight_sql_prepare_binds_total{result="unimplemented"} 1` + "\n",
		`arc_flight_sql_prepare_binds_total{result="timeout"} 1` + "\n",
		`arc_flight_sql_prepare_binds_total{result="cancelled"} 0` + "\n",
		`arc_flight_sql_prepare_binds_total{result="failed"} 0` + "\n",
		"# TYPE arc_flight_sql_prepare_bind_duration_seconds histogram\n",
		`arc_flight_sql_prepare_bind_duration_seconds_bucket{le="0.001"} 1` + "\n", // the negative one
		`arc_flight_sql_prepare_bind_duration_seconds_bucket{le="0.0025"} 2` + "\n",
		`arc_flight_sql_prepare_bind_duration_seconds_bucket{le="0.01"} 3` + "\n", // a bound includes its value
		`arc_flight_sql_prepare_bind_duration_seconds_bucket{le="2.5"} 3` + "\n",
		`arc_flight_sql_prepare_bind_duration_seconds_bucket{le="5"} 4` + "\n",
		`arc_flight_sql_prepare_bind_duration_seconds_bucket{le="30"} 4` + "\n",
		`arc_flight_sql_prepare_bind_duration_seconds_bucket{le="+Inf"} 5` + "\n",
		"arc_flight_sql_prepare_bind_duration_seconds_count 5\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("Prometheus output is missing %q", want)
		}
	}
	// The exposition truncates to six decimals, so compare the number.
	var sum float64
	if _, err := fmt.Sscanf(grepLines(out, "bind_duration_seconds_sum "), "arc_flight_sql_prepare_bind_duration_seconds_sum %g", &sum); err != nil || sum < 63.011 || sum > 63.013 {
		t.Errorf("Prometheus sum = %q (%v), want 63.012", grepLines(out, "bind_duration_seconds_sum "), err)
	}
	if strings.Contains(out, "typo") {
		t.Error("an unknown result emitted a series")
	}
}
