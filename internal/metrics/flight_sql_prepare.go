package metrics

import (
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

// Label values for arc_flight_sql_prepare_binds_total: how a Flight SQL
// prepare's bind ended. Exported so internal/api names them by constant.
const (
	// FlightSQLBindOK: the statement bound and its result schema was read.
	FlightSQLBindOK = "ok"
	// FlightSQLBindInvalid: DuckDB refused to bind the statement (an
	// unknown column, a parameter); the client is told InvalidArgument.
	FlightSQLBindInvalid = "invalid"
	// FlightSQLBindUnimplemented: the statement bound but Arc could not
	// give its schema without running it; the client is told
	// Unimplemented and ADBC runs it as a plain statement instead.
	FlightSQLBindUnimplemented = "unimplemented"
	// FlightSQLBindTimeout: the request's deadline passed during the bind.
	FlightSQLBindTimeout = "timeout"
	// FlightSQLBindCancelled: the client went away, or Arc is shutting down.
	FlightSQLBindCancelled = "cancelled"
	// FlightSQLBindFailed: anything else, such as a partition removed
	// during the bind.
	FlightSQLBindFailed = "failed"
)

var flightSQLBindResults = [...]string{
	FlightSQLBindOK, FlightSQLBindInvalid, FlightSQLBindUnimplemented,
	FlightSQLBindTimeout, FlightSQLBindCancelled, FlightSQLBindFailed,
}

// flightSQLBindBounds are the bind-duration histogram's upper bounds in
// seconds. A bind lists the measurement's files and reads Parquet footers: a
// few milliseconds on a small local store, seconds over many files on object
// storage.
var flightSQLBindBounds = [...]float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
}

var flightSQLBindLabels = func() []string {
	labels := make([]string, 0, len(flightSQLBindBounds)+1)
	for _, b := range flightSQLBindBounds {
		labels = append(labels, strconv.FormatFloat(b, 'g', -1, 64))
	}
	return append(labels, "+Inf")
}()

// flightSQLPrepare holds the Flight SQL prepare metrics. Buckets are not
// cumulative; exposition adds them up.
type flightSQLPrepare struct {
	results [len(flightSQLBindResults)]atomic.Int64
	buckets [len(flightSQLBindBounds) + 1]atomic.Int64
	sumNS   atomic.Int64
}

// ObserveFlightSQLBind records one Flight SQL prepare bind: how it ended
// (one of the FlightSQLBind* constants; any other value is dropped, so a typo
// cannot emit a malformed series) and how long it took.
func (m *Metrics) ObserveFlightSQLBind(result string, d time.Duration) {
	i := flightSQLBindResultIndex(result)
	if i < 0 {
		return
	}
	if d < 0 {
		d = 0
	}
	p := &m.flightSQLPrepare
	p.results[i].Add(1)
	// First bound >= the value: Prometheus "le" buckets include their bound.
	// Sum before bucket: a reader that sees the count sees its sum.
	p.sumNS.Add(int64(d))
	p.buckets[sort.SearchFloat64s(flightSQLBindBounds[:], d.Seconds())].Add(1)
}

func flightSQLBindResultIndex(result string) int {
	for i, r := range flightSQLBindResults {
		if r == result {
			return i
		}
	}
	return -1
}

// appendFlightSQLPrepare writes the bind counter and the duration histogram
// in Prometheus text format.
func (m *Metrics) appendFlightSQLPrepare(b []byte) []byte {
	p := &m.flightSQLPrepare
	b = append(b, "# HELP arc_flight_sql_prepare_binds_total Flight SQL prepared statements bound to read their result schema, by result\n"...)
	b = append(b, "# TYPE arc_flight_sql_prepare_binds_total counter\n"...)
	for i, result := range flightSQLBindResults {
		b = appendMetricWithLabel(b, "arc_flight_sql_prepare_binds_total", "result", result, float64(p.results[i].Load()))
	}

	b = append(b, "# HELP arc_flight_sql_prepare_bind_duration_seconds Time a Flight SQL prepare spent waiting for a bind slot, rewriting its SQL, binding it and reading the result schema\n"...)
	b = append(b, "# TYPE arc_flight_sql_prepare_bind_duration_seconds histogram\n"...)
	var cumulative int64
	for i, label := range flightSQLBindLabels {
		cumulative += p.buckets[i].Load()
		b = appendMetricWithLabel(b, "arc_flight_sql_prepare_bind_duration_seconds_bucket", "le", label, float64(cumulative))
	}
	b = appendMetric(b, "arc_flight_sql_prepare_bind_duration_seconds_sum", time.Duration(p.sumNS.Load()).Seconds())
	return appendMetric(b, "arc_flight_sql_prepare_bind_duration_seconds_count", float64(cumulative))
}
