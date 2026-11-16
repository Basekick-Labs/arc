//go:build duckdb_arrow && !flightbench

package api

import "testing"

// The direct benchmark arm skips RBAC, governance and audit. A build without
// the flightbench tag, which every release is, must not have it.
func TestFlightSQLBenchArmIsNotInOrdinaryBuilds(t *testing.T) {
	if flightSQLBenchDirect != nil {
		t.Fatal("flightSQLBenchDirect is set in a build without the flightbench tag")
	}
}
