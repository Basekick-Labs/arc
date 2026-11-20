package main

import "github.com/basekick-labs/arc/internal/database"

// duckDBCheckScanError marks a failure reading the startup check's row, so
// main reports it as "Failed to scan result" rather than as a failed query.
// Its text is the underlying error's.
type duckDBCheckScanError struct{ err error }

func (e duckDBCheckScanError) Error() string { return e.err.Error() }
func (e duckDBCheckScanError) Unwrap() error { return e.err }

// checkDuckDB runs the startup test query and returns its row; found is false
// when the query returned none. A query error is returned as is, a scan error
// as a duckDBCheckScanError.
//
// The result set is closed before it returns (#1782). main used to defer the
// close, and main does not return while Arc serves, so the pooled connection
// the query ran on stayed checked out for the life of the process: one
// connection fewer for queries, and none at all with database.max_connections=1.
// Only one row is read, so the result set never ends on its own; the deferred
// Close here is what gives the connection back.
func checkDuckDB(db *database.DuckDB) (message, version string, found bool, err error) {
	rows, err := db.Query("SELECT 'Hello from Arc in Go!' as message, version() as duckdb_version")
	if err != nil {
		return "", "", false, err
	}
	defer rows.Close()

	// No row is not an error, as before: main logs nothing and carries on.
	if !rows.Next() {
		return "", "", false, nil
	}
	if err := rows.Scan(&message, &version); err != nil {
		return "", "", false, duckDBCheckScanError{err}
	}
	return message, version, true, nil
}
