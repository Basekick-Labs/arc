package database

import (
	"context"
	"database/sql"
	"testing"
)

// readPIO returns preserve_insertion_order as seen by the given connection.
func readPIO(t *testing.T, ctx context.Context, conn *sql.Conn) bool {
	t.Helper()
	var v bool
	if err := conn.QueryRowContext(ctx, "SELECT current_setting('preserve_insertion_order')").Scan(&v); err != nil {
		t.Fatalf("read preserve_insertion_order: %v", err)
	}
	return v
}

// TestForcePreserveInsertionOrderSessionScoped verifies the override is
// session-scoped: while connection A holds the forced override, connection B
// on the same database instance must still see the configured global (false).
// A global-scoped SET would leak to B — flipping the instance-wide setting
// during DELETE rewrites and racing concurrent rewrites into unordered
// output — so this is the discriminating assertion for the mechanism.
func TestForcePreserveInsertionOrderSessionScoped(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "SET GLOBAL preserve_insertion_order=false"); err != nil {
		t.Fatalf("set global: %v", err)
	}

	connA, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn A: %v", err)
	}
	defer connA.Close()
	connB, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn B: %v", err)
	}
	defer connB.Close()

	restore, err := ForcePreserveInsertionOrder(ctx, connA)
	if err != nil {
		t.Fatalf("ForcePreserveInsertionOrder: %v", err)
	}
	if !readPIO(t, ctx, connA) {
		t.Fatal("connection A does not see the forced override")
	}
	if readPIO(t, ctx, connB) {
		t.Fatal("override leaked beyond the session: connection B sees preserve_insertion_order=true")
	}

	restore()
	if readPIO(t, ctx, connA) {
		t.Fatal("restore did not clear the session override on connection A")
	}
	if readPIO(t, ctx, connB) {
		t.Fatal("restore corrupted the global: connection B sees preserve_insertion_order=true")
	}
}

// TestExecPreservingInsertionOrder verifies the statement wrapper: the
// statement runs under the override, errors propagate, no override survives
// for later pool users, and a true global is left untouched (no-op path).
func TestExecPreservingInsertionOrder(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	// Single connection so a session override leaked by the helper (or by its
	// statement-error path) would be visible to the follow-up queries below.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "SET GLOBAL preserve_insertion_order=false"); err != nil {
		t.Fatalf("set global: %v", err)
	}

	if err := ExecPreservingInsertionOrder(ctx, db, "CREATE TABLE t AS SELECT 1 AS x"); err != nil {
		t.Fatalf("ExecPreservingInsertionOrder: %v", err)
	}

	var val bool
	if err := db.QueryRowContext(ctx, "SELECT current_setting('preserve_insertion_order')").Scan(&val); err != nil {
		t.Fatalf("read setting: %v", err)
	}
	if val {
		t.Fatal("session override leaked into pool: preserve_insertion_order=true, want false")
	}

	// The table created through the helper must exist (statement really ran).
	var n int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("query table created via helper: %v", err)
	}
	if n != 1 {
		t.Fatalf("unexpected row count %d", n)
	}

	// Statement errors must propagate, and the error path must not leak the
	// override either (checked while the global is still false).
	if err := ExecPreservingInsertionOrder(ctx, db, "SELECT * FROM missing_table"); err == nil {
		t.Fatal("expected error for invalid statement, got nil")
	}
	if err := db.QueryRowContext(ctx, "SELECT current_setting('preserve_insertion_order')").Scan(&val); err != nil {
		t.Fatalf("read setting: %v", err)
	}
	if val {
		t.Fatal("statement-error path leaked the override: preserve_insertion_order=true, want false")
	}

	// With the global already true (operator set preserve_insertion_order=true),
	// the helper must be a no-op: the session value stays true afterwards
	// rather than being "restored" to false.
	if _, err := db.ExecContext(ctx, "SET GLOBAL preserve_insertion_order=true"); err != nil {
		t.Fatalf("set global true: %v", err)
	}
	if err := ExecPreservingInsertionOrder(ctx, db, "CREATE TABLE t2 AS SELECT 1 AS x"); err != nil {
		t.Fatalf("ExecPreservingInsertionOrder with global true: %v", err)
	}
	if err := db.QueryRowContext(ctx, "SELECT current_setting('preserve_insertion_order')").Scan(&val); err != nil {
		t.Fatalf("read setting: %v", err)
	}
	if !val {
		t.Fatal("helper flipped a true global to false: preserve_insertion_order=false, want true")
	}
}
