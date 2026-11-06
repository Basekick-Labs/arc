package api

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/basekick-labs/arc/internal/pruning"
)

// #1491: the range the scanner reads from an epoch_ms / make_timestamp /
// to_timestamp bound holds the instant DuckDB itself compares the time
// column with, on both sides, with the session time zone pinned to UTC as
// Arc pins it (database/duckdb.go). A range that misses that instant by one
// microsecond at an hour boundary drops that hour's partition.
func TestEpochCallBoundsHoldDuckDBsInstant(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("SET TimeZone='UTC'"); err != nil {
		t.Fatal(err)
	}
	calls := []string{
		"epoch_ms(1790848800000)", "epoch_ms(1790852399999)", "epoch_ms(-1)", "epoch_ms(0)", "epoch_ms(5)",
		"epoch_ms(253402300799999)",
		"make_timestamp(1790848800000000)", "make_timestamp(1790852399999999)", "make_timestamp(-1)",
		"make_timestamp(253402300799999999)", "make_timestamp(-62135596800000000)",
		"to_timestamp(1790848800)", "to_timestamp(1790852399.999999)", "to_timestamp(1790856000.1234567)",
		"to_timestamp(-1.9)", "to_timestamp(-0.5)",
		// DuckDB rounds these half to even through a double: 0 and .000002.
		"to_timestamp(0.0000005)", "to_timestamp(0.0000015)",
		// The far end of the range, where the double is coarsest.
		"to_timestamp(253402300799.999)", "to_timestamp(253402300799)",
	}
	for _, call := range calls {
		var us int64
		if err := db.QueryRow("SELECT epoch_us(" + call + "::TIMESTAMP)").Scan(&us); err != nil {
			t.Fatalf("%s: %v", call, err)
		}
		duck := time.UnixMicro(us).UTC()
		for _, op := range []string{">=", ">", "<", "<="} {
			q := fmt.Sprintf("SELECT * FROM db.q WHERE time %s %s", op, call)
			tr := pruning.ExtractTimeRangeAt(q, time.Now())
			if tr == nil {
				t.Errorf("%s: no range", q)
				continue
			}
			switch op {
			case ">=", ">":
				// Every row DuckDB returns is at or after duck: the start must not be later.
				if tr.Start.After(duck) {
					t.Errorf("%s: start %v is after DuckDB's %v", q, tr.Start, duck)
				}
			case "<":
				// Rows are before duck: the exclusive end must not be earlier.
				if tr.End.Before(duck) {
					t.Errorf("%s: end %v is before DuckDB's %v", q, tr.End, duck)
				}
			case "<=":
				// Rows reach duck itself: the exclusive end must be past it.
				if !tr.End.After(duck) {
					t.Errorf("%s: end %v is not past DuckDB's %v", q, tr.End, duck)
				}
			}
		}
	}
}
