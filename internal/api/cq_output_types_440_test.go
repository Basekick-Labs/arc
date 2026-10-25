package api

import (
	"bytes"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// A continuous query's UUID output is written as text and its BLOB output as
// bytes: duckdb-go scans both as []byte, so the column type decides (#440).
// Through scanCQRecords, the scan executeAggregation runs.
func TestCQOutputUUIDAsTextBlobAsBytes(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT '6f1c2c1e-8a3b-4d2e-9f00-1234567890ab'::UUID AS u, '\xFF\x00'::BLOB AS b, 'x' AS s`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	records, err := scanCQRecords(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	rec := records[0]
	if rec["u"] != "6f1c2c1e-8a3b-4d2e-9f00-1234567890ab" {
		t.Errorf("UUID = %#v, want its text", rec["u"])
	}
	if b, ok := rec["b"].([]byte); !ok || !bytes.Equal(b, []byte{0xff, 0x00}) {
		t.Errorf("BLOB = %#v, want bytes ff00", rec["b"])
	}
	if rec["s"] != "x" {
		t.Errorf("VARCHAR = %#v", rec["s"])
	}
}
