package api

import (
	"bytes"
	"database/sql"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

// A continuous query's UUID output is written as text and its BLOB output as
// bytes: duckdb-go scans both as []byte, so the column type decides (#440).
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
	isUUID := cqUUIDColumns(rows, 3)
	if !isUUID[0] || isUUID[1] || isUUID[2] {
		t.Fatalf("UUID columns = %v, want [true false false]", isUUID)
	}
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	vals := make([]interface{}, 3)
	if err := rows.Scan(&vals[0], &vals[1], &vals[2]); err != nil {
		t.Fatal(err)
	}
	for i := range vals {
		if isUUID[i] {
			vals[i] = cqUUIDText(vals[i])
		}
	}
	if vals[0] != "6f1c2c1e-8a3b-4d2e-9f00-1234567890ab" {
		t.Errorf("UUID = %#v, want its text", vals[0])
	}
	if b, ok := vals[1].([]byte); !ok || !bytes.Equal(b, []byte{0xff, 0x00}) {
		t.Errorf("BLOB = %#v, want bytes ff00", vals[1])
	}
}
