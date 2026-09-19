package fieldschema

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	_ "github.com/duckdb/duckdb-go/v2"
)

func TestAnchorRoundTripAndDuckDBTypes(t *testing.T) {
	s := Normalize(fields("time", tTsTZ, "host", tStr, "value", tFloat, "count", tInt64, "ok", tBool, "price", tDec186), []string{"host"})
	data, err := EncodeAnchor(s)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeAnchor(data)
	if err != nil {
		t.Fatal(err)
	}
	if Fingerprint(back) != Fingerprint(s) {
		t.Fatalf("round trip changed the schema:\n%v\n%v", s, back)
	}
	path := filepath.Join(t.TempDir(), "anchor.parquet")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query("DESCRIBE SELECT * FROM read_parquet('" + filepath.ToSlash(path) + "', union_by_name=true)")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[string]string{"time": "TIMESTAMP WITH TIME ZONE", "host": "VARCHAR", "value": "DOUBLE", "count": "BIGINT", "ok": "BOOLEAN", "price": "DECIMAL(18,6)"}
	var order []string
	for rows.Next() {
		var name, typ string
		var a, b, c, d interface{}
		if err := rows.Scan(&name, &typ, &a, &b, &c, &d); err != nil {
			t.Fatal(err)
		}
		order = append(order, name)
		if want[name] != typ {
			t.Errorf("%s: DuckDB type %s, want %s", name, typ, want[name])
		}
		// The API renders the same names DuckDB does.
		if got := DuckDBTypeName(s.Field(s.FieldIndices(name)[0]).Type); got != typ {
			t.Errorf("%s: DuckDBTypeName %s vs DuckDB %s", name, got, typ)
		}
		mapped, ok := ArrowTypeFromDuckDB(typ)
		if !ok || !arrow.TypeEqual(mapped, s.Field(s.FieldIndices(name)[0]).Type) {
			t.Errorf("%s: DESCRIBE type %s maps to %v, want %s", name, typ, mapped, s.Field(s.FieldIndices(name)[0]).Type)
		}
	}
	if len(order) != 6 || order[0] != "time" || order[1] != "host" {
		t.Fatalf("column order %v", order)
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM read_parquet('" + filepath.ToSlash(path) + "')").Scan(&n); err != nil || n != 0 {
		t.Fatalf("anchor rows=%d err=%v", n, err)
	}
	if _, err := EncodeAnchor(arrow.NewSchema(nil, nil)); err == nil {
		t.Fatal("empty anchor must be refused")
	}
	if _, ok := ArrowTypeFromDuckDB("STRUCT(a INTEGER)"); ok {
		t.Fatal("unknown DuckDB type must not map")
	}
}
