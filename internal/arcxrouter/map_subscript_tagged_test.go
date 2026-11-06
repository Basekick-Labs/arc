// buildScanSQL with map subscripts (map-1 companion). Tagged: Decision and
// buildScanSQL live in the tagged router.go.

//go:build cgo && arcx_engine

package arcxrouter

import "testing"

func TestBuildScanSQL_Subscript(t *testing.T) {
	m, ok := eligibleShape("SELECT Timestamp, LogAttributes['userId'] FROM otel_logs WHERE ResourceAttributes['service.name'] = 'payment' AND Body LIKE '%card%' ORDER BY Timestamp DESC LIMIT 100")
	if !ok {
		t.Fatal("must be eligible")
	}
	d := Decision{Eligible: true, Shape: m.shape, Cols: m.cols, Preds: m.preds, WhereText: m.whereText, OrderBy: m.orderBy, Limit: m.limit}
	sql, ok := buildScanSQL(d, "['/data/a.parquet']")
	if !ok {
		t.Fatal("buildScanSQL must accept subscript refs")
	}
	want := "SELECT Timestamp, LogAttributes['userId'] FROM read_parquet(['/data/a.parquet']) WHERE ResourceAttributes['service.name'] = 'payment' AND Body LIKE '%card%' ORDER BY Timestamp DESC LIMIT 100"
	if sql != want {
		t.Fatalf("sql =\n%s\nwant\n%s", sql, want)
	}
	// A hand-built Decision with an unsafe "subscript" is refused at the SQL boundary.
	bad := Decision{Eligible: true, Shape: ShapeScan, Cols: []string{"m['x'] FROM evil"}}
	if _, ok := buildScanSQL(bad, "['/data/a.parquet']"); ok {
		t.Fatal("buildScanSQL must refuse a malformed subscript item")
	}
	bad2 := Decision{Eligible: true, Shape: ShapeScan, Cols: []string{"id"}, Preds: []scanPred{{col: "m['x''] OR 1=1 --", op: "=", str: "a", isStr: true}}}
	if _, ok := buildScanSQL(bad2, "['/data/a.parquet']"); ok {
		t.Fatal("buildScanSQL must refuse a malformed subscript pred")
	}
}
