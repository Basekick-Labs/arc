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

// A shadow MISMATCH names the first differing column; a map subscript's key is
// user data and must not reach the log (the SQL masker hides the same literal
// one field over).
func TestLogColumnName_MasksSubscriptKey(t *testing.T) {
	if got := logColumnName("LogAttributes['tenant-42']"); got != "LogAttributes['...']" {
		t.Fatalf("got %q", got)
	}
	if got := logColumnName("host"); got != "host" {
		t.Fatalf("got %q", got)
	}
	if got := firstDiffColumn(scanRow{"a=1", "m['k']=x"}, scanRow{"a=1", "m['k']=y"}, []string{"a", "m['k']"}); got != "m['...']" {
		t.Fatalf("firstDiffColumn = %q", got)
	}
}

// Brackets now lex, so a bracket query that is not the served shape moves from the
// `unlexable` census bucket to a shape bucket. Pin the new label so a dashboard
// reader can find the reason here.
func TestCensusClassify_BracketQueryIsNotUnlexable(t *testing.T) {
	for _, sql := range []string{"SELECT arr[1] FROM t", "SELECT m['a']['b'] FROM t", "SELECT id FROM t ORDER BY m['k']"} {
		reason, _ := CensusClassify(sql, "")
		if reason == "unlexable" || reason == "eligible" {
			t.Fatalf("%s classified %q", sql, reason)
		}
	}
	if reason, _ := CensusClassify("SELECT a[1:2] FROM t", ""); reason != "unlexable" {
		t.Fatalf("slice syntax should stay unlexable, got %q", reason)
	}
}

func TestBuildScanAggSQL_Subscript(t *testing.T) {
	m, ok := eligibleShape("SELECT count(*), count(LogAttributes['userId']) FROM otel_logs WHERE ResourceAttributes['service.name'] = 'payment'")
	if !ok || m.shape != ShapeScanAgg {
		t.Fatalf("eligibleShape: ok=%v shape=%q", ok, m.shape)
	}
	d := Decision{Eligible: true, Shape: m.shape, AggItems: m.aggItems, WhereText: m.whereText}
	sql, ok := buildScanAggSQL(d, "['/data/a.parquet']")
	if !ok {
		t.Fatal("buildScanAggSQL must accept count over a subscript")
	}
	want := "SELECT count(*), count(LogAttributes['userId']) FROM read_parquet(['/data/a.parquet']) WHERE ResourceAttributes['service.name'] = 'payment'"
	if sql != want {
		t.Fatalf("sql =\n%s\nwant\n%s", sql, want)
	}
	for _, bad := range []string{"min(m['k'])", "count(m['k']) FROM evil", "count(m['k''])", "count(a.b['k'])"} {
		if isAggItem(bad) {
			t.Fatalf("isAggItem must refuse %q", bad)
		}
	}
	if !isAggItem("count(m['k'])") || !isAggItem("count(host)") {
		t.Fatal("isAggItem must accept count over a bare column or a subscript")
	}
}
