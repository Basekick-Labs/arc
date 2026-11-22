package metrics

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type fakeTableLag struct {
	pending []IcebergTableLag
	total   int
	last    []IcebergTableLag
	gotN    int
}

func (f *fakeTableLag) PendingTables(n int, _ time.Time) ([]IcebergTableLag, int) {
	f.gotN = n
	return f.pending, f.total
}
func (f *fakeTableLag) LastLagTables(n int, _ time.Time) []IcebergTableLag { return f.last }

func TestIcebergTableLagSeries(t *testing.T) {
	m := &Metrics{}
	if out := string(m.appendIcebergExportTables(nil)); out != "" {
		t.Fatalf("no source: %q, want nothing", out)
	}
	src := &fakeTableLag{
		pending: []IcebergTableLag{{Database: "prod", Measurement: "cpu", Seconds: 180}},
		total:   3,
		last:    []IcebergTableLag{{Database: `we"ird\db`, Measurement: "line\nbreak\xff", Seconds: 42}},
	}
	m.SetIcebergTableLagSource(src, 2)
	out := string(m.appendIcebergExportTables(nil))
	if src.gotN != 2 {
		t.Errorf("source asked for %d tables, want 2", src.gotN)
	}
	for _, want := range []string{
		"arc_iceberg_export_tables_pending 3\n",
		`arc_iceberg_export_table_pending_seconds{database="prod",measurement="cpu"} 180` + "\n",
		`arc_iceberg_export_table_last_lag_seconds{database="we\"ird\\db",measurement="line\nbreak` + "\uFFFD" + `"} 42` + "\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("exposition lacks %q:\n%s", want, out)
		}
	}
	if !utf8.ValidString(out) {
		t.Error("exposition is not valid UTF-8")
	}
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		if strings.ContainsRune(line, '\n') {
			t.Errorf("a raw newline inside a line: %q", line)
		}
	}
	// Off again.
	m.SetIcebergTableLagSource(src, 0)
	if out := string(m.appendIcebergExportTables(nil)); out != "" {
		t.Fatalf("n = 0: %q, want nothing", out)
	}
}

// The JSON snapshot never carries table names (#1772): the JSON metrics
// endpoints stay name-free.
func TestIcebergTableLagNotInJSONSnapshot(t *testing.T) {
	m := Get()
	m.SetIcebergTableLagSource(&fakeTableLag{
		pending: []IcebergTableLag{{Database: "secretdb", Measurement: "secretm", Seconds: 99}},
		total:   1,
		last:    []IcebergTableLag{{Database: "secretdb", Measurement: "secretm", Seconds: 9}},
	}, 5)
	t.Cleanup(func() { m.SetIcebergTableLagSource(nil, 0) })
	raw, err := json.Marshal(m.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "secretdb") || strings.Contains(string(raw), "secretm") {
		t.Fatal("a table name reached the JSON snapshot")
	}
	if !strings.Contains(string(m.PrometheusFormat()), "secretdb") {
		t.Fatal("the Prometheus text lacks the table series")
	}
}
