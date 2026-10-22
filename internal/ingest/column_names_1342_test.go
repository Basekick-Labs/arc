package ingest

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/basekick-labs/arc/pkg/models"
)

// #1342: names Arc cannot read back are refused at ingest.

func TestColumnNameRules(t *testing.T) {
	cases := map[string]string{
		"value":      "",
		"time":       ColumnNameTimeAlike, // as a tag or field; the timestamp column is never asked
		"Time":       ColumnNameTimeAlike,
		"TIME":       ColumnNameTimeAlike,
		"tImE":       ColumnNameTimeAlike,
		"time ":      "", // DuckDB keeps it apart
		"TİME":       "", // non-ASCII folding: DuckDB keeps it apart
		"times":      "",
		"\xb0":       ColumnNameNotUTF8,
		"temp\xb0C":  ColumnNameNotUTF8,
		"temp°C":     "",
		"Temp_value": "",
	}
	for name, want := range cases {
		if got := ColumnNameProblem(name); got != want {
			t.Errorf("ColumnNameProblem(%q) = %q, want %q", name, got, want)
		}
	}
	if FoldColumnName("abc") != "abc" || FoldColumnName("AbC") != "abc" || FoldColumnName("ÄB") != "Äb" {
		t.Error("FoldColumnName folds ASCII letters only")
	}
	if QuoteColumnName("ok") != "ok" || QuoteColumnName("\xb0") != `"\xb0"` {
		t.Errorf("QuoteColumnName = %s", QuoteColumnName("\xb0"))
	}
}

func lpDropReasons(drops []LPDrop) map[string]int {
	out := map[string]int{}
	for _, d := range drops {
		out[d.Column+"|"+d.Reason] += d.Points
	}
	return out
}

func TestLPWriteDropsUnstorableNames(t *testing.T) {
	p := NewLineProtocolParser()
	body := []byte("cpu,host=a v=1 1\n" +
		"cpu,host=a Time=2 2\n" + // time in another case
		"cpu,host=a,TIME=x v=3 3\n" + // as a tag
		"cpu,host=a t\xb0=4 4\n" + // not UTF-8
		"cpu,host=a Temp=5,temp=6 5\n" + // twins in one point
		"cpu,host=a v=7 6\n" +
		"cpu,host=a V=8 7\n" + // twin of an earlier line's column
		"cpu,Host=b v=9 8\n") // twin of a tag
	batches, rows, drops, err := p.ParseLPWrite(body, "ns")
	if err != nil {
		t.Fatal(err)
	}
	if rows["cpu"] != 2 {
		t.Fatalf("rows = %d, want the 2 good points", rows["cpu"])
	}
	got := lpDropReasons(drops)
	want := map[string]int{
		"Time|" + LPDropTime: 1, "TIME|" + LPDropTime: 1, "t\xb0|" + LPDropName: 1,
		"temp|" + LPDropCase: 1, "V|" + LPDropCase: 1, "Host|" + LPDropCase: 1,
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("drop %q = %d, want %d (all: %v)", k, got[k], n, got)
		}
	}
	var cols []string
	for c := range batches["cpu"].Data {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	if len(cols) != 3 { // host, time, v
		t.Errorf("columns = %v", cols)
	}
}

// A column the measurement already has under another spelling (an earlier
// write's, from the field schema anchor) is refused too.
func TestLPWriteDropsTwinOfAStoredColumn(t *testing.T) {
	p := NewLineProtocolParser()
	known := func(m string) map[string]string {
		if m == "cpu" {
			return map[string]string{"temp": "Temp", "time": "time"}
		}
		return nil
	}
	_, rows, drops, err := p.ParseLPWriteKnown([]byte("cpu temp=1 1\ncpu Temp=2 2\nmem temp=3 3\n"), "ns", known)
	if err != nil {
		t.Fatal(err)
	}
	if rows["cpu"] != 1 || rows["mem"] != 1 {
		t.Fatalf("rows = %v, want cpu's stored spelling and mem kept", rows)
	}
	if got := lpDropReasons(drops); got["temp|"+LPDropCase] != 1 {
		t.Errorf("drops = %v", got)
	}
}

// The exact key "time" keeps its existing drop (column "").
func TestLPWriteExactTimeKeyUnchanged(t *testing.T) {
	_, _, drops, _ := NewLineProtocolParser().ParseLPWrite([]byte("cpu time=1 1\ncpu v=1 2\n"), "ns")
	if got := lpDropReasons(drops); got["|"+LPDropTime] != 1 {
		t.Errorf("drops = %v", got)
	}
}

func TestFilterRecordNames(t *testing.T) {
	good := &models.ColumnarRecord{Measurement: "m", Columns: map[string][]interface{}{"time": {int64(1), int64(2)}, "temp": {1.0, 2.0}}}
	timeTwin := &models.ColumnarRecord{Measurement: "m", Columns: map[string][]interface{}{"time": {int64(1)}, "Time": {1.0}}}
	caseTwin := &models.ColumnarRecord{Measurement: "m", Columns: map[string][]interface{}{"time": {int64(3)}, "Temp": {3.0}}}
	other := &models.ColumnarRecord{Measurement: "n", Columns: map[string][]interface{}{"time": {int64(3)}, "Temp": {3.0}}}
	row := &models.Record{Measurement: "m", Fields: map[string]interface{}{"\xb0": 1.0}}
	kept, drops, removed := FilterRecordNames([]interface{}{good, timeTwin, caseTwin, other, row}, nil)
	list := kept.([]interface{})
	if len(list) != 2 || list[0] != good || list[1] != other || removed != 3 {
		t.Fatalf("kept %v removed %d", list, removed)
	}
	got := lpDropReasons(drops)
	if got["Time|"+ColumnNameTimeAlike] != 1 || got["Temp|"+ColumnNameCaseTwin] != 1 || got["\xb0|"+ColumnNameNotUTF8] != 1 {
		t.Errorf("drops = %v", got)
	}
	// A stored spelling wins over the write's.
	_, drops, _ = FilterRecordNames(good, func(string) map[string]string { return map[string]string{"temp": "Temp"} })
	if len(drops) != 1 {
		t.Errorf("twin of a stored column not refused: %v", drops)
	}
}

// MQTT keeps a reading whose payload carries a key like Tasmota's "Time",
// dropping only that key.
func TestStripRecordNamesKeepsTheReading(t *testing.T) {
	rec := &models.Record{Measurement: "tasmota", Fields: map[string]interface{}{"Time": "2026-10-22T10:00:00", "Power": 12.0, "power": 1.0}}
	drops := StripRecordNames([]interface{}{rec}, nil)
	if _, ok := rec.Fields["Time"]; ok {
		t.Error("Time kept")
	}
	if _, ok := rec.Fields["Power"]; !ok {
		t.Error("the first spelling of a twin was dropped")
	}
	if _, ok := rec.Fields["power"]; ok {
		t.Error("the later spelling of a twin kept")
	}
	if len(drops) != 2 {
		t.Errorf("drops = %v", drops)
	}
}

func TestTruncateColumnName(t *testing.T) {
	long := strings.Repeat("é", 300) // 600 bytes
	got := TruncateColumnName(long)
	if len(got) > maxShownNameBytes+len("…") || !utf8.ValidString(got) {
		t.Fatalf("len %d valid %v", len(got), utf8.ValidString(got))
	}
	if TruncateColumnName("short") != "short" {
		t.Fatal("short name changed")
	}
}

// A line ending in a twin of its first field, with many fields, is refused in
// linear time: the point's new columns are checked through a map.
func TestLPWriteWidePointTwinIsLinear(t *testing.T) {
	var b strings.Builder
	b.WriteString("cpu ")
	for i := 0; i < 20000; i++ {
		fmt.Fprintf(&b, "F%d=1i,", i)
	}
	b.WriteString("f0=1i 1\n")
	start := time.Now()
	_, _, drops, err := NewLineProtocolParser().ParseLPWrite([]byte(b.String()), "ns")
	if err != nil || len(drops) != 1 {
		t.Fatalf("drops %v err %v", drops, err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("a 20000-field point took %v", d) // quadratic took ~3s here
	}
}
