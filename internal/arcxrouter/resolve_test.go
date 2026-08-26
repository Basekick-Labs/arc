// Resolution + path-helper tests. Cgo-free.

package arcxrouter

import "testing"

func TestResolveMeasurementToken(t *testing.T) {
	cases := []struct {
		name     string
		token    string
		headerDB string
		wantDB   string
		wantMeas string
		wantOK   bool
	}{
		{"bare with header", "cpu", "prod", "prod", "cpu", true},
		{"bare no header defaults", "cpu", "", "default", "cpu", true},
		{"dotted uses own db", "mydb.cpu", "prod", "mydb", "cpu", true},
		{"dotted ignores header", "mydb.cpu", "prod", "mydb", "cpu", true},
		{"hyphen ok", "my-db.my-meas", "", "my-db", "my-meas", true},
		{"underscore ok", "my_db.my_meas", "", "my_db", "my_meas", true},
		{"three parts declines", "a.b.c", "", "", "", false},
		{"invalid char declines", "cpu$", "prod", "", "", false},
		{"empty declines", "", "prod", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, meas, ok := resolveMeasurementToken(tc.token, tc.headerDB)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v want %v", ok, tc.wantOK)
			}
			if ok && (db != tc.wantDB || meas != tc.wantMeas) {
				t.Fatalf("got (%q,%q) want (%q,%q)", db, meas, tc.wantDB, tc.wantMeas)
			}
		})
	}
}

func TestQuotePathEscapes(t *testing.T) {
	// A path with an embedded single quote must be '' -escaped so it can't break
	// out of the read_parquet literal (the SQL-injection boundary).
	got := quotePath("/data/o'brien/f.parquet")
	want := "'/data/o''brien/f.parquet'"
	if got != want {
		t.Fatalf("quotePath = %q, want %q", got, want)
	}
}

func TestParseMode(t *testing.T) {
	cases := map[string]Mode{
		"":         ModeShadow,
		"shadow":   ModeShadow,
		"SHADOW":   ModeShadow,
		"  serve ": ModeServe,
		"off":      ModeOff,
		"garbage":  ModeShadow, // unknown → safe default
	}
	for in, want := range cases {
		if got := ParseMode(in); got != want {
			t.Fatalf("ParseMode(%q)=%v want %v", in, got, want)
		}
	}
}
