package sql

import (
	"strings"
	"testing"
)

// DuckDB errors as Arc's query path returns them (collected from a live
// instance): the message and its hints stay, the LINE block with Arc's
// rewritten SQL goes.
func TestClientErrText_KeepsTheMessageDropsTheStatement(t *testing.T) {
	tests := []struct {
		raw  string
		keep []string
	}{
		{"arrow query failed: Binder Error: Referenced column \"altitud\" not found in FROM clause!\nCandidate bindings: \"altitude\", \"latitude\", \"longitude\", \"callsign\", \"time\"\n\nLINE 1: SELECT avg(altitud) FROM read_parquet(['/app/.tmp/arc-uploads/schema...\n                   ^",
			[]string{`Referenced column "altitud"`, `Candidate bindings: "altitude"`}},
		{"arrow query failed: Binder Error: column \"icao24\" must appear in the GROUP BY clause or must be part of an aggregate function.\nEither add it to the GROUP BY list, or use \"ANY_VALUE(icao24)\" if the exact value of \"icao24\" is not important.\n\nLINE 1: SELECT icao24 FROM read_parquet(['/app/.tmp/arc-uploads/schema...\n",
			[]string{"GROUP BY", `"ANY_VALUE(icao24)"`}},
		{"arrow query failed: Conversion Error: Could not convert string 'JBU193' to INT32 when casting from source column callsign\n\nLINE 1: SELECT CAST(callsign AS INTEGER) FROM read_parquet(['/app/.tmp...\n               ^",
			[]string{"'JBU193'", "callsign"}},
		{"arrow query failed: Catalog Error: Scalar Function with name nosuchfunc does not exist!\nDid you mean \"cosh\"?\n\nLINE 1: SELECT nosuchfunc(altitude) FROM read_parquet(['/app/.tmp/arc-uploads...\n               ^",
			[]string{"nosuchfunc", `Did you mean "cosh"?`}},
		{"arrow query failed: Parser Error: SELECT clause without selection list", []string{"Parser Error: SELECT clause without selection list"}},
	}
	for _, tt := range tests {
		got := clientText(tt.raw)
		for _, k := range tt.keep {
			if !strings.Contains(got, k) {
				t.Errorf("lost %q: %q", k, got)
			}
		}
		for _, leak := range []string{"LINE 1", "/app/", "read_parquet(["} {
			if strings.Contains(got, leak) {
				t.Errorf("leaked %q: %q", leak, got)
			}
		}
	}
}

func TestClientErrText_RedactsStorageByContent(t *testing.T) {
	tests := []struct{ raw, leak string }{
		{"Invalid Input Error: failed to read s3://bucket/db/cpu/x.parquet now", "s3://"},
		{"Invalid Input Error: No magic bytes found at end of file 'azure://acct/c/db/cpu/x.parquet'", "azure://"},
		{`Binder Error: path "C:\arc\data\x" unreadable`, `C:\arc`},
		{`Binder Error: share \\fileserver\share\arc missing`, "fileserver"},
		{"Binder Error: name `/app/data/x` bad", "/app/data"},
		{"Binder Error: at file:/app/data/tenant2/cpu", "tenant2"},
		{"Binder Error: key %2Fapp%2Fdata%2Ftenant2 bad", "tenant2"},
		{"Binder Error: under data/tenant2/cpu/2026/11/x.parquet", "tenant2"},
		{"Binder Error: partition year=2026/month=11 bad", "year=2026"},
		{"HTTP Error: endpoint minio.internal.corp:9000 refused", "minio.internal"},
		{"HTTP Error: endpoint 10.0.3.7 refused", "10.0.3.7"},
		{"Invalid Input Error: File '/app/my data/db/x.parquet' too small to be a Parquet file", "my data"},
		{"Binder Error: read_parquet(['/app/a(1).parquet', '/app/b.parquet']) failed", "a(1)"},
	}
	for _, tt := range tests {
		if got := clientText(tt.raw); strings.Contains(got, tt.leak) {
			t.Errorf("%q leaked %q: %q", tt.raw, tt.leak, got)
		}
	}
}

// What a client needs to fix its query survives, and a schema-drift message
// stays recognisable, with the quotes around the redacted path.
func TestClientErrText_KeepsTheClientsOwnText(t *testing.T) {
	for raw, keep := range map[string]string{
		"Binder Error: No function matches the given name and argument types '/(VARCHAR, INTEGER_LITERAL)'":     "'/(VARCHAR, INTEGER_LITERAL)'",
		"Conversion Error: Could not convert string '2026/11/26' to TIMESTAMP":                                  "'2026/11/26'",
		`Binder Error: Referenced column "usage/idle" not found`:                                                `"usage/idle"`,
		"Conversion Error: In file \"/app/data/acme/cpu/x.parquet\" the column \"usage_idle\" has type VARCHAR": `In file "<path>" the column "usage_idle"`,
	} {
		if got := clientText(raw); !strings.Contains(got, keep) {
			t.Errorf("%q: want %q in %q", raw, keep, got)
		}
	}
	if got := clientText("Binder Error: x\n\nLINE 1: SELECT 1\n ^"); got != "Binder Error: x" {
		t.Errorf("got %q", got)
	}
}

func clientText(s string) string {
	t, _ := ClientErrText(s)
	return t
}

// The removed paths are returned for the server log.
func TestClientErrText_ReturnsWhatItRemoved(t *testing.T) {
	text, paths := ClientErrText("Conversion Error: In file \"/app/data/acme/cpu/x.parquet\" the column \"v\" has type VARCHAR")
	if len(paths) != 1 || paths[0] != "/app/data/acme/cpu/x.parquet" || strings.Contains(text, "/app/") {
		t.Errorf("text %q paths %q", text, paths)
	}
}

// A quote inside a path does not end the span early; a cap never leaves
// half a path.
func TestClientErrText_QuoteRulesAndCap(t *testing.T) {
	if got := clientText("Binder Error: file 'it''s/a/b.parquet' bad"); strings.Contains(got, "a/b") {
		t.Errorf("escaped quote: %q", got)
	}
	long := "Binder Error: " + strings.Repeat("x", 990) + " '/app/data/x.parquet'"
	if got := clientText(long); strings.Contains(got, "/app") {
		t.Errorf("cap leaked: %q", got)
	}
}

// DELETE keeps the database-relative file it names, without the statement.
func TestTrimErrStatement(t *testing.T) {
	got := TrimErrStatement("Binder Error: column x not found in cpu/2026/10/11/10/cpu_a.parquet\n\nLINE 1: SELECT ... read_parquet(['/data/a', '/data/b'])")
	if got != "Binder Error: column x not found in cpu/2026/10/11/10/cpu_a.parquet" {
		t.Errorf("got %q", got)
	}
}
