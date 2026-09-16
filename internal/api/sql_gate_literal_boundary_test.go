package api

import (
	"strings"
	"testing"
)

// The read-SQL gates run on the masked form of the statement, so anything the
// masker believes is inside a literal is invisible to them. Four spellings used
// to make its idea of a literal's boundaries differ from DuckDB's, and each one
// hid the rest of a statement from every gate while DuckDB parsed and executed
// it: a plain literal ending in a backslash, a quote inside a comment, a run of
// `$` continuing an identifier, and a quote inside a quoted identifier (that
// last one through the denylist's own normalisation, which used to strip
// identifier quotes before masking).
func TestValidateSQLRequest_LiteralBoundariesDoNotHideTheRest(t *testing.T) {
	hidden := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "file I/O function after a backslash literal",
			sql:  `SELECT * FROM cpu WHERE host = '\' UNION ALL SELECT * FROM read_parquet('/other/x.parquet') -- '`,
			want: "read_parquet",
		},
		{
			name: "table-position literal after a backslash literal",
			sql:  `SELECT * FROM cpu WHERE host = '\' UNION ALL SELECT * FROM '/other/x.parquet' -- '`,
			want: "table position",
		},
		{
			// Goes through the multi-statement check rather than the I/O
			// denylist: the denylist's own normalisation removes identifier
			// quotes, so it caught the read_csv spelling either way, while the
			// statement splitter runs on the masked form alone.
			name: "second statement after a backslash literal in a quoted identifier",
			sql:  `SELECT "c\" FROM cpu WHERE host = 'a' ; DROP TABLE x -- '`,
			want: "Multiple SQL statements",
		},
		{
			name: "file I/O function after a quote inside a line comment",
			sql:  "SELECT 0 AS x -- '\nUNION ALL SELECT * FROM read_parquet('/other/x.parquet')",
			want: "read_parquet",
		},
		{
			name: "file I/O function after a quote inside a block comment",
			sql:  `SELECT 0 AS x /* ' */ UNION ALL SELECT * FROM read_parquet('/other/x.parquet')`,
			want: "read_parquet",
		},
		{
			name: "file I/O function after a dollar run continuing an identifier",
			sql:  `SELECT * FROM cpu AS t$$$ UNION ALL SELECT * FROM read_parquet('/other/x.parquet')`,
			want: "read_parquet",
		},
		{
			name: "table-position literal after a dollar run continuing an identifier",
			sql:  `SELECT * FROM cpu AS t$$$ UNION ALL SELECT * FROM '/other/x.parquet'`,
			want: "table position",
		},
		{
			name: "file I/O function hidden by a quote inside a quoted identifier",
			sql:  `SELECT 1 AS "a'b", (SELECT count(*) FROM read_parquet('/other/x.parquet')) AS n`,
			want: "read_parquet",
		},
		{
			name: "table-position literal hidden by a quote inside a quoted identifier",
			sql:  `SELECT * FROM cpu AS "a'b" UNION ALL SELECT * FROM '/other/x.parquet'`,
			want: "table position",
		},
	}
	for _, tc := range hidden {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSQLRequest(tc.sql)
			if err == nil {
				t.Fatalf("accepted SQL whose %s is hidden behind a backslash literal", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("rejected for the wrong reason: %v (want a refusal naming %q)", err, tc.want)
			}
		})
	}

	// The same statements without the backslash were already refused; the fix
	// must not have changed that, and must not start refusing a plain value
	// that merely ends in a backslash.
	if err := ValidateSQLRequest(`SELECT * FROM cpu WHERE host = 'a' UNION ALL SELECT * FROM read_parquet('/other/x.parquet')`); err == nil {
		t.Error("the plain form of the file-I/O call was accepted")
	}
	for _, ok := range []string{
		`SELECT * FROM cpu WHERE dir = 'C:\'`,
		`SELECT * FROM cpu WHERE dir = 'C:\' AND name = 'report.csv'`,
		`SELECT * FROM cpu WHERE note = E'it\'s here'`,
		`SELECT * FROM cpu WHERE pattern = 'a\\b'`,
		`SELECT * FROM cpu WHERE note = $tag$body$tag$`,
		`SELECT * FROM cpu AS t$x -- a comment with an apostrophe: don't`,
		`SELECT 1 AS "a'b" FROM cpu`,
		"SELECT 1 /* it's fine */ FROM cpu",
	} {
		if err := ValidateSQLRequest(ok); err != nil {
			t.Errorf("FALSE POSITIVE: refused benign SQL %q: %v", ok, err)
		}
	}
}
