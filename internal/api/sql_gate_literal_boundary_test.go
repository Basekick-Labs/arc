package api

import (
	"strings"
	"testing"
)

// The read-SQL gates run on the masked form of the statement. A plain literal
// that ends in a backslash used to run the masker's scan past its own closing
// quote, so everything up to the next quote collapsed into one placeholder and
// the gates never saw it — while DuckDB, which treats that backslash as an
// ordinary character, parsed and executed the hidden remainder.
func TestValidateSQLRequest_LiteralEndingInBackslashDoesNotHideTheRest(t *testing.T) {
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
			name: "file I/O function after a backslash literal in a quoted identifier",
			sql:  `SELECT "c\" FROM cpu WHERE host = 'a' UNION ALL SELECT * FROM read_csv('/other/x.csv') -- '`,
			want: "read_csv",
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
	} {
		if err := ValidateSQLRequest(ok); err != nil {
			t.Errorf("FALSE POSITIVE: refused benign SQL %q: %v", ok, err)
		}
	}
}
