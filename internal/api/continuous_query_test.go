package api

import "testing"

func TestWrapSourceMeasurement(t *testing.T) {
	const rp = "read_parquet('s3://b/db/m/**/*.parquet', union_by_name=true)"

	tests := []struct {
		name        string
		query       string
		database    string
		measurement string
		rpExpr      string
		want        string
	}{
		{
			name:        "FROM db.measurement",
			query:       "SELECT * FROM db.m WHERE x > 1",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT * FROM " + rp + " WHERE x > 1",
		},
		{
			name:        "case-insensitive keyword (replacement normalizes to uppercase FROM)",
			query:       "select * from db.m",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			// The matched lowercase "from" is replaced with literal "FROM ";
			// DuckDB is case-insensitive on the keyword so this is cosmetic.
			want: "select * FROM " + rp,
		},
		{
			name:        "dollar sign in path is literal (not submatch expansion)",
			query:       "SELECT * FROM db.m",
			database:    "db",
			measurement: "m",
			rpExpr:      "read_parquet('s3://b/$1/m', union_by_name=true)",
			want:        "SELECT * FROM read_parquet('s3://b/$1/m', union_by_name=true)",
		},
		// --- negative cases: must be left UNCHANGED ---
		// These document the deliberately narrow scope. A regex cannot safely
		// rewrite these without corrupting valid SQL, so they pass through. If a
		// future change widens the pattern, these guard against silent corruption
		// (e.g. rewriting a table-valued function into a scalar position).
		{
			name:        "measurement as prefix is not matched",
			query:       "SELECT * FROM db.measurement_5m",
			database:    "db",
			measurement: "measurement",
			rpExpr:      rp,
			want:        "SELECT * FROM db.measurement_5m",
		},
		{
			name:        "different database is not matched",
			query:       "SELECT * FROM otherdb.m",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT * FROM otherdb.m",
		},
		{
			name:        "comma column reference is not rewritten",
			query:       "SELECT a, db.m AS x FROM t",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT a, db.m AS x FROM t",
		},
		{
			name:        "GROUP BY column reference is not rewritten",
			query:       "SELECT a FROM t GROUP BY a, db.m",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT a FROM t GROUP BY a, db.m",
		},
		{
			// KNOWN PRE-EXISTING LIMITATION (documented, not endorsed): a
			// three-part name in a table position (FROM db.m.field) has its db.m
			// prefix rewritten because the trailing \b matches the '.'. Arc's
			// query model uses two-part db.measurement names, so this shape does
			// not arise in practice today. Captured here so a future parser-based
			// rewrite is expected to FIX it (change this assertion when it does).
			name:        "three-part name: known partial-rewrite limitation",
			query:       "SELECT x FROM db.m.field",
			database:    "db",
			measurement: "m",
			rpExpr:      rp,
			want:        "SELECT x FROM read_parquet('s3://b/db/m/**/*.parquet', union_by_name=true).field",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapSourceMeasurement(tt.query, tt.database, tt.measurement, tt.rpExpr)
			if got != tt.want {
				t.Errorf("wrapSourceMeasurement(%q, %q, %q)\n  got:  %q\n  want: %q",
					tt.query, tt.database, tt.measurement, got, tt.want)
			}
		})
	}
}
