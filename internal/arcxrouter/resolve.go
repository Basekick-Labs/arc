// Measurement resolution + path interpolation helpers. Cgo-free so they compile
// and test in every build; the tagged router calls them.

package arcxrouter

import (
	"regexp"
	"strings"

	arcsql "github.com/basekick-labs/arc/internal/sql"
)

// validIdent mirrors Arc's validateIdentifier charset (query.go:746): alphanumeric,
// underscore, hyphen. Database and measurement names must match, or we decline —
// a name outside this set would be a path-injection risk and isn't a real Arc
// measurement anyway.
var validIdent = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// resolveMeasurementToken splits a FROM token (as written) into (database,
// measurement) and folds headerDB for the bare form, mirroring
// checkQueryPermissions (query.go:1220): a bare `measurement` resolves to the
// x-arc-database header, else "default". A `db.measurement` token uses its own
// database. Returns ok=false if the token isn't a clean one- or two-part
// identifier (system tables, >2 parts, invalid chars → decline).
func resolveMeasurementToken(token, headerDB string) (database, measurement string, ok bool) {
	parts := strings.Split(token, ".")
	switch len(parts) {
	case 1:
		measurement = parts[0]
		database = headerDB
		if database == "" {
			database = "default"
		}
	case 2:
		database = parts[0]
		measurement = parts[1]
	default:
		return "", "", false
	}
	if !validIdent.MatchString(database) || !validIdent.MatchString(measurement) {
		return "", "", false
	}
	return database, measurement, true
}

// bareIdent matches a plain SQL column identifier (optionally dotted). The scalar
// aggregate column is interpolated into engine SQL UNQUOTED (the engine parser
// wants a bare identifier), so it must be exactly this — no quotes, spaces, or
// operators that could break out. The tokenizer already guarantees this shape;
// isBareIdent is the defensive re-check at the SQL-construction boundary.
var bareIdent = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

// isBareIdent reports whether col is safe to interpolate unquoted into engine SQL.
func isBareIdent(col string) bool {
	return bareIdent.MatchString(col)
}

// quotePath returns a single-quoted DuckDB path literal, escaping embedded quotes
// via Arc's canonical escaper — the single source of truth for the read_parquet
// interpolation SQL-injection boundary (internal/sql.EscapeStringLiteral).
func quotePath(path string) string {
	return "'" + arcsql.EscapeStringLiteral(path) + "'"
}

// escapeStringLiteral exposes the canonical escaper for the unit literal.
func escapeStringLiteral(s string) string {
	return arcsql.EscapeStringLiteral(s)
}
