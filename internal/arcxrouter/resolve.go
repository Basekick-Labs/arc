// Measurement resolution + path interpolation helpers. Cgo-free so they compile
// and test in every build; the tagged router calls them.

package arcxrouter

import (
	"regexp"
	"strings"

	arcsql "github.com/basekick-labs/arc/internal/sql"
)

// validIdent is DELIBERATELY stricter than Arc's validateIdentifier (query.go), which
// also permits a hyphen. It must match what Arc's RBAC patterns can actually parse:
// patternDBTable / patternSimpleTable / patternJoinDBTable (query.go) are all
// `[a-zA-Z0-9_]` with NO hyphen, so RBAC reads `FROM my-db.cpu` as database `default`,
// measurement `my`. If a hyphenated name reached the router, the router and RBAC would
// disagree about which database is being read — a permission check against one name and
// a read against another.
//
// That is unreachable today (the tokenizer rejects `-` inside an identifier), so this is
// defense in depth: the router must never be the component that widens what RBAC believes
// it authorized. Decline instead — the shape falls back to DuckDB, which does its own
// checks.
var validIdent = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

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

// projFuncItem matches a re-serialized computed-projection item — the ONLY non-bare-
// column forms buildScanSQL emits: `length(host)` (2f-0), `substr(host, 1, 3)` (2f-1),
// and `starts_with(host, 'web')` / `ends_with`/`contains` (2f-2). The function name and
// column arg are bare identifiers; the remaining args are EITHER (optionally `-`-signed)
// integers (substr) OR a single properly-escaped single-quoted string literal (the 2f-2
// needle, `'([^']|”)*'` — every embedded `'` is DOUBLED, so no unescaped quote can break
// out). Injection-safe. `isProjFuncItem` is the SQL-boundary defensive re-check that the
// item came from matchProjFunc.
var projFuncItem = regexp.MustCompile(
	`^[a-zA-Z_][a-zA-Z0-9_]*\([a-zA-Z_][a-zA-Z0-9_.]*` +
		`(?:(?:, -?[0-9]+){1,2}|, '(?:[^']|'')*')?\)$`,
)

func isProjFuncItem(col string) bool {
	return projFuncItem.MatchString(col)
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

// isCmpOp guards the operator string emitted into SQL (defense-in-depth; the
// tokenizer only ever produces these). Cgo-free so the untagged recognizer/re-serializer
// and the tagged buildScanSQL both call it.
func isCmpOp(op string) bool {
	switch op {
	case "=", "!=", "<", "<=", ">", ">=":
		return true
	}
	return false
}

// isIntLiteral reports whether s is a base-10 integer (optional leading '-'),
// matching what the tokenizer's tokNum produces for a WHERE literal.
func isIntLiteral(s string) bool {
	if s == "" {
		return false
	}
	i := 0
	if s[0] == '-' {
		if len(s) == 1 {
			return false
		}
		i = 1
	}
	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
