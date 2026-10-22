package ingest

import (
	"strconv"
	"unicode/utf8"
)

// Field and tag names Arc refuses at ingest (#1342).
//
// A name Arc writes into a Parquet schema must be one it can read back.
// Three kinds cannot:
//
//   - a name that is not valid UTF-8: compaction, SELECT * and pyarrow fail
//     with "Invalid unicode";
//   - a name equal to "time" ignoring ASCII case ("Time", "TIME"): DuckDB
//     binds identifiers case-insensitively, and a file's columns are in map
//     order, so in some files `time` binds to the other column (compaction
//     finds no time column, the DELETE count's cast fails);
//   - two names of one measurement equal ignoring ASCII case ("Temp" and
//     "temp"): in one file DuckDB renames the second (temp_1); across files
//     union_by_name merges them into whichever comes first.
//
// The folding is ASCII only, as DuckDB's identifier folding and compaction's
// column keys are: "TİME" or "ｔｉｍｅ" are names of their own to both.

// FoldColumnName is name with ASCII letters lower-cased. It allocates only
// when name has an upper-case ASCII letter.
func FoldColumnName(name string) string {
	for i := 0; i < len(name); i++ {
		if c := name[i]; c >= 'A' && c <= 'Z' {
			b := []byte(name)
			for j := i; j < len(b); j++ {
				if c := b[j]; c >= 'A' && c <= 'Z' {
					b[j] = c + ('a' - 'A')
				}
			}
			return string(b)
		}
	}
	return name
}

// isTimeName reports whether name is "time" ignoring ASCII case.
func isTimeName(name string) bool {
	return len(name) == 4 && FoldColumnName(name) == "time"
}

// Reasons a name is refused.
const (
	ColumnNameNotUTF8   = "a tag or field name that is not valid UTF-8"
	ColumnNameTimeAlike = "a tag or field named \"time\" in any letter case (time is the timestamp)"
	ColumnNameCaseTwin  = "a tag or field name that differs only in letter case from another of the measurement"
)

// ColumnNameProblem is why name cannot be a tag or field column, or "".
// The timestamp column itself is not a tag or field and never asked about.
func ColumnNameProblem(name string) string {
	if !utf8.ValidString(name) {
		return ColumnNameNotUTF8
	}
	if isTimeName(name) {
		return ColumnNameTimeAlike
	}
	return ""
}

// QuoteColumnName is name for a message or log line: as is when it is valid
// UTF-8, otherwise Go-quoted, so an invalid byte is shown escaped and never
// written raw.
func QuoteColumnName(name string) string {
	if utf8.ValidString(name) {
		return name
	}
	return strconv.Quote(name)
}

// KnownColumns returns, for a measurement, the spelling each column name the
// measurement already has is stored under, keyed by its FoldColumnName: the
// names of earlier batches, which a new column must not differ from only in
// letter case. nil when unknown. Must not block on I/O: it is asked on every
// write.
type KnownColumns func(measurement string) map[string]string

// ColumnCaseTwin reports whether a new column name differs only in letter
// case from a name in batch (fold -> spelling, this write's columns) or in
// known (stored columns), and the spelling it collides with.
func ColumnCaseTwin(name string, batch, known map[string]string) (string, bool) {
	f := FoldColumnName(name)
	if other, ok := batch[f]; ok && other != name {
		return other, true
	}
	if other, ok := known[f]; ok && other != name {
		return other, true
	}
	return "", false
}
