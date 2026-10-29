package sql

import "strings"

// HivePartitioningOff disables DuckDB's Hive partition inference.
//
// DuckDB derives a column from any `key=value` DIRECTORY component of the path
// it is reading. When that name collides with a real column in the file, the
// path's value REPLACES the stored value and its type, in place; when the file
// has no such column, one is appended. Neither case raises an error, and
// union_by_name=true does not mitigate it.
//
// Arc never wants this. The storage layout is parsed explicitly
// ({database}/{measurement}/{YYYY}/{MM}/{DD}/{HH}/file.parquet) and no code
// consumes an inferred column, so disabling inference removes a corruption
// mode and costs nothing.
//
// There is no global or session setting — `SET hive_partitioning` and
// `SET parquet_hive_partitioning` are both unrecognized configuration
// parameters — so this has to travel with every single read. That is why reads
// are built through ReadParquet rather than assembled at each call site, and
// why TestNoUnscopedReadParquetLiterals pins where such literals may live.
const HivePartitioningOff = "hive_partitioning=false"

// ReadParquet renders a DuckDB read_parquet(...) call with Hive partition
// inference disabled.
//
// pathExpr is the already-quoted path expression: a single quoted literal from
// QuoteStringLiteral, or a bracketed list of them. It is interpolated verbatim,
// so every caller must have quoted it already.
//
// extraOpts are appended after the inference flag, each as a bare `k=v`
// fragment (for example "union_by_name=true", "filename=true").
//
// NOT used by internal/arcxrouter, and it must not be: the arcx engine's SQL
// recognizer accepts only paths between the parentheses and rejects ANY option
// argument, so routing arcx's generated SQL through here would make every arcx
// query decline — silently in serve mode. arcx reads Parquet itself and
// performs no Hive inference, so it needs no flag; the DuckDB shadow oracle
// gets it through the normal query path. See #1005.
func ReadParquet(pathExpr string, extraOpts ...string) string {
	var b strings.Builder
	// "read_parquet(" + ", " + ")" is 16 bytes; leave room for the options too.
	n := len(pathExpr) + len(HivePartitioningOff) + 16
	for _, opt := range extraOpts {
		n += len(opt) + 2
	}
	b.Grow(n)
	b.WriteString("read_parquet(")
	b.WriteString(pathExpr)
	b.WriteString(", ")
	b.WriteString(HivePartitioningOff)
	for _, opt := range extraOpts {
		if opt == "" {
			continue
		}
		b.WriteString(", ")
		b.WriteString(opt)
	}
	b.WriteByte(')')
	return b.String()
}

// ReadParquetList renders a read_parquet call over several already-quoted
// paths, as the bracketed list form DuckDB requires.
func ReadParquetList(quotedPaths []string, extraOpts ...string) string {
	return ReadParquet("["+strings.Join(quotedPaths, ", ")+"]", extraOpts...)
}

// ExcludeFiles wraps the one read_parquet(...) call that expr ends with so the
// read leaves out the given files: a subquery over the read with a file-name
// column (named column, so a data column called "filename" cannot collide)
// and a NOT IN filter, the column then dropped. quotedFiles are SQL string
// literals of the file paths exactly as DuckDB reports them. expr is returned
// unchanged when it holds no such call or there is nothing to exclude.
func ExcludeFiles(expr, column string, quotedFiles []string) string {
	i := strings.Index(expr, "read_parquet(")
	if i < 0 || len(quotedFiles) == 0 || !strings.HasSuffix(expr, ")") {
		return expr
	}
	return expr[:i] + "(SELECT * EXCLUDE (" + column + ") FROM " +
		expr[i:len(expr)-1] + ", filename='" + column + "') WHERE " +
		column + " NOT IN (" + strings.Join(quotedFiles, ", ") + "))"
}
