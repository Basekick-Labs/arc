// Package fieldschema keeps a measurement's field schema stable across time
// ranges (#914).
//
// Arc rewrites FROM <measurement> into read_parquet(<selected files>,
// union_by_name=true). DuckDB binds the union of the SELECTED files' schemas,
// so a field that no file in a narrow time range carries fails to bind, while
// the same projection over a wider range succeeds and yields NULLs. Grafana
// panels then work or break depending on the zoom level.
//
// The fix is a per-measurement "anchor": a zero-row Parquet file carrying
// every field the measurement has ever been written with. The anchor is
// listed first in every read_parquet call for the measurement. union_by_name
// then fills the anchor's columns with typed NULLs for files that lack them,
// keeps SELECT * order stable, and returns zero rows with the full schema when
// the anchor is the only file scanned. DuckDB's own type promotion still
// applies between the anchor and the files, and the plan keeps a single
// READ_PARQUET node with projection and filter pushdown (verified, see the
// tests).
//
// Two copies exist. The source of truth is the stored anchor at
// _schema/{database}/{measurement}.parquet on the storage backend, shared by
// every node and surviving restarts. Each process also materializes a local
// copy under its DuckDB-allowlisted temp directory and that is the path
// DuckDB reads: a local file cannot vanish behind the query, costs no object
// store round trip per query, and DuckDB revalidates its Parquet metadata
// cache on local mtime, so a schema change is visible on the next query.
//
// Type policy: the anchor records the NARROWEST type observed for a field
// (BOOLEAN < BIGINT < DOUBLE < VARCHAR; INT32 < BIGINT; DECIMAL < DOUBLE;
// TIMESTAMP < TIMESTAMPTZ). An anchor at or below every file's type can never
// change what DuckDB binds for a range whose files carry the column, which is
// what makes the anchor incapable of breaking a query that works today. A
// widening anchor would: one malformed batch that wrote a field as a string
// would turn every later sum() over that field into a Binder Error. Types
// that DuckDB cannot order (BIGINT vs DECIMAL) keep the stored type and are
// logged.
//
// The registry is additive: ingest and bootstrap only ever add fields.
// Deleting a database deletes its anchors; nothing else removes a field.
package fieldschema
