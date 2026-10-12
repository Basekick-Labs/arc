package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	sqlutil "github.com/basekick-labs/arc/internal/sql"
)

// readTagColumnsFromParquetFiles reads and unions "arc:tags" metadata from multiple Parquet files.
// Returns the union of all tag column names across files, handling schema evolution where
// newer files may have additional tags. Returns nil if no files have tag metadata.
//
// The "arc:tags" key is written to the Parquet footer by the Arrow writer during ingestion.
// Arrow schema metadata keys are stored directly as Parquet key-value metadata entries,
// so DuckDB's parquet_kv_metadata() reads them without any extra decoding.
func readTagColumnsFromParquetFiles(ctx context.Context, db *sql.DB, filePaths []string) ([]string, error) {
	tagSet := make(map[string]struct{})
	foundAny := false

	for _, filePath := range filePaths {
		tags, err := readTagColumnsFromParquet(ctx, db, filePath)
		if err != nil {
			return nil, err
		}
		if tags != nil {
			foundAny = true
			for _, tag := range tags {
				tagSet[tag] = struct{}{}
			}
		}
	}

	if !foundAny {
		return nil, nil
	}

	result := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		result = append(result, tag)
	}
	sort.Strings(result)
	return result, nil
}

// dedupMode is how a compaction batch removes duplicate rows.
type dedupMode int

const (
	// dedupNone keeps every row: raw ingest without tag metadata.
	dedupNone dedupMode = iota
	// dedupTagsTime keeps one row per (tags..., time): files carry arc:tags.
	dedupTagsTime
	// dedupTimeOnly keeps one row per time. Only for a tagless batch in which
	// every file carries arc:dedup_time and is itself unique on time — the
	// output of a continuous query with no grouping (#521, #1227). Its output
	// is marked in turn, so a later compaction can dedup against it.
	dedupTimeOnly
	// dedupExactRows drops only rows identical in every column: a tagless
	// batch that holds continuous-query output (some file is marked) but does
	// not qualify for dedupTimeOnly. That removes WAL-replayed or replicated
	// copies and keeps every distinct row (#1227).
	dedupExactRows
)

func (m dedupMode) String() string {
	switch m {
	case dedupTagsTime:
		return "tags_time"
	case dedupTimeOnly:
		return "time_only"
	case dedupExactRows:
		return "exact_rows"
	}
	return "none"
}

// readDedupTimeMarkers counts the given Parquet files that carry the
// "arc:dedup_time" marker. The marker says only that its own file was written
// one row per time — continuous-query output (#521). It says nothing about the
// other files of the batch, which is why compaction needs the count rather
// than "any": before #1227 one marked file made the whole batch dedup on time,
// so a grouped continuous query without tag_columns lost every series but one
// in its multi-series windows.
func readDedupTimeMarkers(ctx context.Context, db *sql.DB, filePaths []string) (int, error) {
	marked := 0
	for _, filePath := range filePaths {
		query := fmt.Sprintf(
			`SELECT value FROM parquet_kv_metadata('%s') WHERE key = 'arc:dedup_time'`,
			escapeSQLPath(filePath),
		)
		var val string
		err := db.QueryRowContext(ctx, query).Scan(&val)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("failed to read parquet dedup_time metadata: %w", err)
		}
		if val == "true" {
			marked++
		}
	}
	return marked, nil
}

// sourceFileColumn names the read_parquet filename column the uniqueness probe
// groups by; chosen so it cannot collide with a data column.
const sourceFileColumn = "__arc_source_file"

// filesUniqueOnTime reports whether every file in the list holds at most one
// row per normalized time. The marker is trusted only alongside this check:
// files written before #1227 could carry it while holding several series per
// time (the ingest buffer used to OR the marker across the batches of a
// flush), and a producer bug would look the same. Time is normalized exactly
// as the compaction read normalizes it, so a VARCHAR epoch and a TIMESTAMPTZ
// of the same instant count as one time here as they will in the dedup.
func filesUniqueOnTime(ctx context.Context, db *sql.DB, fileListSQL string) (bool, error) {
	query := fmt.Sprintf(
		`SELECT count(*) FROM (SELECT %s, %s AS t FROM %s GROUP BY ALL HAVING count(*) > 1)`,
		sourceFileColumn, timeNormalizeExpr,
		sqlutil.ReadParquet(fileListSQL, "union_by_name=true", "filename='"+sourceFileColumn+"'"))
	var dupKeys int64
	if err := db.QueryRowContext(ctx, query).Scan(&dupKeys); err != nil {
		return false, fmt.Errorf("failed to probe per-file time uniqueness: %w", err)
	}
	return dupKeys == 0, nil
}

// readTagColumnsFromParquet reads the "arc:tags" metadata from a single Parquet file.
// Returns nil if no tag metadata is found (file written before dedup feature).
func readTagColumnsFromParquet(ctx context.Context, db *sql.DB, filePath string) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT value FROM parquet_kv_metadata('%s') WHERE key = 'arc:tags'`,
		escapeSQLPath(filePath),
	)

	var tagValue string
	err := db.QueryRowContext(ctx, query).Scan(&tagValue)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet metadata: %w", err)
	}

	if tagValue == "" {
		return nil, nil
	}

	tags := strings.Split(tagValue, ",")

	// Validate tag names: reject anything that doesn't look like a safe column identifier.
	// This prevents SQL injection via crafted Parquet metadata (arc:tags value).
	for _, tag := range tags {
		if tag == "" || !isValidIdentifier(tag) {
			return nil, fmt.Errorf("invalid tag column name in parquet metadata: %q", tag)
		}
	}

	return tags, nil
}

// isValidIdentifier checks if a string is a safe SQL column identifier.
// Allows alphanumeric characters, underscores, and hyphens (matching Arc's measurement name rules).
func isValidIdentifier(name string) bool {
	if len(name) == 0 {
		return false
	}
	for _, c := range name {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// dedupStagingTable is the temp table the dedup path materializes into. The
// caller (job.go) pins both statements to one connection via db.Conn (a DuckDB
// TEMP table is connection-local) and DROPs it before returning the connection
// to the pool — the DuckDB driver does not reset sessions, so a leftover temp
// table would otherwise retain a full partition's rows on a pooled connection.
// CREATE OR REPLACE additionally guards against any stale table on a reused
// connection. A fixed name is safe: TEMP tables are per-connection, so even
// concurrent jobs (distinct pinned connections) do not collide.
const dedupStagingTable = "arc_compaction_staged"

// buildCompactionQuery builds the DuckDB statement(s) for compaction. It returns
// a slice of statements to execute in order on the same connection.
//
// mode dedupNone returns a single standard COPY (zero overhead). Every other
// mode returns two statements (CREATE OR REPLACE TEMP TABLE ... AS <normalized
// read> ; COPY <dedup over the table>) — see the long comment on the dedup
// branch for why a temp table is required rather than a CTE or subquery.
//
// dedupTagsTime keys on (tagColumns..., "time"); dedupTimeOnly on "time" alone,
// which Job.decideDedup chooses only for a tagless batch whose every file is
// marked and unique on time (#521, #1227); dedupExactRows keeps one copy of
// each fully identical row.
func buildCompactionQuery(fileListSQL, orderByClause, outputFile string, tagColumns []string, mode dedupMode) []string {
	escapedOutput := escapeSQLPath(outputFile)

	if mode == dedupNone {
		// Standard compaction — no dedup overhead.
		// Normalize "time" to TIMESTAMP so a partition that mixes file schemas
		// (some time=TIMESTAMP, some time=VARCHAR from a misbehaving writer)
		// still compacts instead of failing the column bind. See timeNormalizeReplace.
		return []string{fmt.Sprintf(`
		COPY (
			SELECT * REPLACE (%s) FROM %s
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, timeNormalizeReplace, sqlutil.ReadParquet(fileListSQL, "union_by_name=true"), orderByClause, escapedOutput)}
	}

	// Dedup compaction. Every mode stages the normalized read in a temp table
	// first; the window modes then keep one row per (tags, time) or per time,
	// and dedupExactRows keeps one copy of each identical row.
	//
	// The "time" normalization MUST be fully materialized into a typed relation
	// BEFORE the dedup window binds. This requires a real temp table, not a CTE
	// or subquery. Three DuckDB facts, all verified empirically against Arc's
	// linked DuckDB on the real 5-column cpu schema (host,time,value,cpu_idle,
	// cpu_user) — NOT a reduced fixture, which hides the bug:
	//
	//  1. A dedup window that references "time" over a many-file
	//     `read_parquet(union_by_name=true)` fails to bind with
	//     `Failed to bind column reference "time": TIMESTAMP WITH TIME ZONE !=
	//     VARCHAR` — EVEN WHEN EVERY FILE'S time IS ALREADY TIMESTAMPTZ and no
	//     VARCHAR exists anywhere. The "VARCHAR" is a phantom the binder
	//     introduces while resolving the window's column reference down through
	//     the union scan. This is what wedged production/cpu on every cycle
	//     (NOT a leftover mixed-type file — the partition was uniformly
	//     TIMESTAMPTZ). It only triggers above a column/file-count threshold, so
	//     the original #493 3-column test never saw it.
	//  2. A plain CTE does NOT fix it — DuckDB inlines the CTE (and OFFSET 0,
	//     contrary to folklore, does not reliably block the inline on the real
	//     schema; it worked on a reduced fixture and failed on real files).
	//  3. The flat no-CTE form `SELECT * REPLACE(time...) ... QUALIFY` DOES bind,
	//     but QUALIFY is evaluated before the projection, so the window runs over
	//     the RAW time. A duplicate written once as TIMESTAMPTZ and once as a
	//     VARCHAR epoch then lands in two window partitions and BOTH survive —
	//     silent under-dedup (verified: 2 rows out instead of 1).
	//
	// CREATE OR REPLACE TEMP TABLE AS forces the normalized, typed result to be
	// fully materialized first; the dedup window then binds against the table's
	// concrete TIMESTAMPTZ "time" — binds correctly (fact 1) and dedups
	// correctly (fact 3). Verified: real all-TIMESTAMPTZ partition binds; a
	// partition with an injected VARCHAR-time duplicate dedups to one row.
	//
	// PARTITION BY all tag columns + time: rows with identical (tags, time) are
	// duplicates; one is kept. This is the dedup key, not the output order — the
	// output is sorted by the separate orderByClause (time).
	stage := fmt.Sprintf(
		`CREATE OR REPLACE TEMP TABLE %s AS SELECT * REPLACE (%s) FROM %s`,
		dedupStagingTable, timeNormalizeReplace, sqlutil.ReadParquet(fileListSQL, "union_by_name=true"))

	if mode == dedupExactRows {
		// Only rows identical in every column collapse: a replayed or
		// replicated copy. The output stays unmarked — it may hold several
		// rows per time.
		// DISTINCT is a hash aggregate, which does not keep insertion order,
		// so without configured sort keys the output is ordered by time
		// explicitly rather than left scrambled.
		order := orderByClause
		if order == "" {
			order = `ORDER BY "time"`
		}
		return []string{stage, fmt.Sprintf(`
		COPY (
			SELECT DISTINCT * FROM %s
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, dedupStagingTable, order, escapedOutput)}
	}

	// A compacted file is marked arc:dedup_time if and only if time-only dedup
	// ran on it, so it really is one row per time and a later compaction (the
	// next hourly pass, the daily tier) can dedup a re-emitted window against
	// it. Never under dedupTagsTime: that output is one row per (tags, time),
	// several per time, and it carries no arc:tags to say so (#1227).
	kvMetadata := ""
	if mode == dedupTimeOnly {
		kvMetadata = `,
			KV_METADATA {'arc:dedup_time': 'true'}`
	}

	// Build the PARTITION BY key: all tag columns plus "time". When there are no
	// tag columns (dedupTimeOnly), the key is "time" alone — build the list
	// without a dangling leading comma.
	if mode == dedupTimeOnly {
		tagColumns = nil
	}
	quotedKeys := make([]string, 0, len(tagColumns)+1)
	for _, tag := range tagColumns {
		// Double-quote escaping: DuckDB treats "" inside a quoted identifier as a literal "
		// (same as PostgreSQL). This prevents identifier breakout even if validation is bypassed.
		escaped := strings.ReplaceAll(tag, `"`, `""`)
		quotedKeys = append(quotedKeys, fmt.Sprintf(`"%s"`, escaped))
	}
	quotedKeys = append(quotedKeys, `"time"`)
	partitionBy := strings.Join(quotedKeys, ", ")

	copyOut := fmt.Sprintf(`
		COPY (
			SELECT * FROM %s
			QUALIFY ROW_NUMBER() OVER (
				PARTITION BY %s
				ORDER BY "time" DESC
			) = 1
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880%s
		)
	`, dedupStagingTable, partitionBy, orderByClause, escapedOutput, kvMetadata)

	return []string{stage, copyOut}
}

// timeNormalizeReplace is the DuckDB `REPLACE (...)` expression that coerces a
// possibly-mixed-type "time" column to a single canonical type during compaction.
// A partition can contain files where "time" is a proper timestamp and others
// where a misbehaving writer wrote it as VARCHAR; read_parquet(union_by_name=true)
// reconciles column NAMES but not conflicting TYPES, so without this the column
// bind fails (TIMESTAMP WITH TIME ZONE != VARCHAR) and the partition can never
// compact.
//
// The target type is TIMESTAMPTZ (TIMESTAMP WITH TIME ZONE), NOT naive TIMESTAMP:
// Arc's Arrow writer types time as Timestamp_us{TimeZone:"UTC"}, which DuckDB
// reads/writes as TIMESTAMP WITH TIME ZONE. Normalizing to naive TIMESTAMP would
// strip the zone and make the compacted file's time type disagree with future
// raw files (TIMESTAMPTZ) — recreating the very bind mismatch this fixes on the
// NEXT compaction cycle. Casting to TIMESTAMPTZ keeps compacted output identical
// to the normal ingest schema.
//
// COALESCE order handles every case:
//   - already TIMESTAMPTZ, or an ISO/datetime string → TRY_CAST(... AS TIMESTAMPTZ)
//   - epoch-microseconds written as a string ("1717689600000000") →
//     make_timestamptz(BIGINT micros). Arc stores time as microseconds, which is
//     exactly make_timestamptz's unit; it yields a UTC-anchored TIMESTAMPTZ.
const timeNormalizeReplace = timeNormalizeExpr + ` AS "time"`

// timeNormalizeExpr is the expression timeNormalizeReplace binds to "time".
const timeNormalizeExpr = `COALESCE(TRY_CAST("time" AS TIMESTAMPTZ), make_timestamptz(TRY_CAST("time" AS BIGINT)))`

// parquetFilesHaveTimeColumn reports whether the unified schema of the given
// Parquet files contains a "time" column. DESCRIBE over read_parquet reads
// footers only (no data scan), and union_by_name gives exactly the schema the
// compaction COPY will see — so "true" here guarantees the REPLACE("time")
// normalization and the default ORDER BY "time" can bind. When only SOME files
// have the column, union_by_name backfills the others with NULLs and the query
// still binds; the probe is false only when NO file has it (e.g. a dataset
// loaded outside Arc's ingest path, which always writes "time").
// fileListSQL is a DuckDB array literal or single quoted path, as built by
// Job.compactFiles.
func parquetFilesHaveTimeColumn(ctx context.Context, db *sql.DB, fileListSQL string) (bool, error) {
	query := fmt.Sprintf(
		`SELECT COUNT(*) FROM (DESCRIBE SELECT * FROM %s) WHERE column_name = 'time'`,
		sqlutil.ReadParquet(fileListSQL, "union_by_name=true"),
	)
	var n int64
	if err := db.QueryRowContext(ctx, query).Scan(&n); err != nil {
		return false, fmt.Errorf("failed to probe parquet schema for time column: %w", err)
	}
	return n > 0, nil
}

// countParquetRows counts total rows across Parquet files using metadata (no data scan).
// fileListSQL is a DuckDB array literal or single quoted path, as built by
// Job.compactFiles — both forms have a parquet_file_metadata overload.
func countParquetRows(ctx context.Context, db *sql.DB, fileListSQL string) (int64, error) {
	query := fmt.Sprintf(`SELECT SUM(num_rows) FROM parquet_file_metadata(%s)`, fileListSQL)
	var count int64
	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count parquet rows: %w", err)
	}
	return count, nil
}
