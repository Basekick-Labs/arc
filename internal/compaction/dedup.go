package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
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

// readDedupTimeFromParquetFiles reports whether ANY of the given Parquet files
// carries the "arc:dedup_time" marker. That marker is written only by producers
// whose data model is one-row-per-(tags,time) — continuous queries (#521) — and
// tells compaction it is safe to dedup on time even when there are no tag
// columns. A missing marker (all raw-ingest files) returns false, so ingest
// dedup behavior is unchanged.
func readDedupTimeFromParquetFiles(ctx context.Context, db *sql.DB, filePaths []string) (bool, error) {
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
			return false, fmt.Errorf("failed to read parquet dedup_time metadata: %w", err)
		}
		if val == "true" {
			return true, nil
		}
	}
	return false, nil
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
// Dedup runs when tagColumns is non-empty OR dedupTime is set. Without either,
// returns a single standard COPY (zero overhead). When deduping, returns two
// statements (CREATE OR REPLACE TEMP TABLE ... AS <normalized read> ; COPY
// <dedup window over the table>) — see the long comment on the dedup branch for
// why a temp table is required rather than a CTE or subquery.
//
// The dedup key is (tagColumns..., "time"). With no tag columns but dedupTime
// set (a no-group-by continuous query, #521), the key is "time" alone — one row
// per timestamp. This is safe ONLY because dedupTime is written exclusively by
// producers whose data is one-row-per-time; raw ingest never sets it.
func buildCompactionQuery(fileListSQL, orderByClause, outputFile string, tagColumns []string, dedupTime bool) []string {
	escapedOutput := escapeSQLPath(outputFile)

	if len(tagColumns) == 0 && !dedupTime {
		// Standard compaction — no dedup overhead.
		// Normalize "time" to TIMESTAMP so a partition that mixes file schemas
		// (some time=TIMESTAMP, some time=VARCHAR from a misbehaving writer)
		// still compacts instead of failing the column bind. See timeNormalizeReplace.
		return []string{fmt.Sprintf(`
		COPY (
			SELECT * REPLACE (%s) FROM read_parquet(%s, union_by_name=true)
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, timeNormalizeReplace, fileListSQL, orderByClause, escapedOutput)}
	}

	// Dedup compaction — keep one row per unique (tags, time) key.
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
	// Build the PARTITION BY key: all tag columns plus "time". When there are no
	// tag columns (a no-group-by CQ reaching here via dedupTime), the key is
	// "time" alone — build the list without a dangling leading comma.
	quotedKeys := make([]string, 0, len(tagColumns)+1)
	for _, tag := range tagColumns {
		// Double-quote escaping: DuckDB treats "" inside a quoted identifier as a literal "
		// (same as PostgreSQL). This prevents identifier breakout even if validation is bypassed.
		escaped := strings.ReplaceAll(tag, `"`, `""`)
		quotedKeys = append(quotedKeys, fmt.Sprintf(`"%s"`, escaped))
	}
	quotedKeys = append(quotedKeys, `"time"`)
	partitionBy := strings.Join(quotedKeys, ", ")

	stage := fmt.Sprintf(
		`CREATE OR REPLACE TEMP TABLE %s AS SELECT * REPLACE (%s) FROM read_parquet(%s, union_by_name=true)`,
		dedupStagingTable, timeNormalizeReplace, fileListSQL)

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
			ROW_GROUP_SIZE 122880
		)
	`, dedupStagingTable, partitionBy, orderByClause, escapedOutput)

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
const timeNormalizeReplace = `COALESCE(TRY_CAST("time" AS TIMESTAMPTZ), make_timestamptz(TRY_CAST("time" AS BIGINT))) AS "time"`

// countParquetRows counts total rows across Parquet files using metadata (no data scan).
// fileListSQL is a DuckDB array literal like "['file1.parquet', 'file2.parquet']".
func countParquetRows(ctx context.Context, db *sql.DB, fileListSQL string) (int64, error) {
	query := fmt.Sprintf(`SELECT SUM(num_rows) FROM parquet_metadata(%s)`, fileListSQL)
	var count int64
	err := db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count parquet rows: %w", err)
	}
	return count, nil
}
