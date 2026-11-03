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
	// dedupNone keeps every row: raw ingest without tag metadata or a
	// continuous-query marker.
	dedupNone dedupMode = iota
	// dedupExact drops a row only when another row of the batch is identical
	// to it in every column — time, every tag and every field (#1232). Rows
	// that share their tags and time but differ in any field are different
	// data and are all kept.
	dedupExact
)

func (m dedupMode) String() string {
	if m == dedupExact {
		return "exact_rows"
	}
	return "none"
}

// exactDedupMarker is the Parquet footer key that marks continuous-query output
// without tag columns, opting the file into exact-row dedup at compaction
// (#1232). It replaced legacyDedupTimeMarker as the key Arc writes because
// builds before #1232 read the old key as "dedup this batch on time alone": a
// file marked with it here, which may hold several rows per time, would lose
// rows to a node rolled back, or not yet upgraded, that compacts it.
const exactDedupMarker = "arc:dedup_exact"

// legacyDedupTimeMarker is the key written before #1232 on continuous-query
// output that was one row per time (#521). Still read, never written.
const legacyDedupTimeMarker = "arc:dedup_time"

// readAnyContinuousQueryMarker reports whether any of the given Parquet files
// carries exactDedupMarker or legacyDedupTimeMarker with the value "true": the
// batch holds continuous-query output without tag columns and gets exact-row
// dedup. One marked file is enough — exact-row dedup never deletes a row that
// differs from every other, so the marker only decides whether the batch is
// checked at all.
func readAnyContinuousQueryMarker(ctx context.Context, db *sql.DB, filePaths []string) (bool, error) {
	for _, filePath := range filePaths {
		query := fmt.Sprintf(
			`SELECT value FROM parquet_kv_metadata('%s') WHERE key IN ('%s', '%s')`,
			escapeSQLPath(filePath), exactDedupMarker, legacyDedupTimeMarker,
		)
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return false, fmt.Errorf("failed to read parquet dedup marker metadata: %w", err)
		}
		marked := false
		for rows.Next() {
			var val string
			if err := rows.Scan(&val); err != nil {
				rows.Close()
				return false, fmt.Errorf("failed to read parquet dedup marker metadata: %w", err)
			}
			if val == "true" {
				marked = true
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return false, fmt.Errorf("failed to read parquet dedup marker metadata: %w", err)
		}
		if marked {
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

// dedupKeysTable holds the (tags..., time) keys that occur on more than one
// staged row; same connection-local lifetime as dedupStagingTable.
const dedupKeysTable = "arc_compaction_dupkeys"

// buildCompactionQuery builds the DuckDB statement(s) for compaction. It returns
// a slice of statements to execute in order on the same connection.
//
// mode dedupNone returns a single standard COPY (zero overhead). dedupExact
// returns three statements: stage the normalized read in a temp table, collect
// the (tagColumns..., time) keys that occur more than once, then copy every row
// whose key is unique as is and, for the rows whose key repeats, one copy of
// each distinct row. Comparing whole rows only inside repeated keys keeps the
// cost close to the old one-row-per-key window — a full DISTINCT over the
// batch hashes every column of every row, which is prohibitive on wide
// measurements (#1232). With no tag columns (continuous-query output marked
// exactDedupMarker) the key is time alone, and the output is marked in turn so a
// later pass checks it again.
func buildCompactionQuery(fileListSQL, orderByClause, outputFile string, tagColumns []string, mode dedupMode) []string {
	escapedOutput := escapeSQLPath(outputFile)

	if mode == dedupNone {
		// Standard compaction — no dedup overhead.
		// Normalize "time" to TIMESTAMP so a partition that mixes file schemas
		// (some time=TIMESTAMP, some time=VARCHAR from a misbehaving writer)
		// still compacts instead of failing the column bind. See timeNormalizeReplace.
		//
		// The ORDER BY applies to a derived table so it binds to the
		// normalized "time", never a raw VARCHAR one (see buildOrderByClause).
		return []string{fmt.Sprintf(`
		COPY (
			SELECT * FROM (SELECT * REPLACE (%s) FROM %s)
			%s
		) TO '%s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880
		)
	`, timeNormalizeReplace, sqlutil.ReadParquet(fileListSQL, "union_by_name=true"), orderByClause, escapedOutput)}
	}

	// Dedup compaction stages the normalized read in a temp table first, and
	// every aggregate below runs over that table, never over the raw scan.
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
	// (These facts were found with the one-row-per-key window this code used
	// before #1232; the GROUP BY and joins below rely on the same temp table
	// for the same reason.)
	stage := fmt.Sprintf(
		`CREATE OR REPLACE TEMP TABLE %s AS SELECT * REPLACE (%s) FROM %s`,
		dedupStagingTable, timeNormalizeReplace, sqlutil.ReadParquet(fileListSQL, "union_by_name=true"))

	// The key: every tag column plus "time". It only narrows where whole rows
	// are compared; it never decides on its own that two rows are duplicates.
	quotedKeys := make([]string, 0, len(tagColumns)+1)
	for _, tag := range tagColumns {
		// Double-quote escaping: DuckDB treats "" inside a quoted identifier as a literal "
		// (same as PostgreSQL). This prevents identifier breakout even if validation is bypassed.
		escaped := strings.ReplaceAll(tag, `"`, `""`)
		quotedKeys = append(quotedKeys, fmt.Sprintf(`"%s"`, escaped))
	}
	quotedKeys = append(quotedKeys, `"time"`)

	// IS NOT DISTINCT FROM, not =: a tag is NULL on every row of a file that
	// lacks the column (union_by_name), and = would match no key for those
	// rows, dropping them from BOTH branches below.
	match := make([]string, len(quotedKeys))
	for i, k := range quotedKeys {
		match[i] = fmt.Sprintf("k.%s IS NOT DISTINCT FROM s.%s", k, k)
	}
	keyMatch := strings.Join(match, " AND ")

	dupKeys := fmt.Sprintf(
		`CREATE OR REPLACE TEMP TABLE %s AS SELECT %s FROM %s GROUP BY ALL HAVING count(*) > 1`,
		dedupKeysTable, strings.Join(quotedKeys, ", "), dedupStagingTable)

	// UNION ALL plus DISTINCT keeps no insertion order, so the output is
	// always ordered explicitly; by time when no sort keys are configured.
	order := orderByClause
	if order == "" {
		order = `ORDER BY "time"`
	}

	kvMetadata := ""
	if len(tagColumns) == 0 {
		kvMetadata = `,
			KV_METADATA {'` + exactDedupMarker + `': 'true'}`
	}

	// The UNION is a derived table: DuckDB rejects an expression (the
	// hour of "time" in buildOrderByClause) in an ORDER BY directly on a UNION.
	copyOut := fmt.Sprintf(`
		COPY (
			SELECT * FROM (
				(SELECT * FROM %[1]s s WHERE NOT EXISTS (SELECT 1 FROM %[2]s k WHERE %[3]s))
				UNION ALL
				(SELECT DISTINCT * FROM %[1]s s WHERE EXISTS (SELECT 1 FROM %[2]s k WHERE %[3]s))
			)
			%[4]s
		) TO '%[5]s' (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE 122880%[6]s
		)
	`, dedupStagingTable, dedupKeysTable, keyMatch, order, escapedOutput, kvMetadata)

	return []string{stage, dupKeys, copyOut}
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

// parquetFilesColumns returns the column names of the unified schema of the
// given Parquet files, keyed by asciiLower(name) — DuckDB binds column names
// without regard to ASCII case — with the name as stored as the value. DESCRIBE over read_parquet reads footers only (no data scan), and
// union_by_name gives exactly the schema the compaction COPY will see.
// fileListSQL is a DuckDB array literal or single quoted path, as built by
// Job.compactFiles.
func parquetFilesColumns(ctx context.Context, db *sql.DB, fileListSQL string) (map[string]string, error) {
	query := fmt.Sprintf(
		`SELECT column_name FROM (DESCRIBE SELECT * FROM %s)`,
		sqlutil.ReadParquet(fileListSQL, "union_by_name=true"),
	)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to probe parquet schema: %w", err)
	}
	defer rows.Close()
	cols := make(map[string]string)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to probe parquet schema: %w", err)
		}
		cols[asciiLower(name)] = name
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to probe parquet schema: %w", err)
	}
	return cols, nil
}

// parquetFilesHaveTimeColumn reports whether the unified schema of the given
// Parquet files contains a "time" column, so the REPLACE("time")
// normalization and the default ORDER BY "time" can bind. When only SOME files
// have the column, union_by_name backfills the others with NULLs and the query
// still binds; the probe is false only when NO file has it (e.g. a dataset
// loaded outside Arc's ingest path, which always writes "time").
func parquetFilesHaveTimeColumn(ctx context.Context, db *sql.DB, fileListSQL string) (bool, error) {
	cols, err := parquetFilesColumns(ctx, db, fileListSQL)
	if err != nil {
		return false, err
	}
	return hasTimeColumn(cols), nil
}

// hasTimeColumn reports whether cols, from parquetFilesColumns, holds a column
// named exactly "time", the name Arc's ingest always writes.
func hasTimeColumn(cols map[string]string) bool {
	return cols["time"] == "time"
}

// asciiLower lower-cases ASCII letters only. DuckDB matches column names
// case-insensitively for ASCII; full Unicode folding would also match names
// DuckDB keeps apart (the Kelvin sign folds to "k").
func asciiLower(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'A' <= c && c <= 'Z' {
			b[i] = c + ('a' - 'A')
		}
	}
	return string(b)
}

// tagsWithColumns splits tag names into those the files have a column for and
// those they do not. A name without a column cannot bind in the dedup query,
// and every later cycle would fail the same way (#1258): files written before
// ingest checked its tag names carry them — a line-protocol tag key starting
// with '_', or one with an escaped comma, which arc:tags splits in two.
// Leaving a name out only narrows the key the exact-row dedup groups on; it
// never makes two different rows equal.
func tagsWithColumns(tags []string, cols map[string]string) (kept, missing []string) {
	for _, tag := range tags {
		if _, ok := cols[asciiLower(tag)]; ok {
			kept = append(kept, tag)
		} else {
			missing = append(missing, tag)
		}
	}
	return kept, missing
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
