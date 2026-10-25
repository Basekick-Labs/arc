package api

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/basekick-labs/arc/internal/database"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"github.com/rs/zerolog"
)

// A DELETE rewrite COPYs a file's surviving rows to a new file. Per thread in
// flight that COPY holds one decoded source row group plus the parquet
// writer's per-column state, and most of it is outside DuckDB's
// memory_limit. On a wide file and a many-core node the main instance runs the
// process out of memory: 3,100 columns, 46 cores and a 4 GB limit took 39
// minutes per row group of spilling, then failed. Such a file is rewritten on
// a private single-threaded instance instead (database.OpenRewriteDB),
// measured at 4 GB: 3,100 columns x 8 row groups in 36 s at 2.0 GB RSS,
// 10,000 columns in 23 s at 2.6 GB.
const (
	// Decoded bytes per value in a DuckDB vector: 8 B and a validity bit for
	// numbers and timestamps; a 16 B string_t (and heap beyond 12 bytes) for
	// strings.
	decodedFixedValueBytes  = 9
	decodedStringValueBytes = 16
	// The parquet writer's state per column per thread, measured on DuckDB
	// 1.5.6; it does not shrink with the codec, the dictionary limit or a row
	// group byte cap.
	writerStatePerColumn = 150 << 10
	// The single-threaded rewrite sizes row groups so one decodes to about
	// this much, in whole vectors (2,048 rows at 10,000 columns, 4,096 at
	// 3,100), less under a small memory limit.
	wideRewriteRowGroupBytes = 128 << 20
	// DuckDB fills row groups in whole vectors of 2,048 rows, so a smaller
	// or uneven ROW_GROUP_SIZE is rounded up when written.
	duckdbVectorRows = 2048
	// Floor for the private instance's memory_limit.
	minRewriteMemoryBytes = 256 << 20
	// Prefix of the private instance's scratch directories under the
	// DELETE staging directory; leftovers of a crash are removed at startup.
	rewriteScratchPrefix = "delete-rewrite-"
)

// rewriteKVKeys are the parquet key/value metadata entries a rewrite carries
// over. Arc reads them back: arc:tags (dedup columns, tag-first schema order),
// arc:dedup_exact (exact-row dedup at compaction; still true after rows are
// removed), arc:decimals (decimal column specs), and the legacy
// arc:dedup_time, kept as the file had it. Never ARROW:schema: it describes
// the ingest writer's Arrow schema, not the file DuckDB writes.
var rewriteKVKeys = map[string]bool{
	"arc:tags":        true,
	"arc:dedup_exact": true,
	"arc:decimals":    true,
	"arc:dedup_time":  true,
}

// rewriteShape is what a rewrite needs from the source's footer.
type rewriteShape struct {
	leafColumns     int
	bytesPerRow     int64 // decoded, summed over leaf columns
	maxRowGroupRows int64
	rowGroups       int
	kv              [][2]string // allowlisted entries, sorted by key
}

// probeRewriteShape reads the source's footer through db: its leaf columns,
// row groups and key/value metadata. Footer only (a ranged read on S3).
func probeRewriteShape(ctx context.Context, db *sql.DB, path string, logger zerolog.Logger) (rewriteShape, error) {
	var s rewriteShape
	src := sqlutil.QuoteStringLiteral(path)
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT type, count(*) FROM parquet_schema(%s) WHERE num_children IS NULL GROUP BY type`, src))
	if err != nil {
		return s, fmt.Errorf("read schema: %w", err)
	}
	for rows.Next() {
		var typ sql.NullString
		var n int64
		if err := rows.Scan(&typ, &n); err != nil {
			rows.Close()
			return s, fmt.Errorf("read schema: %w", err)
		}
		per := int64(decodedFixedValueBytes)
		if typ.String == "BYTE_ARRAY" || typ.String == "FIXED_LEN_BYTE_ARRAY" {
			per = decodedStringValueBytes
		}
		s.leafColumns += int(n)
		s.bytesPerRow += per * n
	}
	if err := rows.Close(); err != nil {
		return s, fmt.Errorf("read schema: %w", err)
	}
	if err := rows.Err(); err != nil {
		return s, fmt.Errorf("read schema: %w", err)
	}
	if err := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count(DISTINCT row_group_id), coalesce(max(row_group_num_rows), 0) FROM parquet_metadata(%s)`, src)).Scan(&s.rowGroups, &s.maxRowGroupRows); err != nil {
		return s, fmt.Errorf("read row groups: %w", err)
	}
	kvRows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT key, value FROM parquet_kv_metadata(%s)`, src))
	if err != nil {
		return s, fmt.Errorf("read key/value metadata: %w", err)
	}
	defer kvRows.Close()
	for kvRows.Next() {
		// BLOBs scanned raw: a ::VARCHAR cast would turn a quote in a tag
		// name into the escape \x27.
		var k, v []byte
		if err := kvRows.Scan(&k, &v); err != nil {
			return s, fmt.Errorf("read key/value metadata: %w", err)
		}
		if !rewriteKVKeys[string(k)] {
			continue
		}
		if !utf8.Valid(v) {
			logger.Warn().Str("file", filepath.Base(path)).Str("key", string(k)).Msg("Not carrying a parquet metadata entry that is not valid UTF-8 into the rewritten file")
			continue
		}
		s.kv = append(s.kv, [2]string{string(k), string(v)})
	}
	if err := kvRows.Err(); err != nil {
		return s, fmt.Errorf("read key/value metadata: %w", err)
	}
	sort.Slice(s.kv, func(i, j int) bool { return s.kv[i][0] < s.kv[j][0] })
	return s, nil
}

// perThreadBytes estimates what one COPY thread holds for this source.
func (s rewriteShape) perThreadBytes() int64 {
	return s.bytesPerRow*s.maxRowGroupRows + writerStatePerColumn*int64(s.leafColumns)
}

// needsSingleThread reports whether rewriting s on the main instance could
// exceed half its memory limit: one source row group in flight per thread, up
// to the number of row groups. A package variable so tests choose the path.
var needsSingleThread = func(s rewriteShape, memoryBytes int64, threads int) bool {
	inFlight := int64(threads)
	if int64(s.rowGroups) < inFlight {
		inFlight = int64(s.rowGroups)
	}
	if inFlight < 1 {
		inFlight = 1
	}
	return s.perThreadBytes()*inFlight > memoryBytes/2
}

// singleThreadRowGroupRows sizes the private rewrite's row groups: about
// wideRewriteRowGroupBytes decoded, or a quarter of the instance's
// memoryLimit when that is smaller (the row group being written and the
// source row group being read are both in memory at once).
func (s rewriteShape) singleThreadRowGroupRows(memoryLimit int64) int64 {
	if s.bytesPerRow <= 0 {
		return parquetRowGroupSize
	}
	target := int64(wideRewriteRowGroupBytes)
	if q := memoryLimit / 4; q > 0 && q < target {
		target = q
	}
	n := target / s.bytesPerRow / duckdbVectorRows * duckdbVectorRows
	if n < duckdbVectorRows {
		n = duckdbVectorRows
	}
	if n > parquetRowGroupSize {
		n = parquetRowGroupSize
	}
	return n
}

// kvMetadataOption renders kv as a COPY option, empty when there is none.
func kvMetadataOption(kv [][2]string) string {
	if len(kv) == 0 {
		return ""
	}
	parts := make([]string, len(kv))
	for i, e := range kv {
		parts[i] = sqlutil.QuoteStringLiteral(e[0]) + ": " + sqlutil.QuoteStringLiteral(e[1])
	}
	return ",\n\t\t\tKV_METADATA {" + strings.Join(parts, ", ") + "}"
}

// survivorsCopy is the COPY of src's rows the DELETE keeps into dst.
func survivorsCopy(src, whereClause, dst string, rowGroupRows int64, kv [][2]string) string {
	return fmt.Sprintf(`
		COPY (
			SELECT * FROM %s WHERE NOT COALESCE((%s), false)
		) TO %s (
			FORMAT PARQUET,
			COMPRESSION ZSTD,
			COMPRESSION_LEVEL 3,
			ROW_GROUP_SIZE %d%s
		)`, sqlutil.ReadParquet(sqlutil.QuoteStringLiteral(src)), whereClause, sqlutil.QuoteStringLiteral(dst), rowGroupRows, kvMetadataOption(kv))
}

// openRewriteDB is database.OpenRewriteDB; tests record the private path.
var openRewriteDB = database.OpenRewriteDB

// copySurvivors writes src's surviving rows to the local file dst, keeping
// their order and Arc's parquet metadata. src is read by the main instance
// (a local path, or an s3:// / azure:// URL). A source too wide for the main
// instance is rewritten on a private single-threaded one; fetch, when set,
// first copies a remote source to a local path, since the private instance
// has no remote access. It reports whether the private path ran.
//
// The caller holds rewriteMu, so at most one private instance is open per
// process.
func (h *DeleteHandler) copySurvivors(ctx context.Context, src, whereClause, dst string, fetch func(ctx context.Context, localPath string) error) (bool, error) {
	db := h.db.DB()
	shape, err := probeRewriteShape(ctx, db, src, h.logger)
	if err != nil {
		// Without the footer the file cannot be sized; the main instance
		// rewrites it as before this check existed. The COPY reports the
		// file's own read errors.
		h.logger.Warn().Err(err).Str("file", filepath.Base(src)).Msg("Could not read the file's footer before rewriting it; rewriting on the main database")
		return false, database.ExecPreservingInsertionOrder(ctx, db, survivorsCopy(src, whereClause, dst, parquetRowGroupSize, nil))
	}
	memoryBytes, threads, err := h.db.EffectiveLimits(ctx)
	if err != nil {
		h.logger.Warn().Err(err).Msg("Could not read DuckDB's memory limit and threads before a rewrite; rewriting on the main database")
		return false, database.ExecPreservingInsertionOrder(ctx, db, survivorsCopy(src, whereClause, dst, parquetRowGroupSize, shape.kv))
	}
	if !needsSingleThread(shape, memoryBytes, threads) {
		return false, database.ExecPreservingInsertionOrder(ctx, db, survivorsCopy(src, whereClause, dst, parquetRowGroupSize, shape.kv))
	}

	// From here a failure fails this file's rewrite: the main instance is
	// the path that runs out of memory.
	parent := h.tempDir
	if parent == "" {
		parent = filepath.Dir(dst)
	}
	scratch, err := os.MkdirTemp(parent, rewriteScratchPrefix+"*")
	if err != nil {
		return true, fmt.Errorf("create rewrite scratch directory: %w", err)
	}
	defer os.RemoveAll(scratch)
	spill := filepath.Join(scratch, "spill")
	if err := os.Mkdir(spill, 0o700); err != nil {
		return true, fmt.Errorf("create rewrite spill directory: %w", err)
	}
	local := src
	if fetch != nil {
		local = filepath.Join(scratch, "source.parquet")
		if err := fetch(ctx, local); err != nil {
			return true, fmt.Errorf("download file to rewrite: %w", err)
		}
	}
	limit := memoryBytes / 2
	if limit < minRewriteMemoryBytes {
		limit = minRewriteMemoryBytes
	}
	rdb, err := openRewriteDB(ctx, database.RewriteDBOptions{
		MemoryLimitBytes: limit,
		SpillDir:         spill,
		AllowedDirs:      []string{filepath.Dir(local), filepath.Dir(dst), spill},
	}, h.logger)
	if err != nil {
		return true, err
	}
	defer rdb.Close()
	rows := shape.singleThreadRowGroupRows(limit)
	h.logger.Info().Str("file", filepath.Base(src)).Int("columns", shape.leafColumns).Int("row_groups", shape.rowGroups).
		Int64("row_group_rows", rows).Msg("Rewriting a wide file on a single thread")
	if _, err := rdb.ExecContext(ctx, survivorsCopy(local, whereClause, dst, rows, shape.kv)); err != nil {
		return true, err
	}
	return true, nil
}

// removeRewriteScratch removes private-rewrite scratch directories a stopped
// process left in dir. Called when the handler is built, before any rewrite.
func removeRewriteScratch(dir string, logger zerolog.Logger) {
	if dir == "" {
		return
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), rewriteScratchPrefix) {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			logger.Warn().Err(err).Str("dir", e.Name()).Msg("Could not remove a DELETE rewrite scratch directory left by a stopped process")
		}
	}
}
