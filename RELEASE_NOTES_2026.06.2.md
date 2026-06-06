# Arc v2026.06.2 Release Notes

> **Status:** In progress (planned release: July 2026).

> **This is a patch release.** Bug fixes and hardening only — no new features, no breaking API changes, no schema migrations. Drop-in upgrade from v2026.06.1.

## Security hardening

This release strengthens internal verification routines on the Arc Enterprise activation path and tightens authorization on database-management API endpoints. The changes are internal to Arc; existing license keys, activation flows, tokens, and `arc.toml` configurations continue to work unchanged.

`POST /api/v1/databases` and `DELETE /api/v1/databases/:name` now consistently require admin-permission tokens (matching the established convention on every other mutating endpoint in Arc). Read-only routes — listing databases, listing measurements, fetching database metadata — remain accessible to any authenticated token.

**`POST /api/v1/query/estimate` and `GET /api/v1/measurements` now enforce RBAC read-permission checks.** These two endpoints were missing the table-level RBAC authorization that the main `POST /api/v1/query` endpoint already performs. `estimateQuery` now checks read permission for every table referenced in the submitted SQL, resolves bare table references against the `x-arc-database` header (preventing a header-based RBAC bypass), and rejects cross-database syntax (`db.table`) when the header is set — matching the validation in `executeQuery`. The permission check normalises the SQL — masking string literals, masking `FROM` keywords inside function bodies (e.g. `EXTRACT(YEAR FROM time)`), and stripping comments — before extracting table references, matching the normalisation the query-transform path already applies. This closes a bypass where a comment interleaved between a `FROM`/`JOIN` keyword and the table name (`FROM /* x */ secret.cpu`) could hide a reference from the check while still resolving to that table at execution. It also avoids false-positive denials: a field name following an in-function `FROM` (`EXTRACT(EPOCH FROM ts)`), a CTE name (`WITH t AS (…) … FROM t`), or a table-valued function (`FROM generate_series(…)`) is no longer mistaken for a real table — the extractor now skips CTE names and function calls, matching the query-transform path. The cross-database-syntax guard applies the same normalisation (including in-function `FROM` masking), so legitimate queries such as `EXTRACT(EPOCH FROM t.ts) FROM cpu t` are no longer rejected when the `x-arc-database` header is set. **`SHOW DATABASES` and `SHOW TABLES`/`SHOW MEASUREMENTS` commands are now separately gated on the estimate endpoint:** these commands contain no `FROM`/`JOIN` table references, so the general `checkQueryPermissions` extraction path would pass them through unchecked. The estimate endpoint now mirrors `executeQuery`'s permission checks — `SHOW DATABASES` requires read on `*.*`, `SHOW TABLES`/`SHOW MEASUREMENTS` requires read on the target database — and returns a `400` directing callers to use `/api/v1/query` instead, since the estimate endpoint has no legitimate use for metadata commands. The SHOW gate on every query endpoint (JSON, MsgPack, Arrow IPC, and estimate) now matches against comment-stripped SQL so a comment cannot hide a `SHOW` from the anchored pattern, recognises quoted database names (`SHOW TABLES FROM "my-db"`), resolves an unqualified `SHOW TABLES` against the `x-arc-database` header, and validates the resolved database name with the standard identifier check — rejecting path-traversal tokens such as `SHOW TABLES FROM ..` before any storage access (relevant when RBAC is disabled). Multi-statement queries are now rejected at validation across all query endpoints: a second statement smuggled behind a semicolon (`SHOW DATABASES; SELECT 1`) previously slipped past the anchored SHOW patterns; a single trailing `;` is still accepted, and semicolons inside string literals or comments do not trip the check. `listMeasurements` requires read permission scoped to the requested database filter (falling back to `*.*:read` when no filter is specified) — matching the existing SHOW DATABASES / SHOW MEASUREMENTS authorization pattern in the query endpoint — and validates the `database` query parameter before use. The RBAC check is skipped when RBAC is not configured (OSS deployments) or when the token has no RBAC memberships (token-level permissions apply). Overhead is negligible: 2 ns disabled / 52 ns allowed / 186 ns denied for a single permission check, 6–30 µs for SQL table-reference extraction (same cost already paid by `executeQuery`).

Operators on 26.06.1 should plan to upgrade.

## Bug fixes

**CSV and Parquet bulk imports now work against the shipped DuckDB version.** `POST /api/v1/import/csv` and `/api/v1/import/parquet` previously introspected the uploaded file by running DuckDB queries against it (`read_csv` / `read_parquet` + `DESCRIBE`); the `DESCRIBE`-as-subquery form was rejected by the DuckDB version Arc ships, so CSV imports failed with an HTTP 422 parser error before any data landed. Both formats now parse rows in-process and ingest through the same streaming pipeline as Line Protocol and TLE imports — no DuckDB queries against the uploaded file.

User-visible behavior changes:

- A 0-byte or header-only upload now returns `400 "file is empty"` / `"file contains no rows"` (previously a confusing parser error).
- Imports validate up front and reject, with a clear `400`: empty, blank, or duplicate column names; a `time_column` rename that would collide with an existing `time` column; or (for Parquet) a `NaN`/`Inf` value in a floating-point time column — cases that could previously have silently dropped a column, crashed the import, or written corrupt partition boundaries.
- Time columns now accept **fractional (floating-point) epochs** (e.g. `1609459200.123`), preserving sub-second precision; the old DuckDB path tolerated these and the in-process rewrite restores parity. Integer epochs keep full precision (no float round-trip). For Parquet, the time column may be a `TIMESTAMP`, an integer or floating-point epoch, or a timestamp string.
- CSV uploads are now subject to the same 500 MB size cap already enforced for the other import formats.
- Parquet `DECIMAL` columns are imported as `DOUBLE` (Arc's ingest path does not carry per-column decimal precision for imports). Use Line Protocol with a configured decimal column if exact decimal precision is required.

Performance: CSV imports now parse each numeric value once (the type-inference and conversion passes were merged), roughly halving CPU on numeric-heavy files; per-column buffers are pre-sized from the upload size to avoid repeated reallocation on large files. No behavior change.

Line Protocol and TLE imports are unchanged. CSV/Parquet imports no longer depend on the DuckDB sandbox's allowed-directories list.

**Line Protocol parser: eliminated per-value `strings.ToLower` allocation.** The field-value parser (`parseFieldValue`) was calling `strings.ToLower` on every ingested field value — even for floats and integers where the lowercase result was immediately discarded. Boolean detection now uses a zero-allocation byte comparison, saving one allocation per field value on the ingest hot path. Real-world throughput: ~5.3M rec/s (up from 4.6M), p50 latency 1.81ms (down from 2.14ms).

**Compaction metric fields are no longer exported.** `Manager` (`TotalJobsCompleted`, `TotalJobsFailed`, `TotalFilesCompacted`, `TotalBytesSaved`, `TotalManifestsRecov`) and `BaseTier` (`TotalCompactions`, `TotalFilesCompacted`, `TotalBytesSaved`) previously had exported struct fields that could be read or written without going through the mutex-protected `Stats()` / `GetBaseStats()` accessors, creating a potential data race. The fields are now unexported; the `Stats()` and `GetBaseStats()` methods remain the sole access path and continue to hold the mutex. `BaseTier` also gains a thread-safe `RecordCompaction(filesCompacted int, bytesSaved int64)` helper for tier implementations to increment metrics under the lock.

**MQTT subscription error handling now uses sentinel errors.** The MQTT API handler previously compared error strings for flow control (`err.Error() == "subscription already running"`, etc.) — fragile against message changes and re-wrapping. Four sentinel errors are now defined in the `mqtt` package (`ErrSubscriptionAlreadyRunning`, `ErrSubscriptionNotRunning`, `ErrSubscriptionRunningCantUpdate`, `ErrSubscriptionUniqueConstraint`), the manager returns them directly, and the API handler uses `errors.Is`. The `isUniqueConstraintError` helper in the repository was also updated to recognize the new sentinel alongside the existing SQLite string check. No user-visible behavior change.

**S3 and Azure listing loops now check context cancellation.** `List`, `ListDirectories`, and `ListObjects` on both `S3Backend` and `AzureBlobBackend` previously did not check `ctx.Err()` between paginated API calls. On very large prefixes (many pages), a cancelled or timed-out context would not propagate until the next SDK call, leaving the loop running longer than necessary. Every pagination loop now checks context cancellation at the top of each iteration, returning immediately when the context is done.

**Tiered storage migration history now has periodic cleanup.** The `tier_migrations` metadata table previously grew without bound — every migration attempt (successful or failed) was recorded and never cleaned up. Each migration cycle now deletes records older than `[tiered_storage].migration_history_retention_days` (default: 90 days). OSS deployments are unaffected (tiering requires an enterprise license).

**WAL reader now uses `io.ReadFull` for fixed-size header reads.** The WAL reader previously used `f.Read` to read fixed-size file and entry headers. `f.Read` may return fewer bytes than the buffer size without an error, which could cause partial header reads that cascade into misaligned subsequent reads — corrupting all entries after the partial read. Both header reads now use `io.ReadFull`, which guarantees the buffer is filled completely or returns an error.

**WAL writer now tracks write failures and attempts rotation.** Previously, `w.currentFile.Write()` errors were logged but not tracked (no counter for operators to monitor), and the same bad file handle kept being used for subsequent writes, causing cascading failures. Write errors now increment a `FailedWrites` counter (exposed in `Stats()` and as `arc_wal_failed_writes_total` in Prometheus), and a rotation to a new WAL file is attempted immediately. If the rotation succeeds, the entry is retried on the new file. Sync errors from `w.currentFile.Sync()` were also silently ignored and are now logged.

## Performance improvements

**Compaction cleanup now uses batch-delete APIs on S3 and Azure.** `deleteOldFiles` previously called `StorageBackend.Delete` once per compacted source file — on large compaction cycles (hundreds of files), this produced hundreds of sequential S3/Azure API calls. Both cloud backends already implemented `DeleteBatch` (S3 `DeleteObjects`, Azure `BlobBatch`) but the compaction cleanup path never used it. The path now prefers `BatchDeleter.DeleteBatch`, falling back to per-file `Delete` if the backend does not support batching. This reduces S3 DELETE API calls by up to 1000× and Azure calls by up to 256× on large compactions, with a proportional drop in cleanup-phase latency. Local-storage deployments are unaffected (a per-file loop is the correct implementation for a local filesystem).

Additionally, the Azure `DeleteBatch` implementation was rewritten to use the actual Azure SDK `BlobBatch` API (`container.Client.NewBatchBuilder()` + `SubmitBatch`) instead of a per-file loop, bringing it into parity with the S3 implementation.

**Paginated FSM manifest walks replace full-snapshot copies.** `ClusterFSM.GetAllFiles()` previously allocated a full O(N) copy of the file manifest — for 1M+ files this caused ~50ms apply-path spikes (RLock held while copying) and ~50MB transient memory. The replication catch-up walker and the `/api/v1/cluster/files` endpoint now use cursor-based pagination via `GetFilesPaginated(cursor, limit)`, which:

- Maintains a lazily-built sorted-key cache (invalidated on manifest mutation)
- Returns pages of entries, releasing the RLock between pages so Raft apply-path latency is unaffected by long-running walks
- Supports `?cursor=<path>&limit=<n>` query parameters on the API endpoint (backward-compatible — omitting both params returns all files as before)

The emergency kill-switch `replication_catchup_enabled=false` remains available.

## Upgrade notes

1. **No configuration change required.** Drop in the new binary; existing `arc.toml` and license keys work as-is.
2. **Active licenses keep working.** Arc binaries running against `enterprise.basekick.net` continue to operate normally; no re-activation or license-key reissuance is required.
3. **OSS-only deployments** (no `[license]` block in `arc.toml`) are unaffected by the license-verification change. The database-API authorization change applies only when authentication is enabled in `arc.toml`.
4. **Token review for operators using non-admin tokens for database management**: if any automation provisions databases via `POST /api/v1/databases`, ensure the token it uses has the `admin` permission. Auto-create-on-write (databases that come into existence as a side-effect of line-protocol or msgpack writes) is unaffected — write tokens continue to work for ingestion.
5. **CSV/Parquet import callers**: imports that relied on a Parquet `DECIMAL` column round-tripping as a decimal will now receive a `DOUBLE`. Malformed files now fail fast with a `400` instead of partially succeeding (empty file, empty/blank/duplicate column names, a `time_column` rename that collides with an existing `time` column, or a `NaN`/`Inf` float time value) — review any automation that ignored import error responses.

## Dependencies

No dependency changes from 26.06.1.

---

_Maintainer notes: keep this file at the repo root (per [memory/project_release_strategy.md](memory/project_release_strategy.md)); do not write to `docs/RELEASE_NOTES_*` (that path is stale)._
