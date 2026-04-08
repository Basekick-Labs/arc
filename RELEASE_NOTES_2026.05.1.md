# Arc v2026.05.1 Release Notes

## Bug Fixes

### Memory Not Released After Delete / Retention Execution

Fixed a memory retention issue where running a retention policy or the delete API endpoint caused Arc to hold onto several GBs of memory that were never released — requiring periodic container restarts to recover.

**Root cause:** DuckDB's internal parquet metadata cache and data block cache were populated during `read_parquet()` queries executed by delete and retention operations, but never cleared after completion. Compaction already performed this cleanup; delete and retention did not.

**Changes:**

- Both the delete handler and retention handler now call `ClearHTTPCache()` after completing their file operations. This evicts DuckDB's glob result cache, file metadata cache, and data block cache for the files that were rewritten or deleted — preventing stale references from accumulating.
- `debug.FreeOSMemory()` is called after the cache flush, returning freed heap pages to the OS immediately (the same mechanism that causes memory to drop after compaction runs).
- The delete file-rewrite COPY queries now include `ROW_GROUP_SIZE 122880`, matching the compaction writer. This limits DuckDB's internal write buffer per row group, reducing peak memory during large rewrites.

**Impact:** Memory usage should return to baseline after a retention policy run or delete operation, instead of climbing GBs per execution. Particularly noticeable in Docker/Kubernetes deployments with memory limits, where this could cause OOM kills during nightly retention runs.
