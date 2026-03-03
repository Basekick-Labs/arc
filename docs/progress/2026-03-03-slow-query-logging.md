# Slow Query Logging

## Context

Arc tracks query execution time but doesn't alert on slow queries. Adding a configurable threshold with WARN-level logging and a Prometheus counter gives operators visibility into queries that may need optimization — without any enterprise licensing.

## Changes

### 1. Config: add `SlowQueryThresholdMs` (`internal/config/config.go`)

Add field to `QueryConfig` struct (line 174):
```go
type QueryConfig struct {
    Timeout              int
    SlowQueryThresholdMs int   // Slow query WARN threshold in ms (0 = disabled)
    EnableS3Cache        bool
    S3CacheSize          int64
    S3CacheTTLSeconds    int
}
```

Add default (in `setDefaults()`): `v.SetDefault("query.slow_query_threshold_ms", 0)`

Add loading (in config loader): `SlowQueryThresholdMs: v.GetInt("query.slow_query_threshold_ms")`

Env var: `ARC_QUERY_SLOW_QUERY_THRESHOLD_MS`

### 2. Metrics: add `slowQueriesTotal` (`internal/metrics/metrics.go`)

Add atomic counter alongside existing query metrics (after line 50):
```go
slowQueriesTotal atomic.Int64
```

Add increment method (after `IncQueryTimeouts`):
```go
func (m *Metrics) IncSlowQueries() { m.slowQueriesTotal.Add(1) }
```

Add to Prometheus output (after `arc_query_timeouts_total` block):
```
# HELP arc_slow_queries_total Queries that exceeded slow query threshold
# TYPE arc_slow_queries_total counter
arc_slow_queries_total <value>
```

Add to `Snapshot` struct and method if it exists.

### 3. Query handler: add threshold + `logSlowQuery` helper (`internal/api/query.go`)

Add field to `QueryHandler` struct (after `queryTimeout`):
```go
slowQueryThreshold time.Duration // Slow query WARN threshold (0 = disabled)
```

Update `NewQueryHandler` signature to accept `slowQueryThresholdMs int`:
```go
func NewQueryHandler(db *database.DuckDB, storage storage.Backend, logger zerolog.Logger, queryTimeoutSeconds int, slowQueryThresholdMs int) *QueryHandler {
```

Set in constructor:
```go
slowQueryThreshold: time.Duration(slowQueryThresholdMs) * time.Millisecond,
```

Add helper method:
```go
func (h *QueryHandler) logSlowQuery(sql string, start time.Time, rowCount int, tokenName string) {
    if h.slowQueryThreshold <= 0 {
        return
    }
    elapsed := time.Since(start)
    if elapsed < h.slowQueryThreshold {
        return
    }
    metrics.Get().IncSlowQueries()
    h.logger.Warn().
        Str("sql", sql).
        Float64("execution_time_ms", float64(elapsed.Milliseconds())).
        Int("row_count", rowCount).
        Str("token", tokenName).
        Msg("Slow query detected")
}
```

Wire into all 4 completion paths (right after existing "Query completed" Info log):

**Path 1** (~line 1221, standard JSON streaming callback):
- Before `SetBodyStreamWriter`: capture `tokenName := ""; if ti := auth.GetTokenInfo(c); ti != nil { tokenName = ti.Name }`
- After "Query completed" log: `h.logSlowQuery(convertedSQL, start, rowCount, tokenName)`

**Path 2** (~line 1367, standard single-query callback):
- Same pattern — capture `tokenName` before callback, call `logSlowQuery` after log

**Path 3** (~line 3013, measurement query callback):
- Same pattern — capture before callback, call after log

**Path 4** (`query_arrow_json.go` ~line 137, Arrow JSON callback):
- Import `auth` package
- Before `SetBodyStreamWriter`: capture `tokenName`
- After "Arrow JSON query completed" log: `h.logSlowQuery(convertedSQL, start, rc, tokenName)`

### 4. Wire config in `cmd/arc/main.go`

Update line 921:
```go
queryHandler := api.NewQueryHandler(db, storageBackend, logger.Get("query"), cfg.Query.Timeout, cfg.Query.SlowQueryThresholdMs)
```

### 5. Config file example (`arc.toml`)

Add under `[query]` section:
```toml
# slow_query_threshold_ms = 1000  # WARN log + metric for queries exceeding this (0 = disabled)
```

## Files Summary

| File | Action |
|------|--------|
| `internal/config/config.go` | Add `SlowQueryThresholdMs` field, default, loading |
| `internal/metrics/metrics.go` | Add `slowQueriesTotal` counter, `IncSlowQueries()`, Prometheus output |
| `internal/api/query.go` | Add `slowQueryThreshold` field, update constructor, add `logSlowQuery` helper, wire into 3 completion paths |
| `internal/api/query_arrow_json.go` | Import `auth`, capture token, wire `logSlowQuery` into Arrow completion path |
| `cmd/arc/main.go` | Pass `cfg.Query.SlowQueryThresholdMs` to `NewQueryHandler` |
| `arc.toml` | Add commented config example |

## Verification

1. `go build -tags=duckdb_arrow ./cmd/arc/` — compiles
2. `go test -tags=duckdb_arrow ./internal/api/... -v` — existing tests pass
3. Manual test: set `ARC_QUERY_SLOW_QUERY_THRESHOLD_MS=1`, run a query, verify WARN log with sql, execution_time_ms, row_count, token fields
4. Check `/metrics` endpoint includes `arc_slow_queries_total`
5. Set threshold to `0`, verify no WARN logs emitted
