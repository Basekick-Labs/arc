# Range-independent field binding acceptance for 26.09.2

Issue #914: a field that no Parquet file in the selected time range carried
failed to bind (`Binder Error: Referenced column "x" not found`) while the
same projection over a wider range returned NULLs. The fix (PR #929, with the
follow-ups #931 and #932) registers a per-measurement schema anchor that every
`read_parquet` lists first.

## Reproduce the native acceptance run

```sh
go build -tags=duckdb_arrow -o /tmp/arc-range-schema ./cmd/arc
python3 scripts/range_schema_acceptance.py \
  --arc /tmp/arc-range-schema --output /tmp/arc-range-schema-acceptance
```

The output directory must not already exist. The script starts Arc bound to
loopback, uses isolated storage and configuration, and retains process logs
and a UTC-stamped `results.json`. It needs Python's standard library only.

The fixtures are a port of the reproducer that came with the field report
against 26.09.1 (Docker container plus `arcli import lp`, one synchronous
import per file). The port uses the Line Protocol write endpoint with one
flush per file, the HTTP query, schema and compaction APIs, and the real
compaction subprocess. The daily tier is triggered manually; the hourly tier
is disabled so the daily candidates are the only ones. The 2026-01-01 and
2026-03-01 partitions are older than the daily tier's seven-day flush-age
window, so no fixture renaming is needed.

The script checks, in order:

- **A late field inside one day.** Twelve files on 2026-01-01: one in hour 00
  with `stable` only, eleven in hour 01 with `late_only`. Before compaction,
  the hour 00 range binds `late_only` as a BIGINT NULL in both `SELECT *`
  and the explicit projection (`typeof` is checked), hour 01 returns 42, an empty range returns zero rows
  with the same columns (the pre-existing whole-measurement fallback, kept
  because dashboards issue it), and `never_written` is a Binder Error. The schema
  endpoint reports `late_only` as BIGINT. Arc is then restarted with
  compaction enabled (the report enabled it by restarting over the same
  data), the twelve files compact into one, and every observation is
  identical to the one before compaction.
- **A field first written 59 days after the earlier day was compacted.**
  January is written and compacted while `weeks_later` exists nowhere, and
  its projection is a Binder Error. March introduces `weeks_later = 99` and
  is compacted as a separate candidate without rewriting the January output
  (same file, same size). January-only then binds `weeks_later` as a BIGINT NULL
  (`SELECT *`, the projection, `count(weeks_later) = 0` over 12 rows), March
  returns 99, and the January-to-March span groups to 0 of 12 and 12 of 12
  with `COALESCE` and `try_cast` behaving accordingly. `SELECT *` has the
  same columns over the span and over January alone.
- **A field that stops being written.** `retired_field = 77` in January,
  absent in March, both days compacted. January reads 77 in all 12 rows,
  March-only binds it as NULL, the span groups to 12 of 12 and 0 of 12.
- **Restarts.** With `query.stable_schema = false` the two narrow-range
  projections above are Binder Errors again and January-only `SELECT *`
  loses `weeks_later`, which is the 26.09.1 behavior
  and proves the assertions observe the feature. The single-day case is not
  part of that control: daily compaction merged that day into one file whose
  union schema carries `late_only`, so 26.09.1 answers it after compaction
  as well (step 05 of the original report). Restarting with the default
  setting reads the stored anchors back and every projection returns NULL.
- Four planned process starts and zero unexpected exits.

Against the 26.09.1 release binary the script fails at the first assertion
(`late_only` absent from the early-range `SELECT *`).

This is native development validation. It does not exercise object-store
backends, multi-node anchor propagation, the bootstrap of measurements that
predate 26.09.2, or the experimental empty-range anchor scan (#928); those
have unit and integration coverage under `internal/fieldschema` and
`internal/api`.

## Test commands

```sh
go test -tags=duckdb_arrow -race ./internal/fieldschema ./internal/api -run 'FieldSchema|Issue914|Issue928'
python3 scripts/range_schema_acceptance.py --arc /tmp/arc-range-schema --output /tmp/arc-range-schema-acceptance
```
