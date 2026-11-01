# Single-node durability: process crash probe

This is the first measured slice of
[#1387](https://github.com/Basekick-Labs/arc/issues/1387). It records which
rows an Arc process recovered after `SIGKILL`, using the locally built server,
a local storage backend and exact row IDs. It does not measure host power loss,
virtual-machine reset, hardware failure or remote object storage.

## Reproduce

Build Arc with the same DuckDB integration used by the project:

```sh
go build -tags=duckdb_arrow -o /tmp/arc ./cmd/arc
```

The probe starts Arc on a free loopback port with authentication and telemetry
disabled, writes a batch to a fresh temporary data directory, kills the process
without graceful shutdown, restarts it, and queries the recovered IDs. The
directory is removed after each run.

For the six WAL mode combinations, run:

```sh
for sync_mode in fdatasync fsync async; do
  for ack_mode in queued durable; do
    python3 scripts/durability/wal_crash_probe.py \
      --binary /tmp/arc --wal-enabled --rows 100000 \
      --sync-mode "$sync_mode" --ack-mode "$ack_mode"
  done
done
```

To observe the WAL-off buffer window, run the same probe without
`--wal-enabled`, varying the delay after the successful response:

```sh
python3 scripts/durability/wal_crash_probe.py \
  --binary /tmp/arc --rows 10000 --pause-after-ack 0
python3 scripts/durability/wal_crash_probe.py \
  --binary /tmp/arc --rows 10000 --pause-after-ack 1
python3 scripts/durability/wal_crash_probe.py \
  --binary /tmp/arc --rows 10000 --pause-after-ack 5.5
```

The buffer age is 5 seconds and the maximum buffer size is 50,000 rows. The
output includes acknowledged and recovered row counts, missing and duplicate
IDs, acknowledgement latency, and time from restart through the verification
query.

## Result on Arc `1943fe8` (Go 1.27.1, macOS arm64)

Each row is one run of a 100,000-row request. Arc answered HTTP 204, then the process was killed immediately. All six WAL-enabled runs recovered 100,000 unique IDs with no missing or duplicate rows.

| WAL sync mode | Acknowledgement mode | Ack latency | Restart and query | Recovered |
| --- | --- | ---: | ---: | ---: |
| `fdatasync` | `queued` | 252 ms | 0.60 s | 100,000 / 100,000 |
| `fsync` | `queued` | 362 ms | 3.72 s | 100,000 / 100,000 |
| `async` | `queued` | 317 ms | 0.82 s | 100,000 / 100,000 |
| `fdatasync` | `durable` | 306 ms | 0.79 s | 100,000 / 100,000 |
| `fsync` | `durable` | 363 ms | 1.14 s | 100,000 / 100,000 |
| `async` | `durable` | 542 ms | 1.50 s | 100,000 / 100,000 |

These timings are single-run observations on this machine, not comparative
performance results. Restart time includes server startup and the SQL query.

With the WAL disabled, `queued` acknowledged 10,000 rows in 24 ms. An
immediate kill and a kill after one second both recovered zero rows; waiting
5.5 seconds recovered all 10,000. This batch stayed below the 50,000-row size
trigger. A 100,000-row request crossed that threshold, was acknowledged in
435 ms, and lost all rows when killed immediately while its size-triggered
flush was still in progress. Setting `wal.ack_mode = "durable"` without
enabling the WAL also acknowledged 10,000 rows and lost all of them after an
immediate kill; the setting has no durability effect when no WAL is active.

## What this result establishes

On this local configuration, the WAL-off 10,000-row run lost the acknowledged
buffer contents before the five-second age flush and recovered them after a
5.5-second wait. The 100,000-row run lost its contents when killed during the
size-triggered flush. With the WAL enabled, all tested configurations recovered
the request after a process-only `SIGKILL`.

The host kernel remained running during these tests. In particular, `SIGKILL`
does not discard the operating system's file cache, so the `queued` results do
not establish survival of sudden power loss. Testing the power-loss guarantee
requires a controlled VM or host reset that discards unflushed writes.
Object-store failures, backup and restore RPO/RTO, and disk pressure remain
outside this measurement.
