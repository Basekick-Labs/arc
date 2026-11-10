# Single-node durability: process crash probe

This is the first measured slice of
[#1387](https://github.com/Basekick-Labs/arc/issues/1387). It measures one quiet
batch followed by a process-only `SIGKILL`, using local storage and exact row
IDs. It does not cover sustained ingest, power loss, VM reset, hardware failure,
remote object storage, disk pressure, or backup and restore RPO/RTO.

## Reproduce

Build the server from the source revision being measured:

```sh
go build -tags=duckdb_arrow -o /tmp/arc ./cmd/arc
```

The probe starts Arc on a free loopback port with authentication and telemetry
disabled. Each run uses a fresh temporary data directory, writes one batch,
kills Arc after HTTP 204, restarts with that same directory, and queries the
recovered IDs. Processes and directories are cleaned up before the next run.

`--ack-mode` defaults to `auto`, Arc's own default since
[#1499](https://github.com/Basekick-Labs/arc/issues/1499): `durable` with the
WAL on, `queued` with it off. `queued` with the WAL on is now an explicit
choice.

```sh
for sync_mode in fdatasync fsync async; do
  for ack_mode in queued durable; do
    python3 scripts/durability/wal_crash_probe.py \
      --binary /tmp/arc --wal-enabled --rows 100000 --runs 3 \
      --sync-mode "$sync_mode" --ack-mode "$ack_mode"
  done
done
```

`--runs` defaults to 1 and must be positive. Output is a JSON object with a
`runs` array containing each measurement, `runs_completed`, `runs_with_loss`,
`lost_rows_per_run`, and `runs_with_duplicates`. Per-run results retain missing
and duplicate counts, acknowledgement latency, and restart/query time. A
non-204 write, startup failure, or query failure raises an error and exits
unsuccessfully instead of being counted as acknowledged data loss.

For WAL-off runs, omit `--wal-enabled`:

```sh
for pause in 0 1 5.5; do
  python3 scripts/durability/wal_crash_probe.py \
    --binary /tmp/arc --rows 10000 --runs 3 --pause-after-ack "$pause"
done
python3 scripts/durability/wal_crash_probe.py \
  --binary /tmp/arc --rows 100000 --runs 3
python3 scripts/durability/wal_crash_probe.py \
  --binary /tmp/arc --rows 10000 --runs 3 --ack-mode durable
```

The buffer age is 5 seconds and the maximum buffer size is 50,000 rows.
Run deterministic probe tests separately:

```sh
python3 -m unittest discover -s scripts/durability -v
```

## Results at `07bb6bf` (2026-11-04)

These runs predate [#1499](https://github.com/Basekick-Labs/arc/issues/1499)
(`durable` acknowledgements by default with the WAL on),
[#1492](https://github.com/Basekick-Labs/arc/issues/1492) (a crash during a
multi-hour flush no longer replays stored hours) and
[#1501](https://github.com/Basekick-Labs/arc/issues/1501) (the durable wait is
bounded by `wal.ack_timeout_seconds`). The acknowledgement semantics they
measure are unchanged.

Measured on Linux 6.18.57 x86_64 with Go 1.26.9, using source commit
`07bb6bf525396cac6a47b2092941a8d71548ead2`. Builds used
`-buildvcs=false -tags=duckdb_arrow` because VCS stamping was unavailable in
the linked worktrees. Each configuration below had three independent runs of
100,000 rows, killed immediately after HTTP 204.

**Queued acknowledgements lost all rows in every run; durable acknowledgements
recovered every row in every run.** These are observations, not durability
guarantees or estimates of a real-world loss probability.

| WAL sync mode | Ack mode | Runs losing rows | Lost rows in each run | Ack latency range |
| --- | --- | ---: | --- | ---: |
| `fdatasync` | `queued` | 3 / 3 | 100,000; 100,000; 100,000 | 118–158 ms |
| `fsync` | `queued` | 3 / 3 | 100,000; 100,000; 100,000 | 140–171 ms |
| `async` | `queued` | 3 / 3 | 100,000; 100,000; 100,000 | 139–162 ms |
| `fdatasync` | `durable` | 0 / 3 | 0; 0; 0 | 135–236 ms |
| `fsync` | `durable` | 0 / 3 | 0; 0; 0 | 128–145 ms |
| `async` | `durable` | 0 / 3 | 0; 0; 0 | 125–164 ms |

No duplicate IDs were observed. Whether a `queued` batch is lost depends on
timing: on macOS (arm64, APFS) at `f1a56dd3` (2026-11-06), two runs each of
100,000 rows with the WAL on lost nothing under `queued` either, nor under
`durable`. A `queued` acknowledgement promises nothing about a crash in either
direction; only `durable` does. The sync modes are distinct on this Linux host;
on macOS `fdatasync` falls back to `fsync`, with a startup log saying so.
Timings are rounded observations on a shared host, not comparative performance
results. Restart/query time includes startup and the verification query.

### Baseline comparison

Repeating all six configurations three times against the original PR head
`7b1ceba1f3504451ce3e301119a98b6372b88645` (server source identical to
`1943fe87e0463bf9b5375dcbab40cf1171c1e731`) on the same Linux host gave the
same loss counts: each queued configuration lost 100,000 rows in all three
runs, and each durable configuration lost zero rows in all three runs. No
duplicates were observed. The earlier single-run macOS results therefore do
not establish that queued writes survive a process crash, nor does this
comparison establish that loss was introduced by a newer main commit.

### WAL disabled

These measurements also used current main, with three runs per case:

| Rows | Ack mode | Delay after 204 | Runs losing rows | Lost rows in each run |
| ---: | --- | ---: | ---: | --- |
| 10,000 | `queued` | 0 s | 3 / 3 | 10,000; 10,000; 10,000 |
| 10,000 | `queued` | 1 s | 3 / 3 | 10,000; 10,000; 10,000 |
| 10,000 | `queued` | 5.5 s | 0 / 3 | 0; 0; 0 |
| 100,000 | `queued` | 0 s | 3 / 3 | 100,000; 100,000; 100,000 |
| 10,000 | `durable` | 0 s | 3 / 3 | 10,000; 10,000; 10,000 |

No duplicate IDs were observed. The 10,000-row batch is below the size trigger;
the 100,000-row batch crosses it, but acknowledgement does not await that
flush. `durable` without an enabled WAL has no durability effect. Arc warns:
`wal.ack_mode = "durable" has no effect while wal.enabled = false: writes are acknowledged without a WAL`.

## What this result establishes

The results are consistent with the WAL acknowledgement contract:

- `queued` returns after admission to the writer queue, before completion of
  the write or sync. An acknowledged batch can be lost at process crash,
  including an entire batch represented by an incomplete WAL entry.
- `durable` waits for a sync covering the entry, including in `async` sync
  mode. In these runs, all acknowledged rows survived process termination.

A finite number of successful runs does not prove a guarantee. The host
kernel remains running during `SIGKILL`; its file cache is not discarded.
Power-loss testing requires a controlled VM or host reset that discards
unflushed writes. Crashing during sustained ingest is a separate, still
unmeasured part of #1387.
