# freshness

Measures how long a row written to Arc takes to show up in a query.

- **`steady`** writes marker rows at a fixed rate and polls a query for them. For each row it
  reports the time from the write's acknowledgement (HTTP 204) to the first query that
  returned it.
- **`late`** reproduces one specific case ([#1757](https://github.com/Basekick-Labs/arc/issues/1757)):
  a row stamped in another hour (late or future data), written into an hour that a query has
  just found empty.

`run.sh` starts a fresh Arc for one run and stops it afterwards:

```bash
go build -tags=duckdb_arrow -o arc ./cmd/arc
scripts/freshness/run.sh r1        # defaults: 5 s buffer age
scripts/freshness/run.sh r2        # ingest.max_buffer_age_ms=100
scripts/freshness/run.sh r6        # one late row into an hour found empty
```

Against an Arc that is already running:

```bash
go run ./scripts/freshness steady -http http://127.0.0.1:8000 -token "$TOKEN" -rate 20 -duration 60s
go run ./scripts/freshness late   -http http://127.0.0.1:8000 -token "$ADMIN_TOKEN"
```

`steady` needs a token that can write and read. `late` needs an admin token, because it calls
the flush endpoint. `-h` after the mode lists every flag.

## Runs

| Run | What it sets | Question |
|---|---|---|
| `r1` | defaults, 20 rows/s | Lag with the age-triggered flush |
| `r1s` | defaults, 5,000 filler rows per request | Lag with the size-triggered flush (`ingest.max_buffer_size`) |
| `r2` | `ingest.max_buffer_age_ms=100` | What tuning the buffer age buys |
| `r6` | one row stamped 3 h back | Is a late row invisible after its file is published? |
| `r6f` | one row stamped 2 h ahead | The same for a future-stamped row |
| `r7` | 500 measurements, 4 probed | Does the age sweep keep up with many buffers |

Not scripted, same harness:

- **Compaction and file count over hours:** `COMPACTION=1 scripts/freshness/run.sh r2 -duration 3h`.
  The probe's round trip grows as the hour's uncompacted files pile up; that growth is the
  cost of a short buffer age.
- **Object storage:** export `ARC_STORAGE_BACKEND=s3` and the bucket settings before `run.sh`
  (SeaweedFS locally).
- **A cluster:** start it yourself and point `steady` at a reader. The lag then includes
  replication; the server-side metric below does not.
- **Another endpoint:** `-endpoint msgpack|arrow|measurement`. The `measurement` endpoint
  returns at most 1,000,000 rows, so keep `rate × duration` under that.
- **A range that spans hour partitions:** `-lookback 3h`. A range inside the current hour and
  one that spans hours are pruned differently.

## Reading the output

```
lag ms  (ack -> first probe that returned the row)  n=300 p50=2562.6 p95=4812.4 p99=5010.0 max=5039.1
        lower bound (ack -> last probe sent that missed it)  n=300 p50=2548.4 ...
server  files_flushed=3  publish_delay: files=3 mean=5020ms max_since_start=5036ms
```

- **lag** is an upper bound: the row became visible some time between the last probe that
  missed it and the first that returned it. The second line is the other end of that bracket.
  The gap between the two is the resolution: the `-poll` pause plus two probe round trips
  (one is printed as `probes ... round trip ms`).
- Rows arrive evenly while a buffer fills, so with the age trigger the lag is spread between
  zero and the buffer age. The **max** is the number to compare with a target.
- **server** is Arc's own `arc_ingest_publish_delay_seconds` over the same period, one
  observation per file: the wait of the oldest row in the flushed buffer. It should match the
  harness's **max** lag, not its median. `max_since_start` is the gauge's value, which is not
  reset between runs against the same server.
- **never_visible** rows did not show up within `-timeout`.
- **ticks_skipped**: the writer skips a tick when 32 writes are already in flight, so a stalled
  server shows up as skipped ticks and not as a burst afterwards.
- **probe errors** are counted and never read as lag.

`late` prints, per trial, the time from the flush returning (the file is in storage) to the
first query that returned the row, for two queries: the same SQL text as the query that found
the hour empty, and a text never sent before. The second leaves out the caches keyed by the
query text, so a large first number with a small second one points at those caches, and two
large numbers point at the listing cache.

Two kinds of trial are marked `discard` and left out of the summary, because they say nothing
about the caches: one that crossed an hour boundary (the caches are keyed by the current hour),
and one started in the first 5 minutes of an hour (Arc then treats the previous hour as still
filling and caches no plan that includes it). Compaction is off in `run.sh` for the same
reason: when a compaction finishes it clears the query caches.

`steady` exits non-zero when rows did not show up within `-timeout` or when every probe failed.

## What it does not measure

- **Ingest throughput.** One row per request at 20 requests a second says nothing about what a
  setting costs at load. Use the load benchmarks for that, and compare binaries in rotated
  order with a second copy of one of them as the noise floor.
- **Dashboard query cost** beyond the probe's own round trip.
- **Whether partition pruning engaged.** The probe uses the time-literal form the pruner reads,
  but nothing in a query response says which paths were read.
- **The SQL transform cache on and off.** Arc has no switch for it. `late` compares a repeated
  SQL text with a new one instead.
- Rows written to measurements that are not probed (`-probe-sample`) are not timed. With many
  measurements, the server-side histogram covers all of them.
