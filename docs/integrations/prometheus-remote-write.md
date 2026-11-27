# Prometheus Remote Write

Arc accepts Prometheus Remote Write 1.0 samples at
`POST /api/v1/write/prometheus`. The database is selected with `?db=...` and
defaults to `default`. When authentication is enabled, use a token with write
permission for the target database.

Arc stores each metric as its own measurement (table), every other label as a
tag column, and each sample as a float64 `value`. This is the layout
[arc-opentelemetry-exporter](https://github.com/basekick-labs/arc-opentelemetry-exporter)
writes for OTLP metrics, so both paths can be queried the same way. They
share a table only when the names match: the exporter keeps OTLP names such as
`system.cpu.time` (stored as `system_cpu_time`), while Prometheus-side names
usually carry unit suffixes (`node_cpu_seconds_total`). Remote Write timestamps are milliseconds; Arc stores them at
microsecond resolution. The receiver accepts the v1 `prometheus.WriteRequest`
protobuf format and Snappy block compression; a Remote Write 2.0 request
(`proto=io.prometheus.write.v2.Request`) is answered 415. Metric metadata is
ignored.

### Limits

All are answered 413, which Prometheus does not retry: the batch is dropped
and logged.

- The compressed body, the decoded protobuf, and the label names and values
  repeated across a request's samples (every sample row repeats its series'
  labels, in memory and in the WAL) are each capped at
  `server.max_payload_size`, at most 100MB. With the default (1GB) the cap is
  100MB; a node set to `10MB` refuses a small body that decodes past 10MB.
- A request is also limited to 100,000 time series, 1 million samples,
  1 million labels and 4 million materialised column cells, whatever
  `server.max_payload_size` says.
- With the WAL on and `wal.max_directory_mb` set, a request whose rows need
  more WAL than that directory can ever hold is refused. A request that only
  does not fit now is answered 503 and Prometheus resends it until it fits,
  which needs the WAL to be flushed and purged; a request close to the cap
  can wait a while. With the default `wal.max_size_mb` the cap is at least
  400MB (four WAL files), above what a request can need.

### Names

- The measurement is the metric name with every character other than an ASCII
  letter, digit or `_` replaced by `_`, as arc-opentelemetry-exporter does: a
  recording rule's `job:http_requests:rate5m` is stored as
  `job_http_requests_rate5m`. Two metric names that differ only there share a
  table.
- Label names are kept as sent, including Prometheus 3 UTF-8 names such as
  `service.name`. An empty label value means the label is absent.
- A series Arc cannot store is dropped and the rest of the request is
  written: a metric name that does not start with a letter or is longer than
  128 characters, a label named `value` or `time` (in any letter case), a
  label whose name starts with `_` (Arc does not store such columns), or a
  series without `__name__`.
- Of label names that differ only in letter case in a metric (`Host` and
  `host`), one spelling is kept and the series using the other are dropped:
  the spelling the table already stores, otherwise the byte-smallest one in
  the request (`Host` before `host`), whatever order the series arrive in.
  The check covers one request against what is already flushed: two
  requests that each carry a different spelling before a flush can both be
  stored, so a sender should use one spelling. If the table has a column
  `Value` (from line protocol, for example), the metric's samples are
  dropped, since they are stored in `value`.
- When something is dropped the answer is a 400
  `partial write` naming what was dropped, as for line protocol; Prometheus
  logs it and does not resend the batch, and the other series are stored.

### Samples

- Staleness markers (the NaN Prometheus sends when a series stops) are not
  stored: a NaN in `value` would make `avg(value)` and `sum(value)` NaN. Any
  other NaN sample is stored as sent; filter it with `WHERE NOT isnan(value)`.
- Samples are stored as sent, in any order. A batch Prometheus resends after
  a 5xx or a timeout can store its samples twice. Compaction later removes
  rows identical in every column for a measurement whose tag names are all
  letters, digits, `_` or `-`; with a label such as `service.name`, or no
  label besides `__name__`, the duplicates stay.
- Native histograms and exemplars are not supported. A native histogram
  series is dropped; the samples of a series with exemplars are stored and
  the exemplars reported as dropped.

### Monitoring

`/metrics` (Prometheus format) and `/api/v1/metrics` have the receiver's own
counters; the line-protocol counters and
`/api/v1/write/line-protocol/stats` do not count remote write.

| Counter | Counts |
|---|---|
| `arc_prometheus_remote_write_requests_total` | Requests this node answered |
| `arc_prometheus_remote_write_errors_total` | Of those, answered 400 or above (a partial write included) |
| `arc_prometheus_remote_write_records_total` | Samples written |
| `arc_prometheus_remote_write_bytes_total` | Decoded (protobuf) bytes of requests that decoded |

A body larger than `server.max_payload_size` is refused (413) by the HTTP
server before it reaches the receiver, and is not counted.

In a cluster, a request a reader or a standby writer forwards is counted by
the writer that answered it. The forwarding node counts only what it answers
itself (a refused token, a writer it could not reach), so on a reader the
error ratio is close to 1 by design. A forward answered 504 (sent, no
answer) is an error on the reader, and the writer may have counted the same
request.

### Files

Every metric is a table, so each buffer flush writes one Parquet file per
metric that received samples. A Prometheus scraping node_exporter sends
several hundred metric names, which is several hundred small files per flush
until [compaction](../operations/compaction.md) merges them; raising
`ingest.max_buffer_age_ms` lowers the count.

## Prometheus

```yaml
remote_write:
  - url: http://arc:8000/api/v1/write/prometheus?db=prometheus
    protobuf_message: prometheus.WriteRequest
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/arc-write-token
    send_exemplars: false
    send_native_histograms: false
```

If Arc authentication is disabled, omit `authorization`. `protobuf_message`
exists from Prometheus 2.54; on older versions leave it out (1.0 is the only
message they send).

## Grafana Alloy

```river
prometheus.remote_write "arc" {
  endpoint {
    url = "http://arc:8000/api/v1/write/prometheus?db=prometheus"
    protobuf_message = "prometheus.WriteRequest"
    bearer_token_file = "/etc/alloy/arc-write-token"
    send_exemplars = false
    send_native_histograms = false
  }
}
```

Omit `bearer_token_file` when Arc authentication is disabled. Alloy enables
exemplar sending by default; with it on, every batch that carries an exemplar
is answered with a `partial write` 400 (its samples are stored), so turn it
off as above.

Query the resulting data with SQL, for example:

```sql
SELECT time, value, instance
FROM prometheus.cpu_usage
ORDER BY time DESC;
```

See the [Remote Write 1.0 specification](https://prometheus.io/docs/specs/prw/remote_write_spec/),
[Prometheus remote-write configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/),
and [Grafana Alloy `prometheus.remote_write`](https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.remote_write/).
