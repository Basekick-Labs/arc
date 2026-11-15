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
(`proto=io.prometheus.write.v2.Request`) is answered 415. Both the compressed body and
decoded protobuf are capped at 100 MiB. Metric metadata is ignored. To keep
sparse label sets from expanding into an unbounded in-memory column matrix, a
request is also limited to 100,000 time series, 1 million samples, 1 million
labels, 4 million materialised column cells and 100 MiB of label values
repeated across its samples (413 above any of them).

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
  series without `__name__`. A label that differs only in letter case from
  another column of the measurement drops that measurement's samples in the
  request. The answer is then a 400
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
