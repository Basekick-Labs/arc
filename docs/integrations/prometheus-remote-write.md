# Prometheus Remote Write

Arc accepts Prometheus Remote Write 1.0 samples at
`POST /api/v1/write/prometheus`. The database is selected with `?db=...` and
defaults to `default`. When authentication is enabled, use a token with write
permission for the target database.

Arc stores `__name__` as the measurement, every other label as a tag, and each
sample as a float64 `value`. Remote Write timestamps are milliseconds; Arc
stores them at microsecond resolution. The receiver accepts the v1
`prometheus.WriteRequest` protobuf format and Snappy block compression. Both
the compressed body and decoded protobuf are capped at 100 MiB. Metric
metadata is ignored.
To keep sparse label sets from expanding into an unbounded in-memory column
matrix, a request is also limited to 100,000 time series, 1 million samples,
1 million labels and 4 million materialised column cells.

Arc's measurement and column rules are stricter than Prometheus': measurement
names must start with a letter and contain only letters, digits, `_` or `-`
(up to 128 characters). Labels named `time` or `value` are rejected, as are
label names that conflict with existing column names. Native histograms and
exemplars are not supported and cause the request to be rejected.

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

If Arc authentication is disabled, omit `authorization`.

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
exemplar sending by default, so the explicit setting above is required for
this receiver.

Query the resulting data with SQL, for example:

```sql
SELECT time, value, instance
FROM prometheus.cpu_usage
ORDER BY time DESC;
```

See the [Remote Write 1.0 specification](https://prometheus.io/docs/specs/prw/remote_write_spec/),
[Prometheus remote-write configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/),
and [Grafana Alloy `prometheus.remote_write`](https://grafana.com/docs/alloy/latest/reference/components/prometheus/prometheus.remote_write/).
