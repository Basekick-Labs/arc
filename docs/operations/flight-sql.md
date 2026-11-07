# Arrow Flight SQL

Arc can expose its read-only query API through an optional Arrow Flight SQL
gRPC listener. The listener is disabled by default and uses the existing query
validation, authorization, RBAC, governance and ArcX routing paths.

Enable it in `arc.toml`:

```toml
[flight_sql]
enabled = true
port = 50051
```

The port must differ from `server.port`. The listener uses `server.host`; when
that setting is empty, it binds to the wildcard address. Set
`server.tls_enabled = true` to enable TLS with the same certificate and key as
the HTTP API. When Arc authentication is enabled, provide an Arc API token with
read permission. SQL writes, transactions and prepared statements are not
supported.

Flight SQL metadata maps Arc databases to catalogs, the empty schema to each
catalog, and measurements to tables. Table metadata can include the Arrow
schema. A catalog-scoped metadata request uses Arc's normal database and
measurement permission checks.

For Python, install `adbc-driver-flightsql`, `adbc-driver-manager` and
`pyarrow`, then connect with a bearer token:

```python
import os

from adbc_driver_flightsql import DatabaseOptions, dbapi

connection = dbapi.connect(
    "grpc://localhost:50051",
    db_kwargs={DatabaseOptions.AUTHORIZATION_HEADER.value:
               f"Bearer {os.environ['ARC_TOKEN']}"},
)

with connection.cursor() as cursor:
    cursor.execute('SELECT * FROM "default"."cpu" LIMIT 10')
    print(cursor.fetchall())

connection.close()
```

The `grpc://` URI uses plaintext transport. For TLS, use
`flightsql://localhost:50051` and configure the client with the CA that signed
Arc's server certificate. The listener is read-only; use Arc's HTTP write APIs
or an ingestion client for writes.

## Governance row limits

If a query reaches its configured governance row cap, Flight SQL returns the
available rows followed by a gRPC `ResourceExhausted` error. Treat that result
as potentially incomplete, not as a successful full result. Flight clients
must check the final stream error (for example, `reader.Err()` in Go). This
makes the HTTP Arrow endpoint's `Arc-Rows-Capped` signal visible to ordinary
Flight SQL clients without requiring custom application metadata. Use an
explicit SQL `LIMIT` below the configured cap when a bounded result is wanted.
