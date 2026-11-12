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
measurement permission checks; a measurement the token cannot read is left
out of a table listing that includes schemas. Any other failure of a schema
query fails the listing.

Metadata calls run ordinary queries: listing tables runs one per database,
and including schemas adds one per measurement. Each counts against the
token's governance rate limits and quotas like any other query, so a client
that browses a large catalogue can be throttled, and the listing then fails
with `ResourceExhausted` rather than returning part of it.

## What is supported

Statement queries (`SELECT`, `SHOW DATABASES`, `SHOW TABLES`) and the catalog,
schema, table and table-type metadata calls. Prepared statements, SqlInfo,
transactions and writes are not implemented. Clients that require prepared
statements, which includes the Arrow Flight SQL JDBC driver and the BI tools
built on it, are not expected to work yet; the ADBC drivers are the tested
clients.

## Logging, audit and shutdown

Flight queries go through the same request logging and, when audit logging is
on, the same audit records as HTTP queries, with the Flight client's address
and User-Agent. In the audit log they appear under the HTTP query paths they
run on (`/api/v1/query`, `/api/v1/query/arrow`). A connection refused by
Flight authentication is not audited, as a refused HTTP request is not. The
HTTP request metrics also count Flight queries.

On shutdown, in-flight Flight queries get 5 seconds to finish and are then
cancelled, the queries included; connections still open 2 seconds later are
closed, so a client that stops reading a result cannot hold shutdown up. With
the internal drain after it, the listener takes at most about 11 seconds to
stop, and less when `server.shutdown_timeout` leaves less: every stage is
scaled to fit in half of the shutdown time remaining when it starts. The
listener sets no limit on concurrent streams per connection; every call needs
a valid token when authentication is on.

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

## Kubernetes (Helm)

Both charts turn the listener on with the same values:

```yaml
flightSql:
  enabled: true        # default false: no port in the container or on the Service
  port: 50051          # container port (flight_sql.port)
  servicePort: 50051   # port on the Service
```

The OSS chart adds a `flight-sql` port to its Deployment and Service. The
enterprise chart adds it to the writers and readers and to their Services;
compactors have no client Service and are not given the listener. Both mark
the Service port `appProtocol: grpc`. A port that collides with one of Arc's
(8000, and in the enterprise chart 9100 and 9200) is refused when the chart
renders, as is an `ARC_FLIGHT_SQL_*` variable in `arc.env` or a role's
`extraEnv`: it would move the listener away from the container and Service
ports. Enabling the listener in a custom `arc.toml` instead of `flightSql`
opens it without a container or Service port. The charts' Ingress routes HTTP
only: expose the port through the Service (for example a LoadBalancer) or an
ingress controller that proxies gRPC.

## Governance row limits

If a query reaches its configured governance row cap, Flight SQL returns the
available rows followed by a gRPC `ResourceExhausted` error. Treat that result
as potentially incomplete, not as a successful full result. Flight clients
must check the final stream error (for example, `reader.Err()` in Go). This
makes the HTTP Arrow endpoint's `Arc-Rows-Capped` signal visible to ordinary
Flight SQL clients without requiring custom application metadata. Use an
explicit SQL `LIMIT` below the configured cap when a bounded result is wanted.
