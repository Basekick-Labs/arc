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

## Authentication

A client can present the token in two ways:

- **As a bearer token**: an `authorization: Bearer <token>` header on every
  call. A `Handshake` that carries the header is checked the same way and
  answers with it, so clients that open the connection with one also work.
  This is ADBC's authorization header option and the JDBC driver's `token`
  property.
- **In a Basic handshake**, with the token as the password; the user name is
  not checked (with an empty password, the user name is taken as the token).
  The handshake answers with an `authorization: Bearer <token>`
  header for the client to send on the calls after it. This is ADBC's user
  name and password options and the JDBC driver's `user` and `password`
  properties.

With the Arrow Flight SQL JDBC driver, for example:

```text
jdbc:arrow-flight-sql://localhost:50051?useEncryption=false&token=<token>
```

The driver turns TLS on unless `useEncryption=false` is set, so leave it out
when `server.tls_enabled = true`. Prepared statements are not implemented yet,
so the JDBC driver can connect and browse catalogs and tables but cannot run
queries (see below).

## What is supported

Statement queries (`SELECT`, `SHOW DATABASES`, `SHOW TABLES`), the catalog,
schema, table and table-type metadata calls, and SqlInfo. Prepared statements,
transactions and writes are not implemented. Clients that require prepared
statements, which includes the Arrow Flight SQL JDBC driver and the BI tools
built on it, cannot run queries yet; the ADBC drivers are the tested clients.

SqlInfo describes the server without running a query. The JDBC driver answers
`DatabaseMetaData` from it, and ADBC's `GetInfo` reads the vendor name and
versions from it. Arc reports:

| SqlInfo | Value |
| --- | --- |
| `FLIGHT_SQL_SERVER_NAME` | `Arc` |
| `FLIGHT_SQL_SERVER_VERSION` | the Arc version |
| `FLIGHT_SQL_SERVER_ARROW_VERSION` | the Arrow Go version Arc is built with |
| `FLIGHT_SQL_SERVER_READ_ONLY` | `true` |
| `FLIGHT_SQL_SERVER_SQL` | `true` |
| `FLIGHT_SQL_SERVER_SUBSTRAIT`, `FLIGHT_SQL_SERVER_CANCEL`, `FLIGHT_SQL_SERVER_BULK_INGESTION` | `false` |
| `FLIGHT_SQL_SERVER_TRANSACTION` | none |
| `SQL_DDL_CATALOG`, `SQL_DDL_SCHEMA`, `SQL_DDL_TABLE` | `false` |
| `SQL_IDENTIFIER_QUOTE_CHAR` | `"` |
| `SQL_SEARCH_STRING_ESCAPE` | `\` (escapes `%` and `_` in metadata patterns) |
| `SQL_CATALOG_TERM` | `database` |
| `SQL_CATALOG_AT_START` | `true` (`database.measurement`) |
| `SQL_SUPPORTS_COLUMN_ALIASING`, `SQL_SUPPORTS_TABLE_CORRELATION_NAMES`, `SQL_NULL_PLUS_NULL_IS_NULL`, `SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY`, `SQL_SUPPORTS_ORDER_BY_UNRELATED`, `SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE`, `SQL_CORRELATED_SUBQUERIES_SUPPORTED` | `true` |
| `SQL_SUPPORTED_GROUP_BY` | unrelated, beyond select |
| `SQL_SUPPORTED_SUBQUERIES` | comparisons, `EXISTS`, `IN`, quantified |
| `SQL_SUPPORTED_UNIONS` | `UNION`, `UNION ALL` |
| `SQL_OUTER_JOINS_SUPPORT_LEVEL` | full outer joins |
| `SQL_SUPPORTED_RESULT_SET_TYPES` | forward only |
| `SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS` | none |
| `SQL_SUPPORTED_GRAMMAR` | the ODBC minimum grammar (its write statements excepted, as ODBC allows a read-only source) |

`SQL_ANSI92_SUPPORTED_LEVEL` is not reported: every ANSI-92 level includes
`CREATE TABLE`, `INSERT`, `UPDATE` and `DELETE`, which Arc refuses. The JDBC
driver answers any SqlInfo the server does not report with `false`, `0` or an
empty string, so `supportsANSI92EntryLevelSQL()` returns `false`. Driver 19.0.0
also reads the outer-join level as a bitmask, so it
reports `supportsOuterJoins()` as `true` but `supportsFullOuterJoins()` as
`false`; full outer joins work.

A request for IDs Arc has no value for is answered with the ones it has; the
rest are left out rather than failing the call. With the JDBC driver, pass
the Arc token as the password (the user name is not checked): the driver's
`token` property opens the connection with a bearer-token handshake, which the
listener does not accept.

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
scaled to fit in half of the time the shutdown hooks have left when it
starts. The hooks get 40% of `server.shutdown_timeout` (the rest is kept for
the buffer flush, the WAL and the database), so at the default 30 s the
Flight SQL listener takes at most 6 seconds on a standalone node and about
3 in a cluster, after the ready drain. The
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
