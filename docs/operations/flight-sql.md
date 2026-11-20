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
read permission. SQL writes, transactions and prepared statements with
parameters are not supported.

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

With the Arrow Flight SQL JDBC driver, set the `user` and `password`
connection properties, with the token as the password; this is what a tool's
user name and password fields send (DBeaver, DataGrip, Tableau):

```text
URL:      jdbc:arrow-flight-sql://localhost:50051?useEncryption=false
user:     arc        (not checked)
password: <token>
```

The token can also go in the URL, as `token=<token>` (or `password=<token>`).
An Arc token ends in `=`, which driver 19.0.0 does not accept inside a URL
value: there, write it as `%3D`. Left as `=`, the connection is refused with
`must authenticate first` (`token`) or `invalid Arc token` (`password`). As a
connection property it needs no escaping.

The driver turns TLS on unless `useEncryption=false` is set, so leave that out
when `server.tls_enabled = true`. If Arc's certificate is not signed by a CA
the JVM already trusts, give the driver a trust store with it:
`trustStore=<path to the .jks>` and `trustStorePassword=<its password>`.

The driver runs every query as a prepared statement, `Statement.executeQuery`
included; see [Prepared statements](#prepared-statements).

## What is supported

Statement queries (`SELECT`, `SHOW DATABASES`, `SHOW TABLES`), prepared
statements without parameters, the catalog, schema, table and table-type
metadata calls, and SqlInfo. Parameters, transactions and writes are not
implemented. The ADBC drivers and the Arrow Flight SQL JDBC driver (19.0.0)
are the tested clients.

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
rest are left out rather than failing the call.

## Prepared statements

The JDBC driver prepares every query, even a plain `Statement.executeQuery`,
and the ADBC drivers prepare before each execute when the server allows it.
Arc prepares a statement without parameters by **binding** it: DuckDB resolves
its tables and columns, which lists the measurement's files and reads their
Parquet footers but scans no data, and the result schema comes from that. The
query runs when the client executes the statement, exactly as an unprepared
one does.

The bind is not free of work, though. DuckDB's optimizer is off while it
binds, but the binder itself still evaluates constant arguments of functions,
so a statement such as `SELECT length(repeat('x', 500000000))` costs a large
part of what running it does to prepare. DuckDB does not interrupt a bind: the query
timeout is applied when it ends. The bind slots below bound how many run at
once.

- **One query per execute.** The prepare is not a query: it is not in
  `/api/v1/queries`, the query metrics or the slow-query log, and it does not
  count against the token's governance limits. It is refused, though, with
  `RESOURCE_EXHAUSTED`, when those limits would refuse the token's next query.
  The execute is the query, counted, registered and audited once.
- **Same checks as a query.** The SQL is validated, RBAC checks every table it
  reads (a token gets no schema for a table it cannot read), and the bind runs
  under the query timeout or the token's governance timeout.
- **Each prepare costs one bind.** With JDBC that is one per `executeQuery`,
  and ADBC prepares before each execute. The bind lists the measurement's
  files, so its cost grows with their number: on 2,000 Parquet files on local
  disk it took about 43 ms, against about 110 ms for the query itself; on a
  few files, about 1 ms.
  At most a quarter of `database.max_connections` binds (at least one, and
  one when `database.max_connections` is 0 or less, unlimited) run at once; more wait
  for a slot. `arc_flight_sql_prepare_binds_total{result}` and
  `arc_flight_sql_prepare_bind_duration_seconds` measure them (the wait for a
  slot included), and a bind that takes `query.slow_query_threshold_ms` or
  more, not counting the wait, is logged as a warning. When
  audit logging is on, a prepare is recorded as an `api.post` event on
  `/internal/flight-sql/schema` with the detail
  `{"operation": "flight_sql.prepare"}`, not as a `data.query`.
- **No server state.** The prepared statement handle carries the SQL (and the
  `x-arc-database` header the prepare saw, with the same rules as a statement
  ticket), so closing it frees nothing and a handle survives a restart.

What answers `UNIMPLEMENTED`, so the ADBC drivers run the statement unprepared
instead (the JDBC driver reports the error):

- a `PIVOT` (or `PIVOT_WIDER`), whose columns DuckDB finds only by reading the
  data. Over JDBC, write it as conditional aggregation instead, one column per
  value: `sum(x) FILTER (WHERE k = 'v') AS v`, which prepares;
- a result column of a type Arc cannot describe without running the query,
  such as a struct with unnamed fields (`(1, 2)`, `row(...)`);
- any prepare on a node that does not run queries (a compactor). The JDBC
  driver cannot run anything there: point it at a node that runs queries.

Parameters (`?`) are refused with `INVALID_ARGUMENT`, as is a statement that
does not bind because of the statement itself (a syntax error, an unknown
column). A bind that could not read storage (an IO or object-store error) or
ran out of memory is `UNAVAILABLE`, to be retried; a damaged Parquet file is
`INTERNAL`. A query on a measurement with
no data yet has a result without columns; JDBC 19.0.0 reads that as an update
and the execute is refused with `INVALID_ARGUMENT`, explaining why. A `SHOW`
statement is prepared without a bind: its schema is fixed.

## Choosing a database

As on the HTTP query API, a statement can carry an `x-arc-database` header;
unqualified table names then resolve in that database. Without it they resolve
in `default`, so qualify them (`"mydb"."cpu"`). With ADBC, set the header as a
call header on the connection:
`adbc.flight.sql.rpc.call_header.x-arc-database` = the database name (see the
Python example below).

- **Same rules as HTTP:** the name is validated, RBAC checks the resolved
  table, and while the header is set any database-qualified name is refused,
  even the header's own database (`"mydb"."cpu"`). Tools that write qualified
  names, as BI tools do, should leave the header unset.
- **Which call carries it:** it can be sent on the call that plans the
  statement (GetFlightInfo), on the call that fetches its result (DoGet), or
  on both; when both carry it the values must match. Sent twice on one call,
  or with an invalid name, it is refused with `INVALID_ARGUMENT`.
- **Calls that ignore it:** catalog, schema, table and SqlInfo calls. Browsing
  every database works with the header set.

Of the client's other headers, only the `authorization` token and the
User-Agent reach the query.

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
    db_kwargs={
        DatabaseOptions.AUTHORIZATION_HEADER.value: f"Bearer {os.environ['ARC_TOKEN']}",
        # Optional: resolve unqualified table names in this database.
        DatabaseOptions.RPC_CALL_HEADER_PREFIX.value + "x-arc-database": "default",
    },
)

with connection.cursor() as cursor:
    cursor.execute("SELECT * FROM cpu LIMIT 10")
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
