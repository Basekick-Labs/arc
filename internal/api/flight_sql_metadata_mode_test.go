//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// newMetadataModeFlightServer serves three databases: w0_sales and its
// sibling w0-sales (the pattern "w0_sales" matches both) and zz. With
// withAuth false, auth is off and calls carry no token.
func newMetadataModeFlightServer(t *testing.T, rbac RBACChecker, withAuth bool) (*flightsql.Client, context.Context) {
	t.Helper()
	logger := zerolog.Nop()
	storageRoot := t.TempDir()
	backend, err := storage.NewLocalBackend(storageRoot, logger)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	t.Cleanup(func() { buffer.Close() })
	now := time.Now().UTC().UnixMicro()
	for _, w := range []struct{ db, measurement, field string }{
		{"w0_sales", "orders", "amount"},
		{"w0-sales", "orders", "sibling"},
		{"zz", "cpu", "value"},
	} {
		require.NoError(t, buffer.WriteTypedColumnarDirect(context.Background(), w.db, w.measurement, &ingest.TypedColumnBatch{
			Data: map[string]interface{}{"time": []int64{now}, w.field: []float64{1}},
		}, 1))
	}
	require.NoError(t, buffer.FlushAllAndWait(context.Background()))

	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: storageRoot}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	queryHandler := NewQueryHandler(duck, backend, logger, 0, 0)
	var token string
	if withAuth {
		authManager, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, logger)
		require.NoError(t, err)
		t.Cleanup(func() { authManager.Close() })
		token, err = authManager.CreateToken(context.Background(), "flight-metadata-mode", "", "read", nil)
		require.NoError(t, err)
		queryHandler.SetAuthAndRBAC(authManager, rbac)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	server, err := NewFlightSQLServer(queryHandler, FlightSQLServerConfig{Host: "127.0.0.1", Port: port}, logger)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	})
	client, err := flightsql.NewClient(fmt.Sprintf("127.0.0.1:%d", port), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if withAuth {
		ctx, err = client.Client.AuthenticateBasicToken(ctx, "", token)
		require.NoError(t, err)
	}
	return client, ctx
}

func withMetadataMode(ctx context.Context, values ...string) context.Context {
	for _, v := range values {
		ctx = metadata.AppendToOutgoingContext(ctx, flightSQLMetadataHeader, v)
	}
	return ctx
}

type metadataRow struct {
	catalogNull bool
	catalog     string
	schema      string
	table       string
	hasSchema   bool
}

// fetchMetadata runs GetFlightInfo with infoCtx and DoGet with getCtx and
// returns the rows: catalog, schema and, for tables, the table name and
// whether the row carries an Arrow schema.
func fetchMetadata(t *testing.T, client *flightsql.Client, info *flight.FlightInfo, getCtx context.Context) []metadataRow {
	t.Helper()
	reader, err := client.DoGet(getCtx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer reader.Release()
	var rows []metadataRow
	for reader.Next() {
		rec := reader.RecordBatch()
		for i := 0; i < int(rec.NumRows()); i++ {
			var row metadataRow
			names := rec.Schema().Fields()
			for c, f := range names {
				col := rec.Column(c)
				switch f.Name {
				case "catalog_name":
					row.catalogNull = col.IsNull(i)
					if !row.catalogNull {
						row.catalog = col.(*array.String).Value(i)
					}
				case "db_schema_name":
					row.schema = col.(*array.String).Value(i)
				case "table_name":
					row.table = col.(*array.String).Value(i)
				case "table_schema":
					row.hasSchema = !col.IsNull(i) && len(col.(*array.Binary).Value(i)) > 0
				}
			}
			rows = append(rows, row)
		}
	}
	require.NoError(t, reader.Err())
	return rows
}

func TestFlightSQLMetadataSchemasShape(t *testing.T) {
	client, ctx := newMetadataModeFlightServer(t, nil, true)
	schemas := withMetadataMode(ctx, "schemas")

	t.Run("no catalogs", func(t *testing.T) {
		info, err := client.GetCatalogs(schemas)
		require.NoError(t, err)
		require.Empty(t, fetchMetadata(t, client, info, schemas))
	})

	t.Run("databases are schemas with a null catalog", func(t *testing.T) {
		info, err := client.GetDBSchemas(schemas, &flightsql.GetDBSchemasOpts{})
		require.NoError(t, err)
		rows := fetchMetadata(t, client, info, schemas)
		var names []string
		for _, r := range rows {
			require.True(t, r.catalogNull, "catalog must be null, got %q", r.catalog)
			names = append(names, r.schema)
		}
		require.ElementsMatch(t, []string{"w0_sales", "w0-sales", "zz"}, names)
	})

	t.Run("the catalog filter is ignored", func(t *testing.T) {
		catalog := "zz" // Tableau sends a catalog it remembers, not the schema it lists
		pattern := "w0_sales"
		info, err := client.GetTables(schemas, &flightsql.GetTablesOpts{Catalog: &catalog, DbSchemaFilterPattern: &pattern})
		require.NoError(t, err)
		rows := fetchMetadata(t, client, info, schemas)
		require.NotEmpty(t, rows)
		require.Equal(t, metadataRow{catalogNull: true, schema: "w0_sales", table: "orders"}, rows[0])
	})

	t.Run("the exact schema name comes first", func(t *testing.T) {
		pattern := "w0_sales" // "_" matches "-" too
		info, err := client.GetDBSchemas(schemas, &flightsql.GetDBSchemasOpts{DbSchemaFilterPattern: &pattern})
		require.NoError(t, err)
		rows := fetchMetadata(t, client, info, schemas)
		require.Len(t, rows, 2)
		require.Equal(t, "w0_sales", rows[0].schema)
		require.Equal(t, "w0-sales", rows[1].schema)

		table := "orders"
		info, err = client.GetTables(schemas, &flightsql.GetTablesOpts{DbSchemaFilterPattern: &pattern, TableNameFilterPattern: &table, IncludeSchema: true})
		require.NoError(t, err)
		tables := fetchMetadata(t, client, info, schemas)
		require.Len(t, tables, 2)
		require.Equal(t, "w0_sales", tables[0].schema)
		require.True(t, tables[0].hasSchema)
	})

	t.Run("tables with schemas", func(t *testing.T) {
		info, err := client.GetTables(schemas, &flightsql.GetTablesOpts{IncludeSchema: true})
		require.NoError(t, err)
		require.Equal(t, schema_ref.TablesWithIncludedSchema.Fields(), mustSchema(t, info).Fields())
		rows := fetchMetadata(t, client, info, schemas)
		require.Len(t, rows, 3)
		for _, r := range rows {
			require.True(t, r.catalogNull)
			require.True(t, r.hasSchema, "%s.%s", r.schema, r.table)
		}
	})

	t.Run("key listings are empty", func(t *testing.T) {
		ref := flightsql.TableRef{DBSchema: ptr("w0_sales"), Table: "orders"}
		for name, call := range map[string]func() (*flight.FlightInfo, error){
			"primary":  func() (*flight.FlightInfo, error) { return client.GetPrimaryKeys(schemas, ref) },
			"imported": func() (*flight.FlightInfo, error) { return client.GetImportedKeys(schemas, ref) },
			"exported": func() (*flight.FlightInfo, error) { return client.GetExportedKeys(schemas, ref) },
			"cross": func() (*flight.FlightInfo, error) {
				return client.GetCrossReference(schemas, ref, ref)
			},
		} {
			info, err := call()
			require.NoError(t, err, name)
			require.Empty(t, fetchMetadata(t, client, info, schemas), name)
		}
	})

	t.Run("statements are unaffected by the header", func(t *testing.T) {
		info, err := client.Execute(schemas, `SELECT count(*) FROM "w0_sales"."orders"`)
		require.NoError(t, err)
		reader, err := client.DoGet(schemas, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		require.True(t, reader.Next(), "%v", reader.Err())
		reader.Release()
	})
}

// Without the header, and with "catalogs", every metadata call answers as it
// always has: the database is the catalog, the schema is empty, and the key
// listings are unimplemented with the base server's own error.
func TestFlightSQLMetadataDefaultShapeUnchanged(t *testing.T) {
	client, ctx := newMetadataModeFlightServer(t, nil, true)
	ref := flightsql.TableRef{Catalog: ptr("w0_sales"), Table: "orders"}
	_, baseErr := (&flightsql.BaseServer{}).GetFlightInfoPrimaryKeys(context.Background(), ref, nil)
	for _, c := range []struct {
		name            string
		infoCtx, getCtx context.Context
	}{
		{"no header", ctx, ctx},
		{"catalogs", withMetadataMode(ctx, "catalogs"), withMetadataMode(ctx, "catalogs")},
		{"catalogs on DoGet only", ctx, withMetadataMode(ctx, "catalogs")},
	} {
		t.Run(c.name, func(t *testing.T) {
			info, err := client.GetCatalogs(c.infoCtx)
			require.NoError(t, err)
			var catalogs []string
			for _, r := range fetchMetadata(t, client, info, c.getCtx) {
				require.False(t, r.catalogNull)
				catalogs = append(catalogs, r.catalog)
			}
			require.ElementsMatch(t, []string{"w0_sales", "w0-sales", "zz"}, catalogs)

			catalog := "w0_sales"
			info, err = client.GetDBSchemas(c.infoCtx, &flightsql.GetDBSchemasOpts{Catalog: &catalog})
			require.NoError(t, err)
			require.Equal(t, []metadataRow{{catalog: "w0_sales"}}, fetchMetadata(t, client, info, c.getCtx))

			info, err = client.GetTables(c.infoCtx, &flightsql.GetTablesOpts{Catalog: &catalog})
			require.NoError(t, err)
			require.Equal(t, []metadataRow{{catalog: "w0_sales", table: "orders"}}, fetchMetadata(t, client, info, c.getCtx))

			_, err = client.GetPrimaryKeys(c.infoCtx, ref)
			require.Equal(t, status.Code(baseErr), status.Code(err), "%v", err)
			require.Equal(t, status.Convert(baseErr).Message(), status.Convert(err).Message())
		})
	}
}

// A ticket planned in the schemas shape and fetched without the header gets
// the default rows: the shape follows the call, never an error.
func TestFlightSQLMetadataModeFollowsEachCall(t *testing.T) {
	client, ctx := newMetadataModeFlightServer(t, nil, true)
	info, err := client.GetCatalogs(withMetadataMode(ctx, "schemas"))
	require.NoError(t, err)
	require.Len(t, fetchMetadata(t, client, info, ctx), 3)

	ref := flightsql.TableRef{Table: "orders"}
	info, err = client.GetPrimaryKeys(withMetadataMode(ctx, "schemas"), ref)
	require.NoError(t, err)
	err = doGetErr(client, ctx, info)
	require.Equal(t, codes.Unimplemented, status.Code(err), "%v", err)
}

func TestFlightSQLMetadataHeaderValidation(t *testing.T) {
	client, ctx := newMetadataModeFlightServer(t, nil, true)
	_, err := client.GetCatalogs(withMetadataMode(ctx, "schema"))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), `"schemas"`)
	_, err = client.GetCatalogs(withMetadataMode(ctx, "schemas", "schemas"))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	// A statement never reads the header.
	_, err = client.Execute(withMetadataMode(ctx, "bogus"), "SELECT 1")
	require.NoError(t, err)
}

func TestFlightSQLMetadataSchemasAuth(t *testing.T) {
	t.Run("an invalid token is refused on every key call", func(t *testing.T) {
		client, ctx := newMetadataModeFlightServer(t, nil, true)
		bad := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer not-a-token", flightSQLMetadataHeader, "schemas")
		ref := flightsql.TableRef{Table: "orders"}
		_, err := client.GetPrimaryKeys(bad, ref)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "%v", err)
		info, err := client.GetPrimaryKeys(withMetadataMode(ctx, "schemas"), ref)
		require.NoError(t, err)
		err = doGetErr(client, bad, info)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "%v", err)
	})

	t.Run("a database-scoped token lists only its databases", func(t *testing.T) {
		client, ctx := newMetadataModeFlightServer(t, &mockRBACChecker{enabled: true, allowedDBs: map[string]bool{"zz": true}, deniedReason: "no read permission"}, true)
		schemas := withMetadataMode(ctx, "schemas")
		info, err := client.GetDBSchemas(schemas, &flightsql.GetDBSchemasOpts{})
		require.NoError(t, err)
		rows := fetchMetadata(t, client, info, schemas)
		require.Equal(t, []metadataRow{{catalogNull: true, schema: "zz"}}, rows)
	})

	t.Run("auth off", func(t *testing.T) {
		client, ctx := newMetadataModeFlightServer(t, nil, false)
		schemas := withMetadataMode(ctx, "schemas")
		info, err := client.GetTables(schemas, &flightsql.GetTablesOpts{IncludeSchema: true})
		require.NoError(t, err)
		require.Len(t, fetchMetadata(t, client, info, schemas), 3)
	})
}

// The metadata calls ignore x-arc-database in both shapes (#1720).
func TestFlightSQLMetadataSchemasIgnoresDatabaseHeader(t *testing.T) {
	client, ctx := newMetadataModeFlightServer(t, nil, true)
	both := withDatabase(withMetadataMode(ctx, "schemas"), "zz")
	pattern := "w0_sales"
	info, err := client.GetTables(both, &flightsql.GetTablesOpts{DbSchemaFilterPattern: &pattern, IncludeSchema: true})
	require.NoError(t, err)
	rows := fetchMetadata(t, client, info, both)
	require.Len(t, rows, 2)
	require.True(t, rows[0].hasSchema)
}

// doGetErr is the error a DoGet of the info's ticket ends with.
func doGetErr(client *flightsql.Client, ctx context.Context, info *flight.FlightInfo) error {
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return err
	}
	defer reader.Release()
	for reader.Next() {
	}
	return reader.Err()
}

func mustSchema(t *testing.T, info *flight.FlightInfo) *arrow.Schema {
	t.Helper()
	schema, err := flight.DeserializeSchema(info.Schema, nil)
	require.NoError(t, err)
	return schema
}

func ptr(s string) *string { return &s }
