//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// newDatabaseHeaderFlightServer runs the real query handler behind Flight SQL
// with one measurement, sample.cpu, and returns an authenticated client.
func newDatabaseHeaderFlightServer(t *testing.T) (*flightsql.Client, context.Context) {
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
	require.NoError(t, buffer.WriteTypedColumnarDirect(context.Background(), "sample", "cpu", &ingest.TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{time.Now().UTC().UnixMicro()},
			"value": []float64{42},
		},
	}, 1))
	require.NoError(t, buffer.FlushAllAndWait(context.Background()))

	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: storageRoot}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })
	authManager, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, logger)
	require.NoError(t, err)
	t.Cleanup(func() { authManager.Close() })
	token, err := authManager.CreateToken(context.Background(), "flight-db-header", "", "read", nil)
	require.NoError(t, err)

	queryHandler := NewQueryHandler(duck, backend, logger, 0, 0)
	queryHandler.SetAuthAndRBAC(authManager, nil)
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
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	ctx, err = client.Client.AuthenticateBasicToken(ctx, "", token)
	require.NoError(t, err)
	return client, ctx
}

func withDatabase(ctx context.Context, values ...string) context.Context {
	for _, v := range values {
		ctx = metadata.AppendToOutgoingContext(ctx, flightSQLDatabaseHeader, v)
	}
	return ctx
}

// rowsOf runs one statement, sending the header values given for each RPC.
func rowsOf(t *testing.T, client *flightsql.Client, executeCtx, doGetCtx context.Context, sql string) (int64, error) {
	t.Helper()
	info, err := client.Execute(executeCtx, sql)
	if err != nil {
		return 0, err
	}
	reader, err := client.DoGet(doGetCtx, info.Endpoint[0].Ticket)
	if err != nil {
		return 0, err
	}
	defer reader.Release()
	var rows int64
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	return rows, reader.Err()
}

func TestFlightSQLDatabaseHeader(t *testing.T) {
	client, ctx := newDatabaseHeaderFlightServer(t)

	t.Run("unqualified table names resolve in the header's database", func(t *testing.T) {
		rows, err := rowsOf(t, client, withDatabase(ctx, "sample"), withDatabase(ctx, "sample"), "SELECT value FROM cpu")
		require.NoError(t, err)
		require.EqualValues(t, 1, rows)
	})

	t.Run("a header sent only with GetFlightInfo carries over in the ticket", func(t *testing.T) {
		rows, err := rowsOf(t, client, withDatabase(ctx, "sample"), ctx, "SELECT value FROM cpu")
		require.NoError(t, err)
		require.EqualValues(t, 1, rows)
	})

	t.Run("a header sent only with DoGet applies", func(t *testing.T) {
		rows, err := rowsOf(t, client, ctx, withDatabase(ctx, "sample"), "SELECT value FROM cpu")
		require.NoError(t, err)
		require.EqualValues(t, 1, rows)
	})

	t.Run("a DoGet header that differs from the ticket's is refused", func(t *testing.T) {
		_, err := rowsOf(t, client, withDatabase(ctx, "sample"), withDatabase(ctx, "other"), "SELECT value FROM cpu")
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	})

	t.Run("the header twice is refused", func(t *testing.T) {
		_, err := client.Execute(withDatabase(ctx, "sample", "other"), "SELECT value FROM cpu")
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	})

	t.Run("an invalid database name is refused before the bridge", func(t *testing.T) {
		_, err := client.Execute(withDatabase(ctx, "sample;x"), "SELECT value FROM cpu")
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	})

	t.Run("with the header, a qualified name is refused as on HTTP", func(t *testing.T) {
		_, err := rowsOf(t, client, withDatabase(ctx, "sample"), withDatabase(ctx, "sample"), "SELECT value FROM sample.cpu")
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	})

	t.Run("an empty header is no header", func(t *testing.T) {
		rows, err := rowsOf(t, client, withDatabase(ctx, ""), withDatabase(ctx, ""), `SELECT value FROM "sample"."cpu"`)
		require.NoError(t, err)
		require.EqualValues(t, 1, rows)
	})

	// The catalog calls qualify every table, which the route refuses next to
	// the header: they must not carry it, or GetTables with schemas fails for
	// every client that sends the header on every call.
	t.Run("GetTables with schemas ignores the header", func(t *testing.T) {
		hctx := withDatabase(ctx, "sample")
		catalog := "sample"
		tables, err := client.GetTables(hctx, &flightsql.GetTablesOpts{Catalog: &catalog, IncludeSchema: true})
		require.NoError(t, err)
		reader, err := client.DoGet(hctx, tables.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer reader.Release()
		found := false
		for reader.Next() {
			names := reader.RecordBatch().Column(2).(*array.String)
			for i := 0; i < names.Len(); i++ {
				found = found || names.Value(i) == "cpu"
			}
		}
		require.NoError(t, reader.Err())
		require.True(t, found, "cpu not listed")
	})
}

// Only x-arc-database crosses the bridge from the client's metadata. The peer
// header is the bridge's proxy header (the audit log's client address), the
// key authenticates the bridge, and the other x-arc-* headers change what the
// route encodes or whether it forwards.
func TestFlightSQLForwardsOnlyTheDatabaseHeader(t *testing.T) {
	var mu sync.Mutex
	var seen http.Header
	record := completionTestRecord(t)
	client := completionTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = r.Header.Clone()
		mu.Unlock()
		w.Header().Set("Content-Type", "application/vnd.apache.arrow.stream")
		var payload bytes.Buffer
		writer := ipc.NewWriter(&payload, ipc.WithSchema(record.Schema()))
		_ = writer.Write(record)
		_ = writer.Close()
		_, _ = w.Write(payload.Bytes())
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx,
		flightSQLDatabaseHeader, "sample",
		flightSQLPeerHeader, "198.51.100.9",
		flightSQLInternalKeyHeader, "forged",
		"x-arc-forwarded-by", "node-x",
		"x-arc-arrow-compression", "zstd",
		"x-arc-arrow-dictionary", "true",
		"x-arc-profile", "true",
	)
	_, err := rowsOf(t, client, ctx, ctx, "SELECT 1")
	require.NoError(t, err)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "sample", seen.Get(flightSQLDatabaseHeader))
	require.NotEqual(t, "198.51.100.9", seen.Get(flightSQLPeerHeader), "a client must not choose the address the audit log records")
	require.NotEqual(t, "forged", seen.Get(flightSQLInternalKeyHeader))
	for _, h := range []string{"X-Arc-Forwarded-By", "X-Arc-Arrow-Compression", "X-Arc-Arrow-Dictionary", "X-Arc-Profile"} {
		require.Empty(t, seen.Get(h), "%s crossed the bridge", h)
	}
}

// The loopback hop asks for no compression: gzip there only costs CPU.
func TestFlightSQLBridgeAsksForNoCompression(t *testing.T) {
	s := newBridgeTestServer(t, nil)
	s.bridge.Post("/test/echo", func(c *fiber.Ctx) error {
		return c.SendString(c.Get(fiber.HeaderAcceptEncoding))
	})
	startBridgeTestServer(t, s)
	resp, err := s.service.query(context.Background(), "/test/echo", "SELECT 1", "", "")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Empty(t, string(body), "Accept-Encoding on the bridge request")
}
