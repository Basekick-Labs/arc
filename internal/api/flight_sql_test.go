//go:build duckdb_arrow

package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestFlightSQLServerQueriesMetadataAndEnforcesAuth(t *testing.T) {
	logger := zerolog.Nop()
	storageRoot := t.TempDir()
	backend, err := storage.NewLocalBackend(storageRoot, logger)
	require.NoError(t, err)
	defer backend.Close()
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize:   100,
		MaxBufferAgeMS:  600000,
		Compression:     "snappy",
		FlushWorkers:    1,
		FlushQueueSize:  4,
		ShardCount:      1,
		DataPageVersion: "2.0",
	}, backend, logger)
	defer buffer.Close()
	require.NoError(t, buffer.WriteTypedColumnarDirect(context.Background(), "sample", "cpu", &ingest.TypedColumnBatch{
		Data: map[string]interface{}{
			"time":  []int64{time.Now().UTC().UnixMicro()},
			"value": []float64{42},
		},
	}, 1))
	require.NoError(t, buffer.FlushAllAndWait(context.Background()))

	duck, err := database.New(&database.Config{
		MemoryLimit:      "256MB",
		ThreadCount:      2,
		MaxConnections:   4,
		LocalStorageRoot: storageRoot,
	}, logger)
	require.NoError(t, err)
	defer duck.Close()

	authManager, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, logger)
	require.NoError(t, err)
	defer authManager.Close()
	token, err := authManager.CreateToken(context.Background(), "flight-test", "", "read", nil)
	require.NoError(t, err)

	queryHandler := NewQueryHandler(duck, backend, logger, 0, 0)
	queryHandler.SetAuthAndRBAC(authManager, nil)
	portListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := portListener.Addr().(*net.TCPAddr).Port
	require.NoError(t, portListener.Close())

	certFile, keyFile, rootCAs := createFlightSQLTestTLS(t)
	server, err := NewFlightSQLServer(queryHandler, FlightSQLServerConfig{
		Host:        "127.0.0.1",
		Port:        port,
		TLSEnabled:  true,
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
	}, logger)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, server.Shutdown(ctx))
	}
	defer shutdown()
	require.Equal(t, port, server.flight.Addr().(*net.TCPAddr).Port)

	client, err := flightsql.NewClient(
		fmt.Sprintf("127.0.0.1:%d", port), nil, nil,
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    rootCAs,
			ServerName: "127.0.0.1",
		})),
	)
	require.NoError(t, err)
	defer client.Client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = client.Execute(ctx, "SELECT 1")
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	ctx, err = client.Client.AuthenticateBasicToken(ctx, "", token)
	require.NoError(t, err)

	info, err := client.Execute(ctx, "SELECT 42::BIGINT AS value")
	require.NoError(t, err)
	require.Len(t, info.Endpoint, 1)
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	require.True(t, reader.Next(), "query returned no record: %v", reader.Err())
	values, ok := reader.RecordBatch().Column(0).(*array.Int64)
	require.True(t, ok)
	require.EqualValues(t, 42, values.Value(0))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
	reader.Release()

	_, err = client.Execute(ctx, "DELETE FROM sample.cpu")
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	catalogs, err := client.GetCatalogs(ctx)
	require.NoError(t, err)
	catalogReader, err := client.DoGet(ctx, catalogs.Endpoint[0].Ticket)
	require.NoError(t, err)
	foundCatalog := false
	for catalogReader.Next() {
		column := catalogReader.RecordBatch().Column(0).(*array.String)
		for row := 0; row < column.Len(); row++ {
			foundCatalog = foundCatalog || column.Value(row) == "sample"
		}
	}
	require.NoError(t, catalogReader.Err())
	catalogReader.Release()
	require.True(t, foundCatalog)

	catalog := "sample"
	schemaPattern := "%"
	schemas, err := client.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{
		Catalog:               &catalog,
		DbSchemaFilterPattern: &schemaPattern,
	})
	require.NoError(t, err)
	schemaReader, err := client.DoGet(ctx, schemas.Endpoint[0].Ticket)
	require.NoError(t, err)
	foundSchema := false
	for schemaReader.Next() {
		record := schemaReader.RecordBatch()
		catalogs := record.Column(0).(*array.String)
		dbSchemas := record.Column(1).(*array.String)
		for row := int64(0); row < record.NumRows(); row++ {
			foundSchema = foundSchema || (catalogs.Value(int(row)) == "sample" && dbSchemas.Value(int(row)) == "")
		}
	}
	require.NoError(t, schemaReader.Err())
	schemaReader.Release()
	require.True(t, foundSchema)

	tablePattern := "c%"
	tables, err := client.GetTables(ctx, &flightsql.GetTablesOpts{
		Catalog:                &catalog,
		TableNameFilterPattern: &tablePattern,
		IncludeSchema:          true,
	})
	require.NoError(t, err)
	tableReader, err := client.DoGet(ctx, tables.Endpoint[0].Ticket)
	require.NoError(t, err)
	foundMeasurement := false
	var tableSchema []byte
	for tableReader.Next() {
		record := tableReader.RecordBatch()
		column := record.Column(2).(*array.String)
		for row := 0; row < column.Len(); row++ {
			if column.Value(row) == "cpu" {
				foundMeasurement = true
				tableSchema = append([]byte(nil), record.Column(4).(*array.Binary).Value(row)...)
			}
		}
	}
	require.NoError(t, tableReader.Err())
	tableReader.Release()
	require.True(t, foundMeasurement)
	schema, err := flight.DeserializeSchema(tableSchema, memory.DefaultAllocator)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"time", "value"}, []string{schema.Field(0).Name, schema.Field(1).Name})
}

func createFlightSQLTestTLS(t *testing.T) (string, string, *x509.CertPool) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	require.NoError(t, err)
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	key := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	certFile := filepath.Join(t.TempDir(), "server.crt")
	keyFile := filepath.Join(filepath.Dir(certFile), "server.key")
	require.NoError(t, os.WriteFile(certFile, certificate, 0o644))
	require.NoError(t, os.WriteFile(keyFile, key, 0o600))
	rootCAs := x509.NewCertPool()
	require.True(t, rootCAs.AppendCertsFromPEM(certificate))
	return certFile, keyFile, rootCAs
}

func TestFlightPatternMatchesEscapesSQLWildcards(t *testing.T) {
	cases := []struct {
		value   string
		pattern string
		want    bool
	}{
		{value: "cpu", pattern: "c%", want: true},
		{value: "cpu", pattern: "c_u", want: true},
		{value: "cpu_1", pattern: `cpu\_%`, want: true},
		{value: "cpu1", pattern: `cpu\_%`, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.pattern+"/"+tc.value, func(t *testing.T) {
			pattern := compileFlightPattern(&tc.pattern)
			if got := matchesFlightPattern(tc.value, pattern); got != tc.want {
				t.Fatalf("matchesFlightPattern(%q, %q) = %t, want %t", tc.value, tc.pattern, got, tc.want)
			}
		})
	}
}
