//go:build duckdb_arrow

package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestFlightSQLBearerHandshake connects the way the Arrow Flight SQL JDBC
// driver does with its `token` property: a Handshake carrying the token as a
// bearer token, then the same header on every call.
func TestFlightSQLBearerHandshake(t *testing.T) {
	client, token := startFlightSQLAuthTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	trailer, err := flightHandshake(ctx, client, "Bearer "+token)
	require.NoError(t, err)
	require.Equal(t, []string{"Bearer " + token}, trailer.Get("authorization"))

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	info, err := client.Execute(ctx, "SELECT 42::BIGINT AS value")
	require.NoError(t, err)
	require.Len(t, info.Endpoint, 1)
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer reader.Release()
	require.True(t, reader.Next(), "query returned no record: %v", reader.Err())
	values, ok := reader.RecordBatch().Column(0).(*array.Int64)
	require.True(t, ok)
	require.EqualValues(t, 42, values.Value(0))
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}

func TestFlightSQLHandshakeRefusals(t *testing.T) {
	client, token := startFlightSQLAuthTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cases := []struct {
		name          string
		authorization string
	}{
		{name: "unknown bearer token", authorization: "Bearer arc_not_a_token"},
		{name: "empty bearer token", authorization: "Bearer "},
		{name: "other scheme", authorization: "Token " + token},
		{name: "no credentials"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := flightHandshake(ctx, client, tc.authorization)
			require.Equal(t, codes.Unauthenticated, status.Code(err), "handshake error: %v", err)
		})
	}

	// A Basic handshake still answers with the token for the calls after it.
	authed, err := client.Client.AuthenticateBasicToken(ctx, "", token)
	require.NoError(t, err)
	_, err = client.Execute(authed, "SELECT 1")
	require.NoError(t, err)
}

func startFlightSQLAuthTestServer(t *testing.T) (*flightsql.Client, string) {
	t.Helper()
	logger := zerolog.Nop()
	storageRoot := t.TempDir()
	backend, err := storage.NewLocalBackend(storageRoot, logger)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	duck, err := database.New(&database.Config{
		MemoryLimit:      "256MB",
		ThreadCount:      2,
		MaxConnections:   4,
		LocalStorageRoot: storageRoot,
	}, logger)
	require.NoError(t, err)
	t.Cleanup(func() { duck.Close() })

	authManager, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, logger)
	require.NoError(t, err)
	t.Cleanup(func() { authManager.Close() })
	token, err := authManager.CreateToken(context.Background(), "flight-bearer", "", "read", nil)
	require.NoError(t, err)

	queryHandler := NewQueryHandler(duck, backend, logger, 0, 0)
	queryHandler.SetAuthAndRBAC(authManager, nil)
	portListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := portListener.Addr().(*net.TCPAddr).Port
	require.NoError(t, portListener.Close())

	server, err := NewFlightSQLServer(queryHandler, FlightSQLServerConfig{Host: "127.0.0.1", Port: port}, logger)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, server.Shutdown(ctx))
	})

	client, err := flightsql.NewClient(fmt.Sprintf("127.0.0.1:%d", port), nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Client.Close() })
	return client, token
}

// flightHandshake runs a Handshake with the given authorization header (none
// when empty) and returns the trailer the server answered with.
func flightHandshake(ctx context.Context, client *flightsql.Client, authorization string) (metadata.MD, error) {
	if authorization != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", authorization)
	}
	stream, err := client.Client.Handshake(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		return nil, err
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("handshake answered with a payload")
		}
		return nil, err
	}
	return stream.Trailer(), nil
}
