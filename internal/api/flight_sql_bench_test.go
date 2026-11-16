//go:build duckdb_arrow && flightbench

package api

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/basekick-labs/arc/internal/config"
	"github.com/basekick-labs/arc/internal/database"
	"github.com/basekick-labs/arc/internal/ingest"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// The direct arm and the bridge must answer a statement with the same schema
// and rows, or the benchmark compares different work.
func TestFlightSQLBenchDirectMatchesTheBridge(t *testing.T) {
	logger := zerolog.Nop()
	root := t.TempDir()
	backend, err := storage.NewLocalBackend(root, logger)
	require.NoError(t, err)
	defer backend.Close()
	buffer := ingest.NewArrowBuffer(&config.IngestConfig{
		MaxBufferSize: 100, MaxBufferAgeMS: 600000, Compression: "snappy",
		FlushWorkers: 1, FlushQueueSize: 4, ShardCount: 1, DataPageVersion: "2.0",
	}, backend, logger)
	defer buffer.Close()
	now := time.Now().UTC().UnixMicro()
	require.NoError(t, buffer.WriteTypedColumnarDirect(context.Background(), "bench", "m", &ingest.TypedColumnBatch{
		Data: map[string]interface{}{"time": []int64{now, now + 1, now + 2}, "v": []int64{1, 2, 3}, "f": []float64{0.5, 1, 1.5}},
	}, 3))
	require.NoError(t, buffer.FlushAllAndWait(context.Background()))
	duck, err := database.New(&database.Config{MemoryLimit: "256MB", ThreadCount: 2, MaxConnections: 4, LocalStorageRoot: root}, logger)
	require.NoError(t, err)
	defer duck.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	server, err := NewFlightSQLServer(NewQueryHandler(duck, backend, logger, 0, 0), FlightSQLServerConfig{Host: "127.0.0.1", Port: port}, logger)
	require.NoError(t, err)
	require.NoError(t, server.Start())
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	client, err := flightsql.NewClient(fmt.Sprintf("127.0.0.1:%d", port), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer client.Client.Close()

	fetch := func(ctx context.Context) (string, [][]string) {
		info, err := client.Execute(ctx, `SELECT time, v, f FROM "bench"."m" ORDER BY v`)
		require.NoError(t, err)
		rdr, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
		require.NoError(t, err)
		defer rdr.Release()
		var rows [][]string
		for rdr.Next() {
			rec := rdr.RecordBatch()
			for r := 0; r < int(rec.NumRows()); r++ {
				var row []string
				for c := 0; c < int(rec.NumCols()); c++ {
					row = append(row, rec.Column(c).ValueStr(r))
				}
				rows = append(rows, row)
			}
		}
		require.NoError(t, rdr.Err())
		return rdr.Schema().String(), rows
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bridgeSchema, bridgeRows := fetch(ctx)
	directSchema, directRows := fetch(metadata.AppendToOutgoingContext(ctx, flightBenchDirectHeader, "1"))
	require.Len(t, bridgeRows, 3)
	require.Equal(t, bridgeSchema, directSchema)
	require.Equal(t, bridgeRows, directRows)
}
