//go:build duckdb_arrow && flightbench

package api

// Benchmark arm for scripts/flightbench, compiled only with the flightbench
// tag. Never part of a release build: it serves a statement straight from
// DuckDB, skipping the bridge and with it RBAC, governance, the query
// registry, forwarding, arcx, the audit log, request logging and the query
// timeout. The gRPC listener still checks that the token is valid, but not
// what it may read: in a bench build any valid token reads every database
// through this arm. Its only purpose is to bound what the bridge costs:
// flight minus this arm is the most a change to the bridge could save, and
// more than one that keeps the bridge's checks can (see scripts/flightbench).

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// flightBenchDirectHeader selects the direct arm for one call.
const flightBenchDirectHeader = "x-arc-bench-direct"

func init() { flightSQLBenchDirect = flightBenchDirect }

func flightBenchDirect(ctx context.Context, s *flightSQLService, sql string) (*arrow.Schema, <-chan flight.StreamChunk, bool, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if v := md.Get(flightBenchDirectHeader); len(v) != 1 || v[0] != "1" {
		return nil, nil, false, nil
	}
	h := s.queryHandler
	converted, _, _, err := h.getTransformedSQLWithRefs(ctx, sql, "")
	if err != nil {
		return nil, nil, true, status.Error(codes.InvalidArgument, err.Error())
	}
	reader, conn, err := h.db.ArrowQueryContext(ctx, converted)
	if err != nil {
		return nil, nil, true, status.Error(codes.Internal, err.Error())
	}
	schema := reader.Schema()
	stream := make(chan flight.StreamChunk)
	go func() {
		// StreamChunksFromReader releases the reader; the connection goes
		// back to the pool after it.
		flight.StreamChunksFromReader(ctx, reader, stream)
		_ = conn.Close()
	}()
	return schema, stream, true, nil
}
