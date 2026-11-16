//go:build duckdb_arrow && flightbench

package api

// Benchmark arm for scripts/flightbench, compiled only with the flightbench
// tag. Never part of a release build: it serves a statement straight from
// DuckDB, skipping the bridge and with it RBAC, governance, the query
// registry, forwarding, arcx and the audit log. Its only purpose is to
// measure what the bridge costs: the difference between this arm and an
// ordinary Flight SQL query is the most any change to the bridge can save.
// The gRPC listener's own authentication still applies.

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
