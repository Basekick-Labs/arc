package api

import (
	"context"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// flightFreshnessRefusal refuses an x-arc-freshness call header on Flight SQL
// (#1760, fresh_flush.go). Fresh queries are an HTTP feature, so the answer is
// InvalidArgument for every value, "flush" included, and stays so when the
// feature is turned on. nil without the header, or with only empty values;
// every value is checked. It is the first statement of every RPC that takes a
// statement or a prepared-statement handle: GetFlightInfoStatement,
// DoGetStatement, CreatePreparedStatement, GetSchemaPreparedStatement,
// GetFlightInfoPreparedStatement and DoGetPreparedStatement. It runs after
// the server's auth middleware and before anything is decoded, planned, run
// or forwarded. (The DoPut statement RPCs refuse every call already;
// ClosePreparedStatement runs nothing; the catalog RPCs run no client SQL.)
func flightFreshnessRefusal(ctx context.Context) error {
	for _, v := range metadata.ValueFromIncomingContext(ctx, FreshnessHeader) {
		if strings.TrimSpace(v) != "" {
			return status.Error(codes.InvalidArgument, "x-arc-freshness is not supported on Flight SQL; the query was not run")
		}
	}
	return nil
}
