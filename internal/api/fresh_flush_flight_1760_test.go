//go:build duckdb_arrow

package api

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// #1760: Flight SQL refuses x-arc-freshness on every statement RPC with
// InvalidArgument, whatever its value, after its auth middleware and before
// anything is planned, run or forwarded.
func TestFreshness_RefusedOnFlightSQL(t *testing.T) {
	client, ctx := newDatabaseHeaderFlightServer(t, nil)
	const sql = "SELECT value FROM sample.cpu"
	fresh := func(ctx context.Context, v string) context.Context {
		return metadata.AppendToOutgoingContext(ctx, FreshnessHeader, v)
	}
	code := func(err error) codes.Code { return status.Code(err) }

	// Without the header (or with only an empty value), unchanged.
	rows, err := rowsOf(t, client, ctx, ctx, sql)
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)
	rows, err = rowsOf(t, client, fresh(ctx, ""), fresh(ctx, ""), sql)
	require.NoError(t, err)
	require.EqualValues(t, 1, rows)

	// GetFlightInfoStatement.
	_, err = client.Execute(fresh(ctx, "flush"), sql)
	refused(t, err)
	_, err = client.Execute(fresh(ctx, " FLUSH "), sql)
	refused(t, err)
	_, err = client.Execute(fresh(ctx, "yes"), sql)
	refused(t, err)

	// DoGetStatement, on a ticket planned without the header.
	_, err = rowsOf(t, client, ctx, fresh(ctx, "flush"), sql)
	refused(t, err)

	// CreatePreparedStatement.
	_, err = client.Prepare(fresh(ctx, "flush"), sql)
	refused(t, err)

	// GetFlightInfoPreparedStatement on a statement prepared without the
	// header, and DoGet of the statement ticket it plans.
	ps, err := client.Prepare(ctx, sql)
	require.NoError(t, err)
	defer ps.Close(ctx)
	_, err = ps.Execute(fresh(ctx, "flush"))
	refused(t, err)
	info, err := ps.Execute(ctx)
	require.NoError(t, err)
	reader, err := client.DoGet(fresh(ctx, "flush"), info.Endpoint[0].Ticket)
	if err == nil {
		for reader.Next() {
		}
		err = reader.Err()
		reader.Release()
	}
	refused(t, err)

	// GetSchemaPreparedStatement, on a statement prepared without the header.
	_, err = ps.GetSchema(fresh(ctx, "flush"))
	refused(t, err)

	// Every value is checked, not only the first.
	_, err = client.Execute(metadata.AppendToOutgoingContext(ctx, FreshnessHeader, "", FreshnessHeader, "flush"), sql)
	refused(t, err)

	// Auth first: an unauthenticated call learns nothing about the header.
	_, err = client.Execute(fresh(context.Background(), "flush"), sql)
	require.Equal(t, codes.Unauthenticated, code(err), "%v", err)
}

// DoGetPreparedStatement serves a ticket that is the prepared statement
// itself, which arrow-go's client never sends (its tickets are statement
// tickets), so it is called directly, as is the handle check order. The
// value's trimming, which a gRPC client may not do, is checked here too.
func TestFreshness_RefusedOnFlightSQLPreparedTicket(t *testing.T) {
	s := newBridgeTestServer(t, nil).service
	in := func(v string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.Pairs(FreshnessHeader, v))
	}
	handle := preparedHandle(`{"v":1,"sql":"SELECT 1"}`)
	// Refused before the handle is decoded: a bad handle gets the refusal.
	_, _, err := s.DoGetPreparedStatement(in("flush"), preparedHandle(`not json`))
	refused(t, err)
	_, err = s.GetSchemaPreparedStatement(in("flush"), preparedHandle(`not json`), nil)
	refused(t, err)
	_, _, err = s.DoGetPreparedStatement(in("flush"), handle)
	refused(t, err)
	_, _, err = s.DoGetPreparedStatement(in(" Flush\t"), handle)
	refused(t, err)
	_, _, err = s.DoGetPreparedStatement(in("no"), handle)
	refused(t, err)
	// Whitespace only counts as absent: the statement runs (and fails later
	// for want of a bridge, not with the refusal).
	_, _, err = s.DoGetPreparedStatement(in(" \t"), handle)
	require.NotContains(t, fmt.Sprint(err), "x-arc-freshness")
}

// refused: the call was refused for x-arc-freshness, not for another reason.
func refused(t *testing.T, err error) {
	t.Helper()
	require.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
	require.Contains(t, status.Convert(err).Message(), "x-arc-freshness", "%v", err)
}
