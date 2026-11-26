//go:build duckdb_arrow

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Follow-ups to #1922 (x-arc-decimals: exact, #1784): the header rule and Flight SQL.

// columnStrings is column 0 of every batch: a decimal128 as its digits, an
// int64 as its digits, NULL as "NULL".
func columnStrings(t *testing.T, reader array.RecordReader) []string {
	t.Helper()
	var out []string
	for reader.Next() {
		col := reader.RecordBatch().Column(0)
		for i := 0; i < col.Len(); i++ {
			switch {
			case col.IsNull(i):
				out = append(out, "NULL")
			case col.DataType().ID() == arrow.DECIMAL128:
				out = append(out, col.(*array.Decimal128).Value(i).BigInt().String())
			case col.DataType().ID() == arrow.INT64:
				out = append(out, strconv.FormatInt(col.(*array.Int64).Value(i), 10))
			default:
				t.Fatalf("unexpected column type %s", col.DataType())
			}
		}
	}
	require.NoError(t, reader.Err())
	return out
}

// arrowWithDecimals posts sql to the Arrow route with one x-arc-decimals line
// per value (none for nil). On 200 it returns the schema of column 0 and its
// values; otherwise the error message.
func (r *overflowRig) arrowWithDecimals(t *testing.T, sql string, values []string) (int, string, arrow.DataType, []string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"sql": sql})
	req, err := http.NewRequest(http.MethodPost, r.base+"/api/v1/query/arrow", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if values != nil {
		req.Header[http.CanonicalHeaderKey(decimalsHeader)] = values
	}
	resp, err := r.client.Do(req)
	require.NoError(t, err)
	if resp.StatusCode != http.StatusOK {
		_, msg := jsonError(t, resp)
		return resp.StatusCode, msg, nil, nil
	}
	defer resp.Body.Close()
	reader, err := ipc.NewReader(resp.Body)
	require.NoError(t, err)
	defer reader.Release()
	typ := reader.Schema().Field(0).Type
	return resp.StatusCode, "", typ, columnStrings(t, reader)
}

// flightDecimals appends one x-arc-decimals call header per value.
func flightDecimals(ctx context.Context, values []string) context.Context {
	for _, v := range values {
		ctx = metadata.AppendToOutgoingContext(ctx, decimalsHeader, v)
	}
	return ctx
}

// The header rule is the same on HTTP and Flight SQL: every value read and
// trimmed, empty ones dropped, more than one refused (identical ones
// included), the one left exactly "exact" or "default".
func TestDecimalsHeaderRuleIsTheSameOnHTTPAndFlight1784(t *testing.T) {
	const sql = "SELECT sum(i) AS s FROM range(3) t(i)"
	const repeated, invalid = "sent more than once", "invalid x-arc-decimals header"
	exact := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	cases := []struct {
		name    string
		values  []string
		want    arrow.DataType // nil: refused
		refusal string
	}{
		{"absent", nil, arrow.PrimitiveTypes.Int64, ""},
		{"empty", []string{""}, arrow.PrimitiveTypes.Int64, ""},
		{"blank", []string{"   "}, arrow.PrimitiveTypes.Int64, ""},
		{"default", []string{"default"}, arrow.PrimitiveTypes.Int64, ""},
		{"exact", []string{"exact"}, exact, ""},
		{"padded exact", []string{"  exact "}, exact, ""},
		{"empty then exact", []string{"", "exact"}, exact, ""},
		{"exact then default", []string{"exact", "default"}, nil, repeated},
		{"default then exact", []string{"default", "exact"}, nil, repeated},
		{"exact twice", []string{"exact", "exact"}, nil, repeated},
		{"default twice", []string{"default", "default"}, nil, repeated},
		{"exact, exact on one line", []string{"exact, exact"}, nil, invalid},
		{"Exact", []string{"Exact"}, nil, invalid},
		{"EXACT", []string{"EXACT"}, nil, invalid},
		{"bogus", []string{"bogus"}, nil, invalid},
	}
	r := newOverflowRig(t)
	rig := newPreparedRig(t, preparedRigOptions{})
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			code, msg, typ, vals := r.arrowWithDecimals(t, sql, c.values)
			if c.want == nil {
				require.Equal(t, http.StatusBadRequest, code, "HTTP")
				require.Contains(t, msg, c.refusal, "HTTP")
			} else {
				require.Equal(t, http.StatusOK, code, "HTTP: %s", msg)
				require.Equal(t, c.want.String(), typ.String(), "HTTP")
				require.Equal(t, []string{"3"}, vals, "HTTP")
			}

			ctx := flightDecimals(rig.ctx, c.values)
			info, err := rig.client.Execute(ctx, sql)
			if c.want == nil {
				require.Equal(t, codes.InvalidArgument, status.Code(err), "Flight: %v", err)
				require.Contains(t, status.Convert(err).Message(), c.refusal, "Flight")
				return
			}
			require.NoError(t, err, "Flight")
			reader, err := rig.client.DoGet(ctx, info.Endpoint[0].Ticket)
			require.NoError(t, err, "Flight")
			defer reader.Release()
			require.Equal(t, c.want.String(), reader.Schema().Field(0).Type.String(), "Flight")
			require.Equal(t, []string{"3"}, columnStrings(t, reader), "Flight")
		})
	}
}

// Ad hoc statements: GetFlightInfo stores the mode in the ticket and DoGet
// must send the same one (kills a ticket that drops it, and a DoGet that does
// not check it).
func TestFlightSQLAdHocStatementKeepsTheMode1784(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	exact := flightDecimals(rig.ctx, []string{decimalModeExact})
	const sql = "SELECT 100000000000000000000::HUGEINT AS v"

	info, err := rig.client.Execute(exact, sql)
	require.NoError(t, err)
	reader, err := rig.client.DoGet(exact, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	require.Equal(t, "decimal(38, 0)", reader.Schema().Field(0).Type.String())
	require.Equal(t, []string{"100000000000000000000"}, columnStrings(t, reader))
	reader.Release()

	for name, ctx := range map[string]context.Context{
		"without the header": rig.ctx,
		"with default":       flightDecimals(rig.ctx, []string{decimalModeDefault}),
	} {
		_, err = doGetAll(rig.client, ctx, info.Endpoint[0].Ticket)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "an exact ticket fetched %s: %v", name, err)
	}

	info, err = rig.client.Execute(rig.ctx, sql)
	require.NoError(t, err)
	_, err = doGetAll(rig.client, exact, info.Endpoint[0].Ticket)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "a default ticket fetched with exact: %v", err)
}

// doGetAll fetches a ticket and drains it, returning the first error.
func doGetAll(client *flightsql.Client, ctx context.Context, ticket *flight.Ticket) (*arrow.Schema, error) {
	reader, err := client.DoGet(ctx, ticket)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	for reader.Next() {
	}
	return reader.Schema(), reader.Err()
}

// A prepared statement's GetSchema and a DoGet on its command (a client that
// fetches the prepared statement directly, without GetFlightInfo) follow the
// stored mode and refuse a call that names another, a call without the header
// after an exact prepare included.
func TestFlightSQLPreparedStatementKeepsTheModeEverywhere1784(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	exact := flightDecimals(rig.ctx, []string{decimalModeExact})
	const sql = "SELECT 100000000000000000000::HUGEINT AS v"

	ps, err := rig.client.Prepare(exact, sql)
	require.NoError(t, err)
	defer func() { require.NoError(t, ps.Close(exact)) }()
	got, err := ps.GetSchema(exact)
	require.NoError(t, err)
	schema, err := flight.DeserializeSchema(got.GetSchema(), memory.DefaultAllocator)
	require.NoError(t, err)
	require.Equal(t, "decimal(38, 0)", schema.Field(0).Type.String())
	_, err = ps.GetSchema(rig.ctx)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "GetSchema without the header after an exact prepare: %v", err)

	ticket := preparedCommandTicket(t, ps.Handle())
	reader, err := rig.client.DoGet(exact, ticket)
	require.NoError(t, err)
	require.Equal(t, "decimal(38, 0)", reader.Schema().Field(0).Type.String())
	require.Equal(t, []string{"100000000000000000000"}, columnStrings(t, reader))
	reader.Release()
	_, err = doGetAll(rig.client, rig.ctx, ticket)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "DoGet on the command without the header: %v", err)

	def, err := rig.client.Prepare(rig.ctx, sql)
	require.NoError(t, err)
	defer func() { require.NoError(t, def.Close(rig.ctx)) }()
	_, err = def.GetSchema(exact)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "GetSchema with exact after a default prepare: %v", err)
	_, err = doGetAll(rig.client, exact, preparedCommandTicket(t, def.Handle()))
	require.Equal(t, codes.InvalidArgument, status.Code(err), "DoGet on a default command with exact: %v", err)
}

// preparedCommandTicket is a DoGet ticket naming the prepared statement
// itself (CommandPreparedStatementQuery), which Flight SQL serves through
// DoGetPreparedStatement.
func preparedCommandTicket(t *testing.T, handle []byte) *flight.Ticket {
	t.Helper()
	cmd, err := anypb.New(&pb.CommandPreparedStatementQuery{PreparedStatementHandle: handle})
	require.NoError(t, err)
	raw, err := proto.Marshal(cmd)
	require.NoError(t, err)
	return &flight.Ticket{Ticket: raw}
}

// The bridge sends x-arc-decimals only to the routes that use it (the Arrow
// route and the schema route); the JSON route answers exact with 400, so SHOW
// and the catalog listings under exact would fail if it got there.
func TestFlightSQLBridgeSendsTheModeOnlyWhereItIsUsed1784(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{storedDecimals: true})
	exact := flightDecimals(rig.ctx, []string{decimalModeExact})

	for _, sql := range []string{"SHOW DATABASES", "SHOW TABLES"} {
		info, err := rig.client.Execute(exact, sql)
		require.NoError(t, err, sql)
		_, err = doGetAll(rig.client, exact, info.Endpoint[0].Ticket)
		require.NoError(t, err, sql)
	}
	ps, err := rig.client.Prepare(exact, "SELECT big FROM sample.dec")
	require.NoError(t, err)
	info, err := ps.Execute(exact)
	require.NoError(t, err)
	_, err = doGetAll(rig.client, exact, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	require.NoError(t, ps.Close(exact))
	for _, include := range []bool{false, true} {
		ti, err := rig.client.GetTables(exact, &flightsql.GetTablesOpts{IncludeSchema: include})
		require.NoError(t, err)
		_, err = doGetAll(rig.client, exact, ti.Endpoint[0].Ticket)
		require.NoError(t, err, "GetTables include_schema=%v", include)
	}

	seen := map[string]bool{}
	for _, req := range rig.bridge.all() {
		switch req.path {
		case "/api/v1/query/arrow", flightSQLSchemaPath:
			require.Equal(t, []string{decimalModeExact}, req.decimals, req.path)
			seen[req.path] = true
		default:
			require.Empty(t, req.decimals, "%s got x-arc-decimals", req.path)
			seen[req.path] = true
		}
	}
	require.True(t, seen["/api/v1/query/arrow"] && seen[flightSQLSchemaPath] && seen["/api/v1/query"], "routes reached: %v", seen)
}

// A stored DECIMAL(38,0) (ingest.decimal_columns) is reported by GetTables
// with schemas in the call's mode, in both metadata shapes, and streams its
// digits under exact. Only the wide scale-0 column changes.
func TestFlightSQLStoredDecimalFollowsTheMode1784(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{storedDecimals: true})
	want := map[string]map[string]string{
		decimalModeDefault: {"big": "int64", "small": "int64", "frac": "float64"},
		decimalModeExact:   {"big": "decimal(38, 0)", "small": "int64", "frac": "float64"},
	}
	for mode, types := range want {
		for _, shape := range []string{"", flightSQLMetadataSchemas} {
			ctx := flightDecimals(rig.ctx, []string{mode})
			if shape != "" {
				ctx = metadata.AppendToOutgoingContext(ctx, flightSQLMetadataHeader, shape)
			}
			table := "dec"
			ti, err := rig.client.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: true, TableNameFilterPattern: &table})
			require.NoError(t, err)
			reader, err := rig.client.DoGet(ctx, ti.Endpoint[0].Ticket)
			require.NoError(t, err)
			rows := 0
			for reader.Next() {
				rec := reader.RecordBatch()
				for i := 0; i < int(rec.NumRows()); i++ {
					rows++
					schema, err := flight.DeserializeSchema(rec.Column(4).(*array.Binary).Value(i), memory.DefaultAllocator)
					require.NoError(t, err)
					for name, typ := range types {
						idx := schema.FieldIndices(name)
						require.Len(t, idx, 1, name)
						require.Equal(t, typ, schema.Field(idx[0]).Type.String(), "%s mode %s shape %q", name, mode, shape)
					}
				}
			}
			require.NoError(t, reader.Err())
			reader.Release()
			require.Equal(t, 1, rows, "mode %s shape %q", mode, shape)
		}
	}

	exact := flightDecimals(rig.ctx, []string{decimalModeExact})
	ps, err := rig.client.Prepare(exact, "SELECT big FROM sample.dec")
	require.NoError(t, err)
	defer func() { require.NoError(t, ps.Close(exact)) }()
	require.Equal(t, "decimal(38, 0)", ps.DatasetSchema().Field(0).Type.String())
	info, err := ps.Execute(exact)
	require.NoError(t, err)
	reader, err := rig.client.DoGet(exact, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	require.Equal(t, []string{"100000000000000000000"}, columnStrings(t, reader))
	reader.Release()
}

// Flight SQL catalog calls ignore x-arc-decimals, except GetTables with
// schemas, which uses it and so refuses an invalid value, checked before any
// table is listed (here a filter that matches none).
func TestFlightSQLCatalogCallsAndTheDecimalsHeader1784(t *testing.T) {
	rig := newPreparedRig(t, preparedRigOptions{})
	bogus := flightDecimals(rig.ctx, []string{"bogus"})

	info, err := rig.client.GetCatalogs(bogus)
	require.NoError(t, err)
	_, err = doGetAll(rig.client, bogus, info.Endpoint[0].Ticket)
	require.NoError(t, err, "GetCatalogs ignores the header")
	info, err = rig.client.GetTableTypes(bogus)
	require.NoError(t, err)
	_, err = doGetAll(rig.client, bogus, info.Endpoint[0].Ticket)
	require.NoError(t, err, "GetTableTypes ignores the header")
	info, err = rig.client.GetTables(bogus, &flightsql.GetTablesOpts{})
	require.NoError(t, err)
	_, err = doGetAll(rig.client, bogus, info.Endpoint[0].Ticket)
	require.NoError(t, err, "GetTables without schemas ignores the header")

	none := "no_such_table"
	for _, shape := range []string{"", flightSQLMetadataSchemas} {
		ctx := bogus
		if shape != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, flightSQLMetadataHeader, shape)
		}
		info, err = rig.client.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: true, TableNameFilterPattern: &none})
		require.NoError(t, err)
		_, err = doGetAll(rig.client, ctx, info.Endpoint[0].Ticket)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "GetTables with schemas, shape %q: %v", shape, err)
	}
}
