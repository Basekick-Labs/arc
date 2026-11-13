//go:build duckdb_arrow

package api

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/basekick-labs/arc/internal/auth"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// GetSqlInfo is answered from what the server registers, with no query: the
// JDBC driver reads DatabaseMetaData from it and ADBC's GetInfo the server
// name and versions.

func startSqlInfoTestServer(t *testing.T, version string, authManager *auth.AuthManager) *flightsql.Client {
	t.Helper()
	qh := NewQueryHandler(nil, nil, zerolog.Nop(), 0, 0)
	if authManager != nil {
		qh.SetAuthAndRBAC(authManager, nil)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())
	s, err := NewFlightSQLServer(qh, FlightSQLServerConfig{Host: "127.0.0.1", Port: port, Version: version}, zerolog.Nop())
	require.NoError(t, err)
	startBridgeTestServer(t, s)
	client, err := flightsql.NewClient(s.flight.Addr().String(), nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Client.Close()) })
	return client
}

// readSqlInfo runs GetSqlInfo for ids and returns the answer by ID, with each
// value decoded from its dense-union member.
func readSqlInfo(ctx context.Context, t *testing.T, client *flightsql.Client, ids ...flightsql.SqlInfo) map[flightsql.SqlInfo]interface{} {
	t.Helper()
	info, err := client.GetSqlInfo(ctx, ids)
	require.NoError(t, err)
	require.Len(t, info.Endpoint, 1)
	reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
	require.NoError(t, err)
	defer reader.Release()
	got := map[flightsql.SqlInfo]interface{}{}
	for reader.Next() {
		record := reader.RecordBatch()
		names := record.Column(0).(*array.Uint32)
		values := record.Column(1).(*array.DenseUnion)
		for row := 0; row < int(record.NumRows()); row++ {
			offset := int(values.ValueOffset(row))
			var value interface{}
			switch child := values.Field(values.ChildID(row)).(type) {
			case *array.String:
				value = child.Value(offset)
			case *array.Boolean:
				value = child.Value(offset)
			case *array.Int64:
				value = child.Value(offset)
			case *array.Int32:
				value = child.Value(offset)
			default:
				t.Fatalf("SqlInfo %d: unexpected value type %T", names.Value(row), child)
			}
			id := flightsql.SqlInfo(names.Value(row))
			_, duplicate := got[id]
			require.False(t, duplicate, "SqlInfo %d answered twice", id)
			got[id] = value
		}
	}
	require.NoError(t, reader.Err())
	return got
}

func TestFlightSQLGetSqlInfoAnswersEveryRegisteredValue(t *testing.T) {
	client := startSqlInfoTestServer(t, "26.12.1", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// An empty request is a request for everything.
	require.Equal(t, map[flightsql.SqlInfo]interface{}{
		flightsql.SqlInfoFlightSqlServerName:          "Arc",
		flightsql.SqlInfoFlightSqlServerVersion:       "26.12.1",
		flightsql.SqlInfoFlightSqlServerArrowVersion:  arrow.PkgVersion,
		flightsql.SqlInfoFlightSqlServerReadOnly:      true,
		flightsql.SqlInfoFlightSqlServerSql:           true,
		flightsql.SqlInfoFlightSqlServerSubstrait:     false,
		flightsql.SqlInfoFlightSqlServerTransaction:   int32(flightsql.SqlTransactionNone),
		flightsql.SqlInfoFlightSqlServerCancel:        false,
		flightsql.SqlInfoFlightSqlServerBulkIngestion: false,
		flightsql.SqlInfoDDLCatalog:                   false,
		flightsql.SqlInfoDDLSchema:                    false,
		flightsql.SqlInfoDDLTable:                     false,
		flightsql.SqlInfoIdentifierQuoteChar:          `"`,
		flightsql.SqlInfoSearchStringEscape:           `\`,
		flightsql.SqlInfoCatalogTerm:                  "database",
		flightsql.SqlInfoCatalogAtStart:               true,

		flightsql.SqlInfoSupportsColumnAliasing:        true,
		flightsql.SqlInfoNullPlusNullIsNull:            true,
		flightsql.SqlInfoSupportsTableCorrelationNames: true,
		flightsql.SqlInfoSupportsExpressionsInOrderBy:  true,
		flightsql.SqlInfoSupportsOrderByUnrelated:      true,
		flightsql.SqlInfoSupportsLikeEscapeClause:      true,
		flightsql.SqlInfoCorrelatedSubqueriesSupported: true,
		// GROUP BY unrelated and beyond select; subqueries in comparisons,
		// EXISTS, IN and quantified comparisons; UNION and UNION ALL.
		flightsql.SqlInfoSupportedGroupBy:    int32(0b11),
		flightsql.SqlInfoSupportedSubqueries: int32(0b1111),
		flightsql.SqlInfoSupportedUnions:     int32(0b11),
		// Forward-only result sets; isolation level none.
		flightsql.SqlInfoSupportedResultSetTypes:              int32(0b10),
		flightsql.SqlInfoSupportedTransactionsIsolationlevels: int32(0b1),
		// SQL_FULL_OUTER_JOINS, an ordinal.
		flightsql.SqlInfoOuterJoinsSupportLevel: int32(2),
		// The ODBC minimum grammar only.
		flightsql.SqlInfoSupportedGrammar: int32(0b1),
	}, readSqlInfo(ctx, t, client))
}

// ADBC's GetInfo asks for the Substrait version range too. The IDs Arc has
// are answered and the rest left out, rather than the whole call failing
// (the base server answers NotFound for the first ID it lacks).
func TestFlightSQLGetSqlInfoLeavesOutIDsItHasNoValueFor(t *testing.T) {
	client := startSqlInfoTestServer(t, "26.12.1", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.Equal(t, map[flightsql.SqlInfo]interface{}{
		flightsql.SqlInfoFlightSqlServerName:         "Arc",
		flightsql.SqlInfoFlightSqlServerVersion:      "26.12.1",
		flightsql.SqlInfoFlightSqlServerArrowVersion: arrow.PkgVersion,
		flightsql.SqlInfoFlightSqlServerSql:          true,
		flightsql.SqlInfoFlightSqlServerSubstrait:    false,
	}, readSqlInfo(ctx, t, client,
		flightsql.SqlInfoFlightSqlServerName,
		flightsql.SqlInfoFlightSqlServerVersion,
		flightsql.SqlInfoFlightSqlServerArrowVersion,
		flightsql.SqlInfoFlightSqlServerSql,
		flightsql.SqlInfoFlightSqlServerSubstrait,
		flightsql.SqlInfoFlightSqlServerSubstraitMinVersion,
		flightsql.SqlInfoFlightSqlServerSubstraitMaxVersion,
	))

	// Only IDs Arc has no value for: an empty answer, not every value.
	require.Empty(t, readSqlInfo(ctx, t, client,
		flightsql.SqlInfoFlightSqlServerSubstraitMinVersion,
		flightsql.SqlInfoFlightSqlServerSubstraitMaxVersion,
	))
}

// The JDBC driver and ADBC ask on every connection, so calls overlap; under
// -race this would catch shared state written per call.
func TestFlightSQLGetSqlInfoAnswersConcurrentCalls(t *testing.T) {
	client := startSqlInfoTestServer(t, "26.12.1", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := client.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerName})
			if !assert.NoError(t, err) {
				return
			}
			reader, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
			if !assert.NoError(t, err) {
				return
			}
			defer reader.Release()
			rows := int64(0)
			for reader.Next() {
				rows += reader.RecordBatch().NumRows()
			}
			assert.NoError(t, reader.Err())
			assert.EqualValues(t, 1, rows)
		}()
	}
	wg.Wait()
}

func TestFlightSQLGetSqlInfoOmitsAnUnsetVersion(t *testing.T) {
	client := startSqlInfoTestServer(t, "", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	got := readSqlInfo(ctx, t, client)
	require.NotContains(t, got, flightsql.SqlInfoFlightSqlServerVersion)
	require.Equal(t, "Arc", got[flightsql.SqlInfoFlightSqlServerName])
}

func TestFlightSQLGetSqlInfoRequiresAToken(t *testing.T) {
	authManager, err := auth.NewAuthManager(filepath.Join(t.TempDir(), "auth.db"), time.Minute, 100, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = authManager.Close() })
	token, err := authManager.CreateToken(context.Background(), "sqlinfo", "", "read", nil)
	require.NoError(t, err)
	client := startSqlInfoTestServer(t, "26.12.1", authManager)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = client.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerName})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	ctx, err = client.Client.AuthenticateBasicToken(ctx, "", token)
	require.NoError(t, err)
	require.Equal(t, map[flightsql.SqlInfo]interface{}{
		flightsql.SqlInfoFlightSqlServerName: "Arc",
	}, readSqlInfo(ctx, t, client, flightsql.SqlInfoFlightSqlServerName))
}
