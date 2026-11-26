package api

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Prepared statements without parameters (#1645). The Arrow Flight SQL JDBC
// driver prepares every statement, even a plain Statement.executeQuery, and
// ADBC prepares before each execute when the server can.
//
// The handle carries the SQL, as the statement ticket does, so the server
// keeps no state and Close has nothing to free. Prepare reads the result
// schema by binding the statement on the bridge-only flightSQLSchemaPath
// route, which runs nothing: the execute is the one query, counted, audited
// and registered once. Executing a prepared statement is a plain statement:
// GetFlightInfo answers a statement ticket and DoGet runs it as
// DoGetStatement does.

// flightSQLSchemaPath is the bridge route that answers a statement's result
// schema without running it (flight_sql_schema_route.go). It is registered on
// the bridge only, never on the main app.
const flightSQLSchemaPath = "/internal/flight-sql/schema"

// flightPreparedStatementVersion is the handle format version. A handle of
// another version is refused rather than guessed at.
const flightPreparedStatementVersion = 1

// flightPreparedStatement is a prepared statement's handle.
type flightPreparedStatement struct {
	Version int    `json:"v"`
	SQL     string `json:"sql"`
	// Database is the x-arc-database call header the prepare saw, as a
	// statement ticket carries the one GetFlightInfo saw.
	Database    string `json:"database,omitempty"`
	DecimalMode string `json:"decimal_mode,omitempty"`
}

// flightPivotPattern finds a PIVOT (or its alias PIVOT_WIDER), statement-
// leading or in a FROM clause, but not UNPIVOT, in literal- and
// comment-masked SQL. A PIVOT without an IN list finds its columns by running
// a DISTINCT scan while DuckDB prepares it, so a prepare would run a query.
var flightPivotPattern = regexp.MustCompile(`(?i)\bPIVOT(?:_WIDER)?\b`)

// flightPivotRefusal is the PIVOT refusal. A JDBC client cannot run a
// statement unprepared (the driver prepares everything), so it is pointed at
// conditional aggregation, which prepares.
const flightPivotRefusal = "a PIVOT statement cannot be prepared: its columns are known only once it runs. " +
	"Write it with conditional aggregation instead, one column per value, e.g. sum(x) FILTER (WHERE k = 'v') AS v; " +
	"a client that can run statements unprepared (ADBC) can also run the PIVOT that way"

// flightMaskPlaceholder finds the placeholders MaskStringLiterals puts where
// a literal or quoted identifier was. They are word characters, so
// `PIVOT"c"` masks to `PIVOT__IDENT_0__`, where \b finds no boundary; DuckDB
// still reads it as PIVOT "c". hasFlightPivot pads them with spaces first.
var flightMaskPlaceholder = regexp.MustCompile(`__(?:STR|IDENT)_\d+__`)

// hasFlightPivot reports whether sql has a PIVOT outside its string literals,
// quoted identifiers and comments.
func hasFlightPivot(sql string) bool {
	features := scanSQLFeatures(sql)
	masked, _ := sqlutil.MaskStringLiterals(sql, features.hasQuotes)
	masked = stripSQLComments(masked, features.hasDashComment || features.hasBlockComment)
	if features.hasQuotes {
		masked = flightMaskPlaceholder.ReplaceAllString(masked, " $0 ")
	}
	return flightPivotPattern.MatchString(masked)
}

func (s *flightSQLService) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (flightsql.ActionCreatePreparedStatementResult, error) {
	if err := flightFreshnessRefusal(ctx); err != nil { // #1760
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}
	if len(req.GetTransactionId()) != 0 {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Error(codes.Unimplemented, "Flight SQL transactions are not supported")
	}
	database, err := flightDatabase(ctx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}
	decimalMode, _, err := flightSQLDecimalMode(ctx)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}
	schema, err := s.preparedSchema(ctx, req.GetQuery(), database)
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, err
	}
	storedDecimalMode := ""
	if decimalMode == decimalModeExact {
		storedDecimalMode = decimalModeExact
	}
	handle, err := json.Marshal(flightPreparedStatement{Version: flightPreparedStatementVersion, SQL: req.GetQuery(), Database: database, DecimalMode: storedDecimalMode})
	if err != nil {
		return flightsql.ActionCreatePreparedStatementResult{}, status.Errorf(codes.Internal, "encode prepared statement handle: %v", err)
	}
	// Said explicitly: a client that is not told reads a schema without
	// columns as an update. JDBC 19.0.0 still does, as it ignores the field
	// (apache/arrow-java#1064 makes later releases honour it).
	isUpdate := false
	return flightsql.ActionCreatePreparedStatementResult{Handle: handle, DatasetSchema: schema, IsUpdate: &isUpdate}, nil
}

// preparedSchema is the result schema of sql, read without running it. Every
// refusal that the client can work around by running the statement
// unprepared is Unimplemented, the code ADBC falls back on.
func (s *flightSQLService) preparedSchema(ctx context.Context, sql, database string) (*arrow.Schema, error) {
	if err := ValidateSQLRequest(sql); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// A node that does not run queries forwards them (a compactor); a bind
	// cannot be forwarded, so it prepares nothing, SHOW included.
	if router := s.queryHandler.router; router != nil && !router.CanRouteLocally(false) {
		return nil, status.Error(codes.Unimplemented, "prepared statements are not served by a node that does not run queries; run the statement unprepared")
	}
	if schema, ok := s.showSchema(sql); ok {
		return schema, nil
	}
	if hasFlightPivot(sql) {
		return nil, status.Error(codes.Unimplemented, flightPivotRefusal)
	}

	resp, err := s.query(ctx, flightSQLSchemaPath, sql, s.token(ctx), database)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	reader, err := ipc.NewReader(resp.Body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read prepared statement schema: %v", err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}

// showSchema is the schema of a SHOW statement Arc answers itself, the one
// DoGetStatement builds its result with. ok is false for any other statement.
func (s *flightSQLService) showSchema(sql string) (schema *arrow.Schema, ok bool) {
	normalised := normalizeSQLForShow(sql)
	var columns []string
	switch {
	case showDatabasesPattern.MatchString(normalised):
		columns = []string{"database"}
		// As handleShowDatabases answers it.
		if s.queryHandler.tieringManager != nil {
			columns = append(columns, "tier")
		}
	case showTablesPattern.MatchString(normalised):
		columns = []string{"database", "table_name", "storage_path", "file_count", "total_size_mb"}
	default:
		return nil, false
	}
	schema, err := flightShowSchema(columns)
	return schema, err == nil
}

// decodeFlightPreparedStatement decodes a prepared statement handle. The
// handle is the client's to send back, so its database is checked like the
// header and its SQL is validated again where it is used.
func decodeFlightPreparedStatement(handle []byte) (flightPreparedStatement, error) {
	var statement flightPreparedStatement
	if err := json.Unmarshal(handle, &statement); err != nil || statement.Version != flightPreparedStatementVersion {
		return flightPreparedStatement{}, status.Error(codes.InvalidArgument, "invalid prepared statement handle")
	}
	if err := validateHeaderDatabase(statement.Database); err != nil {
		return flightPreparedStatement{}, status.Error(codes.InvalidArgument, "invalid database in prepared statement handle")
	}
	if statement.DecimalMode != "" && statement.DecimalMode != decimalModeDefault && statement.DecimalMode != decimalModeExact {
		return flightPreparedStatement{}, status.Error(codes.InvalidArgument, "invalid decimal mode in prepared statement handle")
	}
	return statement, nil
}

func (s *flightSQLService) GetSchemaPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, _ *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if err := flightFreshnessRefusal(ctx); err != nil { // #1760
		return nil, err
	}
	statement, err := decodeFlightPreparedStatement(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}
	database, err := flightStatementDatabase(ctx, statement.Database)
	if err != nil {
		return nil, err
	}
	if _, err := flightStatementDecimalMode(ctx, statement.DecimalMode); err != nil {
		return nil, err
	}
	schema, err := s.preparedSchema(ctx, statement.SQL, database)
	if err != nil {
		return nil, err
	}
	return &flight.SchemaResult{Schema: flight.SerializeSchema(schema, memory.DefaultAllocator)}, nil
}

// GetFlightInfoPreparedStatement answers an ordinary statement ticket, so the
// statement executes as GetFlightInfoStatement's do.
func (s *flightSQLService) GetFlightInfoPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if err := flightFreshnessRefusal(ctx); err != nil { // #1760
		return nil, err
	}
	statement, err := decodeFlightPreparedStatement(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, err
	}
	if err := ValidateSQLRequest(statement.SQL); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	database, err := flightStatementDatabase(ctx, statement.Database)
	if err != nil {
		return nil, err
	}
	if _, err := flightStatementDecimalMode(ctx, statement.DecimalMode); err != nil {
		return nil, err
	}
	return flightStatementInfo(desc, statement.SQL, database, statement.DecimalMode)
}

// DoGetPreparedStatement runs the statement as DoGetStatement runs a ticket.
func (s *flightSQLService) DoGetPreparedStatement(ctx context.Context, cmd flightsql.PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if err := flightFreshnessRefusal(ctx); err != nil { // #1760
		return nil, nil, err
	}
	statement, err := decodeFlightPreparedStatement(cmd.GetPreparedStatementHandle())
	if err != nil {
		return nil, nil, err
	}
	handle, err := json.Marshal(flightStatement{SQL: statement.SQL, Database: statement.Database, DecimalMode: statement.DecimalMode})
	if err != nil {
		return nil, nil, status.Errorf(codes.Internal, "encode statement ticket: %v", err)
	}
	return s.DoGetStatement(ctx, flightStatementHandle(handle))
}

// ClosePreparedStatement frees nothing: the handle holds the whole statement.
func (s *flightSQLService) ClosePreparedStatement(context.Context, flightsql.ActionClosePreparedStatementRequest) error {
	return nil
}

func (s *flightSQLService) DoPutPreparedStatementQuery(context.Context, flightsql.PreparedStatementQuery, flight.MessageReader, flight.MetadataWriter) ([]byte, error) {
	return nil, status.Error(codes.Unimplemented, "Arc prepared statements take no parameters")
}

func (s *flightSQLService) DoPutPreparedStatementUpdate(context.Context, flightsql.PreparedStatementUpdate, flight.MessageReader) (int64, error) {
	return 0, status.Error(codes.InvalidArgument, "Arc runs queries only, so a prepared statement is never an update. "+
		"A client that executes one as an update read its result as having no columns, which is what a query on a measurement with no data yet has: execute it as a query")
}

// flightStatementHandle is a statement ticket's handle, for DoGetStatement.
type flightStatementHandle []byte

func (h flightStatementHandle) GetStatementHandle() []byte { return h }
