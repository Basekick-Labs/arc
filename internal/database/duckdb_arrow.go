//go:build duckdb_arrow

package database

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	sqlutil "github.com/basekick-labs/arc/internal/sql"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/duckdb/duckdb-go/v2"
)

func init() {
	ArrowEnabled = true
}

// arrowQueryOnConn executes a query via the Arrow API on a raw driver connection.
func arrowQueryOnConn(ctx context.Context, driverConn any, query string) (array.RecordReader, error) {
	dc, ok := driverConn.(driver.Conn)
	if !ok {
		return nil, fmt.Errorf("connection does not implement driver.Conn")
	}
	arrowAPI, err := duckdb.NewArrowFromConn(dc)
	if err != nil {
		return nil, fmt.Errorf("failed to create Arrow interface: %w", err)
	}
	return arrowAPI.QueryContext(ctx, query)
}

// ArrowQueryContext executes a query using DuckDB's native Arrow API,
// returning an array.RecordReader that yields Arrow record batches directly
// from DuckDB's internal columnar chunks — no row-by-row scanning.
//
// The caller MUST close both resources when done:
//  1. reader.Release() — releases Arrow batches and closes underlying rows
//  2. conn.Close() — returns the connection to the pool
func (d *DuckDB) ArrowQueryContext(ctx context.Context, query string) (array.RecordReader, *sql.Conn, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	var reader array.RecordReader
	err = conn.Raw(func(driverConn any) error {
		var rawErr error
		reader, rawErr = arrowQueryOnConn(ctx, driverConn, query)
		return rawErr
	})

	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("arrow query failed: %w", err)
	}

	d.logger.Debug().
		Str("query", sqlutil.ForLog(query)).
		Msg("Arrow query executed")

	return reader, conn, nil
}

// ErrSchemaBind wraps DuckDB's error for a statement DescribeArrowSchema could
// not bind because of the statement itself: a syntax error, an unknown table
// or column, a type mismatch (schemaBindErrorTypes).
var ErrSchemaBind = errors.New("the statement does not bind")

// ErrSchemaStorage wraps DuckDB's error for a bind that failed reading
// storage (the files' listing or Parquet footers): an IO, HTTP or network
// error. The statement may be right; a retry may bind it. Its text has no
// quote: Arc's error logging masks quoted spans.
var ErrSchemaStorage = errors.New("storage could not be read during the bind")

// ErrSchemaMemory wraps DuckDB's out-of-memory error during a bind. A retry
// may bind it once other queries have finished.
var ErrSchemaMemory = errors.New("out of memory during the bind")

// schemaBindErrorTypes are the DuckDB error types that blame the statement.
// A bind error of another type wraps none of ErrSchemaBind, ErrSchemaStorage
// and ErrSchemaMemory, unless schemaStorageErrorTypes has it or it is out of
// memory. NotImplemented is the statement's: DuckDB has no implementation
// for what it asks, and running it unprepared would fail the same way.
var schemaBindErrorTypes = map[duckdb.ErrorType]bool{
	duckdb.ErrorTypeParser:               true,
	duckdb.ErrorTypeSyntax:               true,
	duckdb.ErrorTypeBinder:               true,
	duckdb.ErrorTypeCatalog:              true,
	duckdb.ErrorTypeConversion:           true,
	duckdb.ErrorTypeMismatchType:         true,
	duckdb.ErrorTypeInvalidType:          true,
	duckdb.ErrorTypeInvalidInput:         true,
	duckdb.ErrorTypeOutOfRange:           true,
	duckdb.ErrorTypeDivideByZero:         true,
	duckdb.ErrorTypeDecimal:              true,
	duckdb.ErrorTypeExpression:           true,
	duckdb.ErrorTypeNotImplemented:       true,
	duckdb.ErrorTypeParameterNotResolved: true,
	duckdb.ErrorTypeParameterNotAllowed:  true,
	duckdb.ErrorTypePermission:           true,
	duckdb.ErrorTypeMissingExtension:     true,
	duckdb.ErrorTypeAutoLoad:             true,
}

// schemaStorageErrorTypes are the DuckDB error types of a bind that could not
// read what the statement names.
var schemaStorageErrorTypes = map[duckdb.ErrorType]bool{
	duckdb.ErrorTypeIO:         true,
	duckdb.ErrorTypeHTTP:       true,
	duckdb.ErrorTypeNetwork:    true,
	duckdb.ErrorTypeConnection: true,
}

// corruptParquetMessage reports whether an Invalid Input error is DuckDB's
// Parquet reader refusing a file in storage. Those texts were measured with
// the duckdb-go Arc ships: a damaged file is not the statement's fault, and
// a retry does not help, so it is left unclassified (the route's 500, which
// logs DuckDB's text and answers without it). Matched from the start of
// DuckDB's message, as duckdbMessage does: a user's error() text cannot
// start there, and forging one would only turn a 400 into a 500.
func corruptParquetMessage(msg string) bool {
	return strings.HasPrefix(msg, "Invalid Input Error: No magic bytes found at end of file ") ||
		strings.HasPrefix(msg, "Invalid Input Error: Footer length error in file ") ||
		(strings.HasPrefix(msg, "Invalid Input Error: File ") && strings.Contains(msg, " too small to be a Parquet file"))
}

// bindError wraps a PrepareContext error by what its DuckDB type blames.
func bindError(err error) error {
	var de *duckdb.Error
	if errors.As(err, &de) {
		switch {
		case de.Type == duckdb.ErrorTypeInvalidInput && corruptParquetMessage(de.Msg):
		case de.Type == duckdb.ErrorTypeOutOfMemory:
			return fmt.Errorf("%w: %w", ErrSchemaMemory, err)
		case schemaBindErrorTypes[de.Type]:
			return fmt.Errorf("%w: %w", ErrSchemaBind, err)
		case schemaStorageErrorTypes[de.Type]:
			return fmt.Errorf("%w: %w", ErrSchemaStorage, err)
		}
	}
	return fmt.Errorf("bind: %w", err)
}

// ErrSchemaParameters refuses a statement with parameter placeholders:
// DescribeArrowSchema describes statements that run as they are written.
var ErrSchemaParameters = errors.New("parameters are not supported")

// ErrSchemaUnsupported wraps every DescribeArrowSchema failure after the
// statement bound, apart from the context ending: the statement is valid, but
// its schema cannot be given without running it.
var ErrSchemaUnsupported = errors.New("the result schema cannot be described without running the statement")

// DescribeArrowSchema returns the Arrow schema query's result would have,
// without running it. DuckDB binds the statement, which lists the files it
// reads and their Parquet footers but scans nothing, and the column names and
// types the bind produced become a `SELECT NULL::<type> AS "<name>", …
// LIMIT 0`, which DuckDB's own Arrow export turns into the schema. That is the
// export ArrowQueryContext streams, on a connection of the same pool, so
// duplicate names, nested types and decimals come out as a query's result
// would. The statement is never wrapped in a `SELECT * FROM (…) LIMIT 0`: that
// renames duplicate columns, breaks on a trailing `;` or `--` comment, and
// runs a query.
//
// A statement that does not bind is refused with DuckDB's error wrapped in
// ErrSchemaBind when the statement is at fault, in ErrSchemaStorage when
// reading storage failed and in ErrSchemaMemory when memory ran out
// (bindError), and one with parameters with ErrSchemaParameters. Once it has
// bound, a failure wraps ErrSchemaUnsupported. When ctx has ended, ctx's
// error is returned instead.
//
// query must be one statement, as the query path's SQL validation requires:
// preparing a string of several, duckdb-go runs every statement but the last.
//
// One pool connection serves the whole call: acquiring a second one while
// holding the first could wait forever on a small pool.
func (d *DuckDB) DescribeArrowSchema(ctx context.Context, query string) (*arrow.Schema, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire connection: %w", err)
	}
	defer conn.Close()

	var schema *arrow.Schema
	err = conn.Raw(func(driverConn any) error {
		var rawErr error
		schema, rawErr = describeArrowSchemaOnConn(ctx, driverConn, query)
		return rawErr
	})
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func describeArrowSchemaOnConn(ctx context.Context, driverConn any, query string) (schema *arrow.Schema, err error) {
	preparer, ok := driverConn.(driver.ConnPrepareContext)
	if !ok {
		return nil, fmt.Errorf("connection does not implement driver.ConnPrepareContext")
	}
	execer, ok := driverConn.(driver.ExecerContext)
	if !ok {
		return nil, fmt.Errorf("connection does not implement driver.ExecerContext")
	}
	// No optimizer during the bind: it would fold constant expressions, which
	// is work no query is counted for. The binder still folds constant
	// arguments of functions; the bind slots bound that. The setting is per
	// connection, so it is restored before the connection goes back to the
	// pool, and a connection it cannot be restored on is discarded.
	//
	// The restore is registered first: a disable that fails after DuckDB
	// applied it (an interrupt landing late) must not leave it off.
	defer func() {
		if _, restoreErr := execer.ExecContext(context.WithoutCancel(ctx), "PRAGMA enable_optimizer", nil); restoreErr != nil {
			schema, err = nil, fmt.Errorf("restore optimizer: %w: %w", restoreErr, driver.ErrBadConn)
		}
	}()
	if _, err := execer.ExecContext(ctx, "PRAGMA disable_optimizer", nil); err != nil {
		return nil, describeFailure(ctx, err)
	}
	stmt, err := preparer.PrepareContext(ctx, query)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, bindError(err)
	}
	columns, err := schemaQueryFromStatement(stmt)
	// Closed before anything else runs on the connection.
	if closeErr := stmt.Close(); closeErr != nil {
		return nil, describeFailure(ctx, closeErr)
	}
	// A statement with parameters, or one that returns no query result, is
	// refused as it is; a type the statement's column types cannot spell
	// is asked of DESCRIBE.
	if errors.Is(err, ErrSchemaParameters) || errors.Is(err, ErrSchemaUnsupported) {
		return nil, err
	}
	if err != nil {
		if columns, err = schemaQueryFromDescribe(ctx, driverConn, query); err != nil {
			return nil, describeFailure(ctx, err)
		}
	}

	reader, err := arrowQueryOnConn(ctx, driverConn, columns)
	if err != nil {
		return nil, describeFailure(ctx, err)
	}
	defer reader.Release()
	return reader.Schema(), nil
}

// describeFailure is the error a failure after the bind is returned as.
func describeFailure(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("%w: %w", ErrSchemaUnsupported, err)
}

// schemaQueryFromStatement builds the `SELECT NULL::<type> AS "<name>", …
// LIMIT 0` for a bound statement from its column types.
func schemaQueryFromStatement(stmt driver.Stmt) (string, error) {
	s, ok := stmt.(*duckdb.Stmt)
	if !ok {
		return "", fmt.Errorf("unexpected statement type %T", stmt)
	}
	if s.NumInput() > 0 {
		return "", ErrSchemaParameters
	}
	kind, err := s.StatementType()
	if err != nil {
		return "", err
	}
	if kind != duckdb.STATEMENT_TYPE_SELECT && kind != duckdb.STATEMENT_TYPE_EXPLAIN {
		return "", fmt.Errorf("%w: statement type %d returns no query result", ErrSchemaUnsupported, kind)
	}
	n, err := s.ColumnCount()
	if err != nil {
		return "", err
	}
	exprs := make([]string, n)
	for i := range n {
		name, err := s.ColumnName(i)
		if err != nil {
			return "", err
		}
		t, err := s.ColumnType(i)
		if err != nil {
			return "", err
		}
		sqlType := ""
		if t != duckdb.TYPE_SQLNULL {
			info, err := s.ColumnTypeInfo(i)
			if err != nil {
				return "", err
			}
			var ok bool
			if sqlType, ok = duckDBTypeSQL(info); !ok {
				return "", fmt.Errorf("column %d: no SQL spelling for its type", i)
			}
		}
		exprs[i] = nullColumn(name, sqlType)
	}
	return selectNullColumns(exprs)
}

// schemaQueryFromDescribe builds the same query from DESCRIBE's column names
// and types, for a type schemaQueryFromStatement cannot spell (a UNION, an
// extension type). DESCRIBE binds the statement again and runs nothing.
func schemaQueryFromDescribe(ctx context.Context, driverConn any, query string) (string, error) {
	reader, err := arrowQueryOnConn(ctx, driverConn, "DESCRIBE "+query)
	if err != nil {
		return "", err
	}
	defer reader.Release()
	nameCol := reader.Schema().FieldIndices("column_name")
	typeCol := reader.Schema().FieldIndices("column_type")
	if len(nameCol) != 1 || len(typeCol) != 1 {
		return "", fmt.Errorf("DESCRIBE answered columns %v", reader.Schema().Fields())
	}
	var exprs []string
	for reader.Next() {
		batch := reader.RecordBatch()
		names, ok1 := batch.Column(nameCol[0]).(*array.String)
		types, ok2 := batch.Column(typeCol[0]).(*array.String)
		if !ok1 || !ok2 {
			return "", fmt.Errorf("DESCRIBE answered column types %s, %s", batch.Column(nameCol[0]).DataType(), batch.Column(typeCol[0]).DataType())
		}
		for row := 0; row < int(batch.NumRows()); row++ {
			sqlType := types.Value(row)
			if sqlType == `"NULL"` || sqlType == "NULL" {
				sqlType = ""
			}
			exprs = append(exprs, nullColumn(names.Value(row), sqlType))
		}
	}
	if err := reader.Err(); err != nil {
		return "", err
	}
	return selectNullColumns(exprs)
}

// nullColumn is one column of the schema query; sqlType "" is DuckDB's NULL
// type, which has no spelling a cast accepts.
func nullColumn(name, sqlType string) string {
	expr := "NULL"
	if sqlType != "" {
		expr += "::" + sqlType
	}
	return expr + " AS " + quoteIdentifier(name)
}

func selectNullColumns(exprs []string) (string, error) {
	if len(exprs) == 0 {
		return "", errors.New("the statement has no result columns")
	}
	return "SELECT " + strings.Join(exprs, ", ") + " LIMIT 0", nil
}

// duckDBPrimitiveTypes spells the types that take no parameters.
var duckDBPrimitiveTypes = map[duckdb.Type]string{
	duckdb.TYPE_BOOLEAN:      "BOOLEAN",
	duckdb.TYPE_TINYINT:      "TINYINT",
	duckdb.TYPE_SMALLINT:     "SMALLINT",
	duckdb.TYPE_INTEGER:      "INTEGER",
	duckdb.TYPE_BIGINT:       "BIGINT",
	duckdb.TYPE_UTINYINT:     "UTINYINT",
	duckdb.TYPE_USMALLINT:    "USMALLINT",
	duckdb.TYPE_UINTEGER:     "UINTEGER",
	duckdb.TYPE_UBIGINT:      "UBIGINT",
	duckdb.TYPE_FLOAT:        "FLOAT",
	duckdb.TYPE_DOUBLE:       "DOUBLE",
	duckdb.TYPE_TIMESTAMP:    "TIMESTAMP",
	duckdb.TYPE_DATE:         "DATE",
	duckdb.TYPE_TIME:         "TIME",
	duckdb.TYPE_INTERVAL:     "INTERVAL",
	duckdb.TYPE_HUGEINT:      "HUGEINT",
	duckdb.TYPE_UHUGEINT:     "UHUGEINT",
	duckdb.TYPE_VARCHAR:      "VARCHAR",
	duckdb.TYPE_BLOB:         "BLOB",
	duckdb.TYPE_TIMESTAMP_S:  "TIMESTAMP_S",
	duckdb.TYPE_TIMESTAMP_MS: "TIMESTAMP_MS",
	duckdb.TYPE_TIMESTAMP_NS: "TIMESTAMP_NS",
	duckdb.TYPE_UUID:         "UUID",
	duckdb.TYPE_BIT:          "BIT",
	duckdb.TYPE_TIME_TZ:      "TIMETZ",
	duckdb.TYPE_TIMESTAMP_TZ: "TIMESTAMPTZ",
}

// duckDBTypeSQL spells a column type as SQL a cast accepts. ok is false for a
// type it has no spelling for: a UNION, an alias other than JSON (an
// extension type), or a struct with an unnamed field.
func duckDBTypeSQL(info duckdb.TypeInfo) (string, bool) {
	if alias := info.Alias(); alias != "" {
		if strings.EqualFold(alias, "JSON") && info.InternalType() == duckdb.TYPE_VARCHAR {
			return "JSON", true
		}
		return "", false
	}
	if name, ok := duckDBPrimitiveTypes[info.InternalType()]; ok {
		return name, true
	}
	switch details := info.Details().(type) {
	case *duckdb.DecimalDetails:
		return fmt.Sprintf("DECIMAL(%d,%d)", details.Width, details.Scale), true
	case *duckdb.EnumDetails:
		values := make([]string, len(details.Values))
		for i, v := range details.Values {
			values[i] = "'" + escapeSQLString(v) + "'"
		}
		return "ENUM(" + strings.Join(values, ", ") + ")", true
	case *duckdb.ListDetails:
		child, ok := duckDBTypeSQL(details.Child)
		return child + "[]", ok
	case *duckdb.ArrayDetails:
		child, ok := duckDBTypeSQL(details.Child)
		return fmt.Sprintf("%s[%d]", child, details.Size), ok
	case *duckdb.MapDetails:
		key, ok1 := duckDBTypeSQL(details.Key)
		value, ok2 := duckDBTypeSQL(details.Value)
		return "MAP(" + key + ", " + value + ")", ok1 && ok2
	case *duckdb.StructDetails:
		fields := make([]string, len(details.Entries))
		for i, entry := range details.Entries {
			field, ok := duckDBTypeSQL(entry.Info())
			if !ok || entry.Name() == "" {
				return "", false
			}
			fields[i] = quoteIdentifier(entry.Name()) + " " + field
		}
		return "STRUCT(" + strings.Join(fields, ", ") + ")", true
	default:
		return "", false
	}
}

// quoteIdentifier double-quotes name for DuckDB.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// ArrowQueryWithProfileContext executes a query with DuckDB profiling enabled,
// returning Arrow record batches and a QueryProfile with timing breakdown.
//
// The caller MUST close both resources when done:
//  1. reader.Release() — releases Arrow batches and closes underlying rows
//  2. conn.Close() — returns the connection to the pool
func (d *DuckDB) ArrowQueryWithProfileContext(ctx context.Context, query string) (array.RecordReader, *sql.Conn, *QueryProfile, error) {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to acquire connection: %w", err)
	}

	// Create temp file for profiling output. MUST land inside the DuckDB
	// sandbox's allowed_directories — d.config.TempDirectory is always
	// allowlisted, os.TempDir() is not. Empty TempDirectory short-circuits
	// to the non-profile path without attempting the file create (CreateTemp
	// with empty dir falls back to os.TempDir which the sandbox rejects).
	var tmpFile *os.File
	if d.config.TempDirectory == "" {
		d.logger.Debug().Msg("Profile mode requested but TempDirectory is unset; returning Arrow result without profile data")
	} else {
		var err error
		tmpFile, err = os.CreateTemp(d.config.TempDirectory, "duckdb_profile_*.json")
		if err != nil {
			d.logger.Warn().Err(err).Str("temp_dir", d.config.TempDirectory).Msg("Failed to create profile temp file; falling back to non-profile Arrow path")
			tmpFile = nil
		}
	}
	if tmpFile == nil {
		// Fall back to non-profile Arrow query
		var reader array.RecordReader
		rawErr := conn.Raw(func(driverConn any) error {
			var err error
			reader, err = arrowQueryOnConn(ctx, driverConn, query)
			return err
		})
		if rawErr != nil {
			conn.Close()
			return nil, nil, nil, rawErr
		}
		return reader, conn, nil, nil
	}
	profilePath := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(profilePath)

	// Enable profiling on this specific connection
	if _, err := conn.ExecContext(ctx, "PRAGMA enable_profiling='json'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to enable profiling")
	}
	// Escape profilePath against the same SQL-injection surface duckdb.go
	// guards (operator's TempDirectory could contain a single quote).
	// ToSlash so Windows backslashes from os.CreateTemp match the sandbox
	// allowlist (allowed_directories stores forward-slash entries).
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA profiling_output='%s'", escapeSQLString(filepath.ToSlash(profilePath)))); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set profiling output")
	}
	if _, err := conn.ExecContext(ctx, "SET custom_profiling_settings='{\"PLANNER\": \"true\", \"PLANNER_BINDING\": \"true\", \"PHYSICAL_PLANNER\": \"true\", \"OPERATOR_TIMING\": \"true\", \"OPERATOR_CARDINALITY\": \"true\"}'"); err != nil {
		d.logger.Warn().Err(err).Msg("Failed to set custom profiling settings")
	}

	// Execute query via Arrow API
	start := time.Now()
	var reader array.RecordReader
	rawErr := conn.Raw(func(driverConn any) error {
		var err error
		reader, err = arrowQueryOnConn(ctx, driverConn, query)
		return err
	})
	totalTime := time.Since(start)

	// Disable profiling
	conn.ExecContext(ctx, "PRAGMA disable_profiling")

	if rawErr != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("arrow query failed: %w", rawErr)
	}

	// Read profile data
	profile := &QueryProfile{
		TotalMs: float64(totalTime.Milliseconds()),
	}
	if profileData, err := os.ReadFile(profilePath); err == nil && len(profileData) > 0 {
		var profileJSON map[string]interface{}
		if err := json.Unmarshal(profileData, &profileJSON); err == nil {
			if timing, ok := profileJSON["timing"].(float64); ok {
				profile.Latency = timing * 1000
			}
			if children, ok := profileJSON["children"].([]interface{}); ok {
				for _, child := range children {
					if childMap, ok := child.(map[string]interface{}); ok {
						if name, ok := childMap["name"].(string); ok && name == "PLANNER" {
							if timing, ok := childMap["timing"].(float64); ok {
								profile.PlannerMs = timing * 1000
							}
						}
					}
				}
			}
			profile.ExecutionMs = profile.TotalMs - profile.PlannerMs
		}
	}

	return reader, conn, profile, nil
}
