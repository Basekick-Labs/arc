//go:build duckdb_arrow

package api

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/basekick-labs/arc/internal/metrics"
	"github.com/gofiber/fiber/v2"
)

// arrowBatchSize is the number of rows per Arrow record batch.
// Smaller batches reduce peak memory usage and enable streaming.
// 10K rows is a good balance between overhead and memory efficiency.
const arrowBatchSize = 10000

// executeQueryArrow handles POST /api/v1/query/arrow - returns Arrow IPC stream
// Optimized to stream rows directly into Arrow batches without intermediate buffering.
func (h *QueryHandler) executeQueryArrow(c *fiber.Ctx) error {
	start := time.Now()
	m := metrics.Get()

	// Parse request body
	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	// Validate SQL (empty, max length, dangerous patterns)
	if err := ValidateSQLRequest(req.SQL); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Extract x-arc-database header for optimized query path
	headerDB := c.Get("x-arc-database")

	// If header is set, reject cross-database syntax (db.table not allowed)
	if headerDB != "" && hasCrossDatabaseSyntax(req.SQL) {
		m.IncQueryErrors()
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Cross-database queries (db.table syntax) not allowed when x-arc-database header is set",
		})
	}

	// Check RBAC permissions for all tables referenced in the query
	if err := h.checkQueryPermissions(c, req.SQL, "read"); err != nil {
		m.IncQueryErrors()
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Convert SQL to storage paths (with caching)
	// If headerDB is set, uses optimized path that skips db.table regex patterns
	convertedSQL, _ := h.getTransformedSQL(req.SQL, headerDB)

	h.logger.Debug().
		Str("original_sql", req.SQL).
		Str("converted_sql", convertedSQL).
		Str("header_db", headerDB).
		Msg("Executing Arrow query")

	// Create context with timeout if configured
	// Use context.Background() instead of c.UserContext() because SetBodyStreamWriter
	// runs asynchronously after the handler returns, and c.UserContext() would be cancelled
	// Note: We don't use defer cancel() here because the streaming callback runs after
	// this handler returns - cancel is called inside the callback after rows are consumed
	ctx := context.Background()
	var cancel context.CancelFunc
	if h.queryTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, h.queryTimeout)
	}

	// Execute query using DuckDB's native Arrow API — returns record batches
	// directly from DuckDB's internal columnar chunks, no row-by-row scanning.
	reader, conn, err := h.db.ArrowQueryContext(ctx, convertedSQL)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		if h.queryTimeout > 0 && ctx.Err() == context.DeadlineExceeded {
			m.IncQueryTimeouts()
			h.logger.Error().Err(err).Str("sql", req.SQL).Dur("timeout", h.queryTimeout).Msg("Arrow query timed out")
			return c.Status(fiber.StatusGatewayTimeout).JSON(fiber.Map{
				"success": false,
				"error":   "Query timed out",
			})
		}
		m.IncQueryErrors()
		h.logger.Error().Err(err).Str("sql", req.SQL).Msg("Arrow query execution failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	schema := reader.Schema()

	// Normalize decimal columns in the schema — DuckDB returns SUM(integer) as
	// decimal(38,0) which many Arrow clients (e.g. Grafana) cannot handle.
	// castInfo is nil when there are no decimal columns (zero overhead on hot path).
	castInfo := normalizeDecimalSchema(schema)
	if castInfo != nil {
		schema = castInfo.schema
	}

	c.Set("Content-Type", "application/vnd.apache.arrow.stream")

	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		ipcWriter := ipc.NewWriter(w, ipc.WithSchema(schema))

		var totalRows int64
		for reader.Next() {
			batch := reader.Record()
			if batch == nil {
				break
			}
			totalRows += batch.NumRows()

			if castInfo != nil {
				var castErr error
				batch, castErr = castDecimalBatch(batch, castInfo)
				if castErr != nil {
					h.logger.Error().Err(castErr).Msg("Failed to cast decimal columns in Arrow batch")
					batch.Release()
					break
				}
				defer batch.Release()
			}

			if err := ipcWriter.Write(batch); err != nil {
				h.logger.Error().Err(err).Msg("Failed to write Arrow batch")
				break
			}
			w.Flush()
		}

		if err := reader.Err(); err != nil {
			h.logger.Error().Err(err).Msg("Error iterating Arrow batches")
		}

		if err := ipcWriter.Close(); err != nil {
			h.logger.Error().Err(err).Msg("Failed to close Arrow IPC writer")
		}
		reader.Release()
		conn.Close()
		if cancel != nil {
			cancel()
		}

		h.logger.Info().
			Int64("row_count", totalRows).
			Float64("execution_time_ms", float64(time.Since(start).Milliseconds())).
			Msg("Arrow streaming query completed")
	})

	return nil
}

// decimalCastInfo holds the modified schema and per-column cast targets for
// queries that return decimal columns (e.g. SUM/AVG on integer columns).
type decimalCastInfo struct {
	schema  *arrow.Schema
	targets []arrow.DataType // nil entry = no cast needed for that column index
}

// normalizeDecimalSchema inspects the schema for decimal columns and returns
// a decimalCastInfo with a substituted schema if any are found, or nil if
// there are no decimal columns (zero overhead on the common path).
//
//   - decimal(x, 0) → int64  (SUM/COUNT of integers)
//   - decimal(x, y) → float64 (AVG or user-configured decimals)
func normalizeDecimalSchema(schema *arrow.Schema) *decimalCastInfo {
	hasDecimal := false
	for i := 0; i < schema.NumFields(); i++ {
		if _, ok := schema.Field(i).Type.(*arrow.Decimal128Type); ok {
			hasDecimal = true
			break
		}
	}
	if !hasDecimal {
		return nil
	}

	targets := make([]arrow.DataType, schema.NumFields())
	fields := make([]arrow.Field, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		f := schema.Field(i)
		if dt, ok := f.Type.(*arrow.Decimal128Type); ok {
			if dt.Scale == 0 {
				targets[i] = arrow.PrimitiveTypes.Int64
			} else {
				targets[i] = arrow.PrimitiveTypes.Float64
			}
			fields[i] = arrow.Field{Name: f.Name, Type: targets[i], Nullable: f.Nullable, Metadata: f.Metadata}
		} else {
			fields[i] = f
		}
	}

	md := schema.Metadata()
	return &decimalCastInfo{
		schema:  arrow.NewSchema(fields, &md),
		targets: targets,
	}
}

// castDecimalBatch replaces decimal columns in the batch with int64 or float64
// using arrow-go's compute.CastArray (SIMD-optimized, handles nulls via bitmap).
// The returned record must be Released by the caller.
func castDecimalBatch(batch arrow.Record, info *decimalCastInfo) (arrow.Record, error) {
	cols := make([]arrow.Array, batch.NumCols())
	toRelease := make([]arrow.Array, 0, batch.NumCols())

	ctx := compute.WithAllocator(context.Background(), memory.DefaultAllocator)

	for i, target := range info.targets {
		if target == nil {
			cols[i] = batch.Column(i)
			continue
		}
		casted, err := compute.CastArray(ctx, batch.Column(i), compute.SafeCastOptions(target))
		if err != nil {
			// Release any arrays we already allocated
			for _, a := range toRelease {
				a.Release()
			}
			return nil, err
		}
		cols[i] = casted
		toRelease = append(toRelease, casted)
	}

	rec := array.NewRecord(info.schema, cols, batch.NumRows())
	for _, a := range toRelease {
		a.Release()
	}
	return rec, nil
}

// sqlTypeToArrowType converts SQL type names to Arrow types
func sqlTypeToArrowType(sqlType string) arrow.DataType {
	sqlType = strings.ToUpper(sqlType)
	switch {
	case strings.Contains(sqlType, "INT64"), strings.Contains(sqlType, "BIGINT"):
		return arrow.PrimitiveTypes.Int64
	case strings.Contains(sqlType, "INT32"), strings.Contains(sqlType, "INTEGER"), strings.Contains(sqlType, "INT"):
		return arrow.PrimitiveTypes.Int64 // Use Int64 for safety
	case strings.Contains(sqlType, "FLOAT"), strings.Contains(sqlType, "DOUBLE"), strings.Contains(sqlType, "REAL"):
		return arrow.PrimitiveTypes.Float64
	case strings.Contains(sqlType, "BOOL"):
		return arrow.FixedWidthTypes.Boolean
	case strings.Contains(sqlType, "TIMESTAMP"), strings.Contains(sqlType, "DATETIME"):
		return arrow.FixedWidthTypes.Timestamp_us
	case strings.Contains(sqlType, "DATE"):
		return arrow.FixedWidthTypes.Date32
	default:
		return arrow.BinaryTypes.String
	}
}

// appendValueToBuilder appends a value to the appropriate Arrow builder
func appendValueToBuilder(builder array.Builder, val interface{}, _ arrow.DataType) {
	if val == nil {
		builder.AppendNull()
		return
	}

	switch b := builder.(type) {
	case *array.Int64Builder:
		switch v := val.(type) {
		case int64:
			b.Append(v)
		case int32:
			b.Append(int64(v))
		case int:
			b.Append(int64(v))
		case float64:
			b.Append(int64(v))
		default:
			b.AppendNull()
		}
	case *array.Float64Builder:
		switch v := val.(type) {
		case float64:
			b.Append(v)
		case float32:
			b.Append(float64(v))
		case int64:
			b.Append(float64(v))
		case int:
			b.Append(float64(v))
		default:
			b.AppendNull()
		}
	case *array.StringBuilder:
		switch v := val.(type) {
		case string:
			b.Append(v)
		case []byte:
			b.Append(string(v))
		case time.Time:
			b.Append(v.Format(time.RFC3339Nano))
		default:
			b.Append(fmt.Sprintf("%v", v))
		}
	case *array.BooleanBuilder:
		switch v := val.(type) {
		case bool:
			b.Append(v)
		default:
			b.AppendNull()
		}
	case *array.TimestampBuilder:
		switch v := val.(type) {
		case time.Time:
			b.Append(arrow.Timestamp(v.UnixMicro()))
		case string:
			if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
				b.Append(arrow.Timestamp(t.UTC().UnixMicro()))
			} else {
				b.AppendNull()
			}
		default:
			b.AppendNull()
		}
	case *array.Date32Builder:
		switch v := val.(type) {
		case time.Time:
			b.Append(arrow.Date32FromTime(v))
		default:
			b.AppendNull()
		}
	default:
		builder.AppendNull()
	}
}

// registerArrowRoutes registers Arrow-specific query endpoints
func (h *QueryHandler) registerArrowRoutes(app *fiber.App) {
	app.Post("/api/v1/query/arrow", h.executeQueryArrow)
}
