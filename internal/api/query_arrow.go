package api

import (
	"bufio"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	// Parse request body
	var req QueryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "Invalid request body: " + err.Error(),
		})
	}

	// Validate SQL
	if strings.TrimSpace(req.SQL) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "SQL query is required",
		})
	}

	if len(req.SQL) > 10000 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   "SQL query exceeds maximum length (10000 characters)",
		})
	}

	// Check for dangerous SQL patterns
	if err := h.validateSQL(req.SQL); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"success": false,
			"error":   err.Error(),
		})
	}

	// Convert SQL to storage paths
	convertedSQL := h.convertSQLToStoragePaths(req.SQL)

	h.logger.Debug().
		Str("original_sql", req.SQL).
		Str("converted_sql", convertedSQL).
		Msg("Executing Arrow query")

	// Execute query using standard database/sql interface
	rows, err := h.db.Query(convertedSQL)
	if err != nil {
		h.logger.Error().Err(err).Str("sql", req.SQL).Msg("Arrow query execution failed")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Query execution failed",
		})
	}
	// Note: rows.Close() is called inside SetBodyStreamWriter callback, not here,
	// because SetBodyStreamWriter runs asynchronously after this handler returns.

	// Get column info
	columns, err := rows.Columns()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get column names")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Query execution failed",
		})
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get column types")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"success": false,
			"error":   "Query execution failed",
		})
	}

	// Build Arrow schema from column types
	arrowFields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		arrowFields[i] = arrow.Field{
			Name:     col,
			Type:     sqlTypeToArrowType(columnTypes[i].DatabaseTypeName()),
			Nullable: true,
		}
	}
	schema := arrow.NewSchema(arrowFields, nil)

	// Set response headers before streaming
	c.Set("Content-Type", "application/vnd.apache.arrow.stream")

	// Use streaming response - write Arrow batches directly to the response
	// This eliminates double-buffering: rows go directly into Arrow builders,
	// and batches are written to the response as they're filled.
	c.Context().SetBodyStreamWriter(func(w *bufio.Writer) {
		mem := memory.NewGoAllocator()
		ipcWriter := ipc.NewWriter(w, ipc.WithSchema(schema))
		defer ipcWriter.Close()

		recordBuilder := array.NewRecordBuilder(mem, schema)
		defer recordBuilder.Release()

		var totalRows int64
		var batchRows int

		// Pre-allocate scan buffers once (reused for each row)
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				h.logger.Error().Err(err).Msg("Failed to scan row")
				continue
			}

			// Append values directly to Arrow builders (no intermediate slice)
			for colIdx, val := range values {
				appendValueToBuilder(recordBuilder.Field(colIdx), val, arrowFields[colIdx].Type)
			}

			batchRows++
			totalRows++

			// Flush batch when it reaches the target size
			if batchRows >= arrowBatchSize {
				record := recordBuilder.NewRecord()
				if err := ipcWriter.Write(record); err != nil {
					h.logger.Error().Err(err).Msg("Failed to write Arrow batch")
					record.Release()
					return
				}
				record.Release()
				w.Flush() // Flush to client immediately

				// Reset builder for next batch
				recordBuilder.Release()
				recordBuilder = array.NewRecordBuilder(mem, schema)
				batchRows = 0
			}
		}

		// Write any remaining rows as final batch
		if batchRows > 0 {
			record := recordBuilder.NewRecord()
			if err := ipcWriter.Write(record); err != nil {
				h.logger.Error().Err(err).Msg("Failed to write final Arrow batch")
				record.Release()
				return
			}
			record.Release()
		}

		if err := rows.Err(); err != nil {
			h.logger.Error().Err(err).Msg("Error iterating rows")
		}

		// Close rows here since we can't use defer (handler returns before streaming completes)
		rows.Close()

		executionTime := float64(time.Since(start).Milliseconds())
		h.logger.Info().
			Int64("row_count", totalRows).
			Float64("execution_time_ms", executionTime).
			Msg("Arrow streaming query completed")
	})

	return nil
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
func appendValueToBuilder(builder array.Builder, val interface{}, arrowType arrow.DataType) {
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
