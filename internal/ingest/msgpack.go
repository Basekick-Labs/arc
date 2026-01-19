package ingest

import (
	"fmt"
	"time"

	"github.com/basekick-labs/arc/pkg/models"
	"github.com/rs/zerolog"
	"github.com/vmihailenco/msgpack/v5"
)

// MessagePackDecoder handles decoding of MessagePack binary protocol
// Supports row format, columnar format (fastest), and batch format
type MessagePackDecoder struct {
	logger        zerolog.Logger
	totalDecoded  uint64
	totalErrors   uint64
}

// NewMessagePackDecoder creates a new MessagePack decoder
func NewMessagePackDecoder(logger zerolog.Logger) *MessagePackDecoder {
	return &MessagePackDecoder{
		logger: logger.With().Str("component", "msgpack-decoder").Logger(),
	}
}

// Decode decodes MessagePack binary data to records or columnar format
// Returns either []models.Record or []models.ColumnarRecord depending on input format
// The raw bytes are preserved in ColumnarRecord.RawPayload for zero-copy WAL
func (d *MessagePackDecoder) Decode(data []byte) (interface{}, error) {
	// IMPORTANT: Decode to generic interface{} first to handle both map and array formats
	// Telegraf and other clients may send data in array-encoded format which would fail
	// if we try to decode directly into a struct
	var rawPayload interface{}
	if err := msgpack.Unmarshal(data, &rawPayload); err != nil {
		d.totalErrors++
		return nil, fmt.Errorf("failed to unmarshal msgpack: %w", err)
	}

	var results []interface{}

	// Process based on the payload type
	switch payload := rawPayload.(type) {
	case map[string]interface{}:
		// Standard map format - convert to MsgPackPayload and decode
		result, err := d.decodeMapPayload(payload, data)
		if err != nil {
			d.totalErrors++
			return nil, err
		}
		if resultSlice, ok := result.([]interface{}); ok {
			results = append(results, resultSlice...)
		} else {
			results = append(results, result)
		}

	case []interface{}:
		// Array format - could be batch of records or a single array-encoded record
		for _, item := range payload {
			switch typedItem := item.(type) {
			case map[string]interface{}:
				result, err := d.decodeMapPayload(typedItem, nil)
				if err != nil {
					d.logger.Error().Err(err).Msg("Failed to decode array item")
					continue
				}
				results = append(results, result)
			default:
				d.logger.Warn().Interface("type", fmt.Sprintf("%T", item)).Msg("Skipping unknown array item type")
			}
		}

	default:
		d.totalErrors++
		return nil, fmt.Errorf("unsupported msgpack payload type: %T", rawPayload)
	}

	d.totalDecoded += uint64(len(results))
	return results, nil
}

// decodeMapPayload decodes a map[string]interface{} payload
func (d *MessagePackDecoder) decodeMapPayload(payload map[string]interface{}, rawData []byte) (interface{}, error) {
	// Check for batch format
	if batch, ok := payload["batch"]; ok {
		if batchSlice, ok := batch.([]interface{}); ok {
			var results []interface{}
			for _, item := range batchSlice {
				if itemMap, ok := item.(map[string]interface{}); ok {
					result, err := d.decodeMapPayload(itemMap, nil)
					if err != nil {
						d.logger.Error().Err(err).Msg("Failed to decode batch item")
						continue
					}
					results = append(results, result)
				}
			}
			return results, nil
		}
	}

	// Convert map to MsgPackPayload struct
	msgPayload := d.mapToPayload(payload)

	// Decode item with raw data for zero-copy WAL
	return d.decodeItemWithRaw(msgPayload, rawData)
}

// mapToPayload converts a generic map to MsgPackPayload struct
func (d *MessagePackDecoder) mapToPayload(m map[string]interface{}) *models.MsgPackPayload {
	payload := &models.MsgPackPayload{}

	if v, ok := m["m"]; ok {
		payload.M = v
	}
	if v, ok := m["t"]; ok {
		payload.T = v
	}
	if v, ok := m["h"]; ok {
		payload.H = v
	}
	if v, ok := m["f"]; ok {
		payload.F = v
	}

	// Handle fields
	if fields, ok := m["fields"]; ok {
		if fieldsMap, ok := fields.(map[string]interface{}); ok {
			payload.Fields = fieldsMap
		}
	}

	// Handle tags
	if tags, ok := m["tags"]; ok {
		if tagsMap, ok := tags.(map[string]interface{}); ok {
			payload.Tags = make(map[string]string)
			for k, v := range tagsMap {
				if s, ok := v.(string); ok {
					payload.Tags[k] = s
				} else {
					payload.Tags[k] = fmt.Sprintf("%v", v)
				}
			}
		}
	}

	// Handle columns (columnar format)
	if columns, ok := m["columns"]; ok {
		if colsMap, ok := columns.(map[string]interface{}); ok {
			payload.Columns = make(map[string][]interface{})
			for k, v := range colsMap {
				if arr, ok := v.([]interface{}); ok {
					payload.Columns[k] = arr
				}
			}
		}
	}

	// Handle batch
	if batch, ok := m["batch"]; ok {
		if batchSlice, ok := batch.([]interface{}); ok {
			payload.Batch = batchSlice
		}
	}

	return payload
}

// decodeItemWithRaw decodes a single item and passes raw bytes for zero-copy WAL
func (d *MessagePackDecoder) decodeItemWithRaw(payload *models.MsgPackPayload, rawData []byte) (interface{}, error) {
	// FAST PATH: Columnar format (zero-copy passthrough to Arrow and WAL)
	if payload.Columns != nil {
		return d.decodeColumnar(payload, rawData)
	}

	// LEGACY PATH: Row format (requires flattening, no zero-copy WAL)
	return d.decodeRow(payload)
}

// decodeColumnar handles columnar format (ZERO-COPY passthrough)
// Input:  {m: "cpu", columns: {time: [...], val: [...], region: [...]}}
// Output: ColumnarRecord with validated columns and optional raw bytes for WAL
func (d *MessagePackDecoder) decodeColumnar(payload *models.MsgPackPayload, rawData []byte) (*models.ColumnarRecord, error) {
	// Extract measurement
	measurement, err := d.extractMeasurement(payload.M)
	if err != nil {
		return nil, err
	}

	// Validate columns exist
	if len(payload.Columns) == 0 {
		return nil, fmt.Errorf("columnar format requires non-empty 'columns' dict")
	}

	// Validate all arrays have same length
	var numRecords int
	firstCol := true
	for colName, colData := range payload.Columns {
		if colData == nil {
			return nil, fmt.Errorf("column '%s' is nil", colName)
		}
		if firstCol {
			numRecords = len(colData)
			firstCol = false
		} else if len(colData) != numRecords {
			return nil, fmt.Errorf("columnar format: array length mismatch (expected %d, got %d for '%s')",
				numRecords, len(colData), colName)
		}
	}

	// Ensure 'time' column exists
	timeCol, hasTime := payload.Columns["time"]
	if !hasTime || timeCol == nil || len(timeCol) == 0 {
		// Generate timestamps if missing - log warning so users know their data lacked timestamps
		d.logger.Warn().
			Str("measurement", measurement).
			Int("row_count", numRecords).
			Msg("Data missing 'time' column - generating UTC timestamps")

		now := time.Now().UTC()
		nowMicros := now.UnixMicro()
		timeCol = make([]interface{}, numRecords)
		for i := 0; i < numRecords; i++ {
			timeCol[i] = nowMicros
		}
		payload.Columns["time"] = timeCol
	}

	// Normalize timestamps to microseconds (Arrow's target precision)
	if err := d.normalizeTimestamps(payload.Columns); err != nil {
		return nil, fmt.Errorf("failed to normalize timestamps: %w", err)
	}

	// Sanitize string columns to ensure valid UTF-8 (prevents DuckDB query failures)
	sanitizedCount := d.sanitizeStringColumns(payload.Columns)
	if sanitizedCount > 0 {
		d.logger.Warn().
			Str("measurement", measurement).
			Int("sanitized_fields", sanitizedCount).
			Msg("Sanitized non-UTF8 characters in string columns")
	}

	return &models.ColumnarRecord{
		Measurement: measurement,
		Columnar:    true,
		Columns:     payload.Columns,
		TimeUnit:    "us",      // microseconds
		RawPayload:  rawData,   // Zero-copy: store original msgpack bytes for WAL
	}, nil
}

// decodeRow handles row format (legacy, requires flattening)
// Input:  {m: "cpu", t: 1633024800000, h: "server01", fields: {...}, tags: {...}}
// Output: Record with flattened structure
func (d *MessagePackDecoder) decodeRow(payload *models.MsgPackPayload) (*models.Record, error) {
	// Extract measurement
	measurement, err := d.extractMeasurement(payload.M)
	if err != nil {
		return nil, err
	}

	// Log warning if timestamp is missing
	if payload.T == nil {
		d.logger.Warn().
			Str("measurement", measurement).
			Msg("Row data missing timestamp - generating UTC timestamp")
	}

	// Extract timestamp
	timestamp, err := d.extractTimestamp(payload.T)
	if err != nil {
		return nil, err
	}

	// Extract host
	host := d.extractHost(payload.H)

	// Extract fields
	fields := payload.Fields
	if fields == nil {
		// Handle compact format (array fields)
		if payload.F != nil {
			fields = d.extractCompactFields(payload.F)
		} else {
			return nil, fmt.Errorf("missing required field 'fields' or 'f'")
		}
	}

	// Extract tags
	tags := payload.Tags
	if tags == nil {
		tags = make(map[string]string)
	}

	// Add host to tags if present
	if host != "" {
		tags["host"] = host
	}

	// Sanitize string fields to ensure valid UTF-8 (prevents DuckDB query failures)
	sanitizedCount := d.sanitizeStringFields(fields)
	if sanitizedCount > 0 {
		d.logger.Warn().
			Str("measurement", measurement).
			Int("sanitized_fields", sanitizedCount).
			Msg("Sanitized non-UTF8 characters in string fields")
	}

	return &models.Record{
		Measurement: measurement,
		Time:        timestamp,
		Fields:      fields,
		Tags:        tags,
	}, nil
}

// extractMeasurement extracts measurement name from payload
func (d *MessagePackDecoder) extractMeasurement(m interface{}) (string, error) {
	if m == nil {
		return "", fmt.Errorf("missing required field 'm' (measurement)")
	}

	switch v := m.(type) {
	case string:
		return v, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("measurement_%v", v), nil
	default:
		return "", fmt.Errorf("invalid measurement type: %T", m)
	}
}

// extractTimestamp extracts and normalizes timestamp
// Auto-detects unit: seconds, milliseconds, or microseconds
func (d *MessagePackDecoder) extractTimestamp(t interface{}) (time.Time, error) {
	if t == nil {
		// Use current time if not provided
		return time.Now().UTC(), nil
	}

	var ts int64
	switch v := t.(type) {
	case int:
		ts = int64(v)
	case int8:
		ts = int64(v)
	case int16:
		ts = int64(v)
	case int32:
		ts = int64(v)
	case int64:
		ts = v
	case uint:
		ts = int64(v)
	case uint8:
		ts = int64(v)
	case uint16:
		ts = int64(v)
	case uint32:
		ts = int64(v)
	case uint64:
		ts = int64(v)
	case float32:
		ts = int64(v)
	case float64:
		ts = int64(v)
	default:
		return time.Time{}, fmt.Errorf("invalid timestamp type: %T", t)
	}

	// Auto-detect unit and convert to time.Time
	if ts < 1e10 {
		// Seconds
		return time.Unix(ts, 0).UTC(), nil
	} else if ts < 1e13 {
		// Milliseconds
		return time.UnixMilli(ts).UTC(), nil
	} else {
		// Microseconds
		return time.UnixMicro(ts).UTC(), nil
	}
}

// extractHost extracts host identifier
func (d *MessagePackDecoder) extractHost(h interface{}) string {
	if h == nil {
		return "unknown"
	}

	switch v := h.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("host_%v", v)
	default:
		return "unknown"
	}
}

// extractCompactFields converts compact field array to map
func (d *MessagePackDecoder) extractCompactFields(f interface{}) map[string]interface{} {
	fields := make(map[string]interface{})

	switch v := f.(type) {
	case []interface{}:
		for i, val := range v {
			fields[fmt.Sprintf("field_%d", i)] = val
		}
	default:
		d.logger.Warn().Interface("type", fmt.Sprintf("%T", f)).Msg("Invalid compact fields format")
	}

	return fields
}

// normalizeTimestamps converts time column to microseconds IN-PLACE
// OPTIMIZATION: Single-pass algorithm that handles both type conversion and normalization
// Modifies values in-place to avoid allocating a new []interface{} slice
// This reduces GC pressure significantly under high load (6M+ RPS)
func (d *MessagePackDecoder) normalizeTimestamps(columns map[string][]interface{}) error {
	timeCol, exists := columns["time"]
	if !exists || len(timeCol) == 0 {
		return nil
	}

	// Detect unit from first value using consolidated helper
	firstVal, ok := toInt64Timestamp(timeCol[0])
	if !ok {
		return fmt.Errorf("invalid timestamp type in columnar format: %T", timeCol[0])
	}

	// Determine multiplier based on detected unit
	var multiplier int64
	if firstVal < 1e10 {
		// Seconds → microseconds
		multiplier = 1_000_000
	} else if firstVal < 1e13 {
		// Milliseconds → microseconds
		multiplier = 1000
	} else {
		// Already microseconds - still need to normalize types to int64
		multiplier = 1
	}

	// SINGLE-PASS: Convert and normalize in one iteration
	// For homogeneous int64 columns (common case), the type assertion is very fast
	// For mixed types, we fall back to the helper on first non-int64 value
	for i, val := range timeCol {
		// Fast path: direct int64 assertion (most common case)
		if ts, ok := val.(int64); ok {
			timeCol[i] = ts * multiplier
			continue
		}

		// Slow path: use helper for other numeric types
		ts, ok := toInt64Timestamp(val)
		if !ok {
			return fmt.Errorf("invalid timestamp type at index %d: %T", i, val)
		}
		timeCol[i] = ts * multiplier
	}

	return nil
}

// toInt64Timestamp converts any numeric type to int64 for timestamp normalization
// Optimized version that only handles types commonly seen in timestamp columns
func toInt64Timestamp(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int:
		return int64(val), true
	case int32:
		return int64(val), true
	case uint:
		return int64(val), true
	case uint32:
		return int64(val), true
	case uint64:
		return int64(val), true
	case float64:
		return int64(val), true
	case float32:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case uint8:
		return int64(val), true
	case uint16:
		return int64(val), true
	default:
		return 0, false
	}
}

// sanitizeStringColumns sanitizes string values in columnar data to ensure valid UTF-8.
// This prevents DuckDB query failures caused by non-UTF-8 data in Parquet files.
// Returns count of sanitized fields for logging.
func (d *MessagePackDecoder) sanitizeStringColumns(columns map[string][]interface{}) int {
	sanitizedCount := 0
	for _, colData := range columns {
		for i, val := range colData {
			if s, ok := val.(string); ok {
				sanitized, modified := SanitizeUTF8(s)
				if modified {
					colData[i] = sanitized
					sanitizedCount++
				}
			}
		}
	}
	return sanitizedCount
}

// sanitizeStringFields sanitizes string values in row-format fields to ensure valid UTF-8.
// This prevents DuckDB query failures caused by non-UTF-8 data in Parquet files.
// Returns count of sanitized fields for logging.
func (d *MessagePackDecoder) sanitizeStringFields(fields map[string]interface{}) int {
	sanitizedCount := 0
	for key, val := range fields {
		if s, ok := val.(string); ok {
			sanitized, modified := SanitizeUTF8(s)
			if modified {
				fields[key] = sanitized
				sanitizedCount++
			}
		}
	}
	return sanitizedCount
}

// GetStats returns decoder statistics
func (d *MessagePackDecoder) GetStats() map[string]interface{} {
	var errorRate float64
	if d.totalDecoded > 0 {
		errorRate = float64(d.totalErrors) / float64(d.totalDecoded)
	}

	return map[string]interface{}{
		"total_decoded": d.totalDecoded,
		"total_errors":  d.totalErrors,
		"error_rate":    errorRate,
	}
}
