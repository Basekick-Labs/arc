// Package ingest provides data ingestion functionality for Arc.
// This file implements an InfluxDB Line Protocol parser.
//
// Line Protocol Format:
//
//	measurement[,tag_key=tag_value...] field_key=field_value[,field_key=field_value...] [timestamp]
//
// Examples:
//
//	cpu,host=server01,region=us-west usage_idle=90.5,usage_system=2.1 1609459200000000000
//	temperature,sensor=bedroom temp=22.5
//	http_requests,method=GET,status=200 count=1i
package ingest

import (
	"bytes"
	"strconv"
	"strings"
	"time"

	"github.com/basekick-labs/arc/pkg/models"
)

// LineProtocolParser parses InfluxDB Line Protocol format
type LineProtocolParser struct{}

// NewLineProtocolParser creates a new Line Protocol parser
func NewLineProtocolParser() *LineProtocolParser {
	return &LineProtocolParser{}
}

// ParseLine parses a single line of InfluxDB Line Protocol.
// Timestamps are assumed to be nanoseconds and converted to microseconds.
// Returns nil if the line is invalid or a comment.
func (p *LineProtocolParser) ParseLine(line []byte) *models.Record {
	return p.parseLineWithPrecision(line, "ns")
}

// ParseBatch parses multiple lines of line protocol
func (p *LineProtocolParser) ParseBatch(data []byte) []*models.Record {
	lines := bytes.Split(data, []byte{'\n'})
	records := make([]*models.Record, 0, len(lines))

	for _, line := range lines {
		record := p.ParseLine(line)
		if record != nil {
			records = append(records, record)
		}
	}

	return records
}

// ParseBatchWithPrecision parses LP data with a specified timestamp precision.
// The precision parameter controls how raw timestamp values are interpreted:
//   - "ns" (default): nanoseconds — same as ParseBatch
//   - "us": microseconds
//   - "ms": milliseconds
//   - "s": seconds
//
// All timestamps are normalized to microseconds in the output records.
func (p *LineProtocolParser) ParseBatchWithPrecision(data []byte, precision string) []*models.Record {
	if precision == "" || precision == "ns" {
		return p.ParseBatch(data)
	}

	lines := bytes.Split(data, []byte{'\n'})
	records := make([]*models.Record, 0, len(lines))

	for _, line := range lines {
		record := p.parseLineWithPrecision(line, precision)
		if record != nil {
			records = append(records, record)
		}
	}

	return records
}

// parseLineWithPrecision parses a single LP line with custom timestamp precision.
// Converts the raw timestamp to microseconds based on the given precision.
func (p *LineProtocolParser) parseLineWithPrecision(line []byte, precision string) *models.Record {
	line = bytes.TrimSpace(line)
	if len(line) == 0 || line[0] == '#' {
		return nil
	}

	parts := p.splitLine(line)
	if len(parts) < 2 {
		return nil
	}

	measurement, tags := p.parseMeasurementTags(parts[0])
	if measurement == "" {
		return nil
	}

	fields := p.parseFields(parts[1])
	if len(fields) == 0 {
		return nil
	}

	var timestamp int64
	if len(parts) >= 3 {
		rawTs, err := strconv.ParseInt(string(bytes.TrimSpace(parts[2])), 10, 64)
		if err != nil {
			timestamp = time.Now().UnixMicro()
		} else {
			switch precision {
			case "us":
				timestamp = rawTs
			case "ms":
				timestamp = rawTs * 1000
			case "s":
				timestamp = rawTs * 1_000_000
			default:
				timestamp = rawTs / 1000 // ns → μs (fallback)
			}
		}
	} else {
		timestamp = time.Now().UnixMicro()
	}

	return &models.Record{
		Measurement: measurement,
		Tags:        tags,
		Fields:      fields,
		Timestamp:   timestamp,
	}
}

// splitOnDelimiter splits data on an unescaped delimiter, respecting escaped chars and quoted strings.
// Used by splitLine (space) and splitOnComma (comma).
func splitOnDelimiter(data []byte, delim byte) [][]byte {
	var parts [][]byte
	var current []byte
	inQuotes := false

	for i := 0; i < len(data); i++ {
		if data[i] == '\\' && i+1 < len(data) {
			// Escaped character - include both backslash and next char
			current = append(current, data[i], data[i+1])
			i++
		} else if data[i] == '"' {
			inQuotes = !inQuotes
			current = append(current, data[i])
		} else if data[i] == delim && !inQuotes {
			if len(current) > 0 {
				parts = append(parts, current)
				current = nil
			}
		} else {
			current = append(current, data[i])
		}
	}

	if len(current) > 0 {
		parts = append(parts, current)
	}

	return parts
}

// splitLine splits a line protocol line into parts on unescaped spaces.
// Returns [measurement_tags, fields, timestamp?]
func (p *LineProtocolParser) splitLine(line []byte) [][]byte {
	return splitOnDelimiter(line, ' ')
}

// splitOnComma splits on unescaped commas, respecting quoted strings.
func (p *LineProtocolParser) splitOnComma(data []byte) [][]byte {
	return splitOnDelimiter(data, ',')
}

// parseMeasurementTags parses measurement name and tags
// Format: measurement[,tag=value,...]
func (p *LineProtocolParser) parseMeasurementTags(part []byte) (string, map[string]string) {
	// Split on unescaped commas
	components := p.splitOnComma(part)

	if len(components) == 0 {
		return "", nil
	}

	measurement := p.unescape(components[0])
	tags := make(map[string]string)

	for _, component := range components[1:] {
		idx := bytes.IndexByte(component, '=')
		if idx > 0 {
			key := p.unescape(component[:idx])
			value := p.unescape(component[idx+1:])
			tags[key] = value
		}
	}

	return measurement, tags
}

// parseFields parses field set
// Format: field_key=field_value[,field_key=field_value...]
func (p *LineProtocolParser) parseFields(part []byte) map[string]interface{} {
	fields := make(map[string]interface{})

	// Split on unescaped commas
	fieldParts := p.splitOnComma(part)

	for _, fieldPart := range fieldParts {
		idx := bytes.IndexByte(fieldPart, '=')
		if idx <= 0 {
			continue
		}

		key := p.unescape(fieldPart[:idx])
		value := p.parseFieldValue(fieldPart[idx+1:])

		if value != nil {
			fields[key] = value
		}
	}

	return fields
}

// parseFieldValue parses field value based on InfluxDB type indicators
// Type indicators:
//   - Integer: ends with 'i' (e.g., 123i)
//   - Unsigned integer: ends with 'u' (e.g., 123u)
//   - Float: numeric without suffix (e.g., 123.45)
//   - String: wrapped in quotes (e.g., "hello")
//   - Boolean: t, T, true, TRUE, f, F, false, FALSE
func (p *LineProtocolParser) parseFieldValue(value []byte) interface{} {
	value = bytes.TrimSpace(value)
	if len(value) == 0 {
		return nil
	}

	// Convert to string for easier comparison
	strValue := string(value)
	lowerValue := strings.ToLower(strValue)

	// Boolean
	if lowerValue == "t" || lowerValue == "true" {
		return true
	}
	if lowerValue == "f" || lowerValue == "false" {
		return false
	}

	// String (quoted)
	if value[0] == '"' {
		if len(value) > 1 && value[len(value)-1] == '"' {
			// Remove quotes and unescape, then sanitize for valid UTF-8
			unescaped := p.unescape(value[1 : len(value)-1])
			sanitized, _ := SanitizeUTF8(unescaped)
			return sanitized
		}
		// Malformed quoted string - return as-is without leading quote, sanitized
		sanitized, _ := SanitizeUTF8(strings.Trim(strValue, "\""))
		return sanitized
	}

	// Integer (ends with 'i')
	if value[len(value)-1] == 'i' {
		intVal, err := strconv.ParseInt(strValue[:len(strValue)-1], 10, 64)
		if err == nil {
			return intVal
		}
		return nil
	}

	// Unsigned integer (ends with 'u')
	if value[len(value)-1] == 'u' {
		uintVal, err := strconv.ParseUint(strValue[:len(strValue)-1], 10, 64)
		if err == nil {
			return uintVal
		}
		return nil
	}

	// Float/number
	floatVal, err := strconv.ParseFloat(strValue, 64)
	if err == nil {
		return floatVal
	}

	// If all else fails, treat as string (sanitized for valid UTF-8)
	sanitized, _ := SanitizeUTF8(strValue)
	return sanitized
}

// unescape unescapes special characters in line protocol (\, \  \=)
// Single-pass: only allocates a new string when escapes are found.
func (p *LineProtocolParser) unescape(data []byte) string {
	// Fast path: no backslash means no escapes
	if !bytes.ContainsRune(data, '\\') {
		return string(data)
	}

	buf := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if data[i] == '\\' && i+1 < len(data) {
			next := data[i+1]
			if next == ',' || next == ' ' || next == '=' {
				buf = append(buf, next)
				i++ // skip next
				continue
			}
		}
		buf = append(buf, data[i])
	}
	return string(buf)
}

// ToFlatRecord converts a parsed record to a flat map for Arrow/Parquet
// Tags and fields are flattened into top-level keys.
// Field names that conflict with tag names get "_value" suffix.
func ToFlatRecord(record *models.Record) map[string]interface{} {
	flat := make(map[string]interface{})

	// Add time and measurement
	flat["time"] = record.Timestamp
	flat["measurement"] = record.Measurement

	// Add tags (all strings)
	for key, value := range record.Tags {
		flat[key] = value
	}

	// Add fields (handle conflicts with tags)
	for key, value := range record.Fields {
		if _, hasTag := record.Tags[key]; hasTag {
			// Field name conflicts with tag - add suffix
			flat[key+"_value"] = value
		} else {
			flat[key] = value
		}
	}

	return flat
}

// BatchToColumnar converts a batch of records to columnar format
// Groups records by measurement and converts to column-oriented data
func BatchToColumnar(records []*models.Record) map[string]map[string][]interface{} {
	// Group by measurement
	byMeasurement := make(map[string][]*models.Record)
	for _, record := range records {
		byMeasurement[record.Measurement] = append(byMeasurement[record.Measurement], record)
	}

	result := make(map[string]map[string][]interface{})

	for measurement, measurementRecords := range byMeasurement {
		// Collect all column names
		columns := make(map[string]bool)
		columns["time"] = true

		for _, record := range measurementRecords {
			for key := range record.Tags {
				columns[key] = true
			}
			for key := range record.Fields {
				if _, hasTag := record.Tags[key]; hasTag {
					columns[key+"_value"] = true
				} else {
					columns[key] = true
				}
			}
		}

		// Build columnar data
		columnarData := make(map[string][]interface{})
		for col := range columns {
			columnarData[col] = make([]interface{}, len(measurementRecords))
		}

		// Fill data
		for i, record := range measurementRecords {
			columnarData["time"][i] = record.Timestamp

			for key, value := range record.Tags {
				columnarData[key][i] = value
			}

			for key, value := range record.Fields {
				if _, hasTag := record.Tags[key]; hasTag {
					columnarData[key+"_value"][i] = value
				} else {
					columnarData[key][i] = value
				}
			}
		}

		result[measurement] = columnarData
	}

	return result
}
