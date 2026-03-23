package config

import (
	"fmt"
	"strconv"
	"strings"
)

// DecimalSpec holds precision and scale for a Decimal128 column.
type DecimalSpec struct {
	Precision int32 // 1-38 (total significant digits)
	Scale     int32 // 0-precision (digits after decimal point)
}

// ParseDecimalColumns parses decimal column configuration from IngestConfig.
// Format: ["measurement:col=precision,scale;col2=p,s", ...]
//
// Returns:
//   - map[measurement]map[column]DecimalSpec: Per-measurement decimal column config
//   - map[column]DecimalSpec: Default decimal columns
//   - error: If configuration is invalid
func ParseDecimalColumns(cfg IngestConfig) (map[string]map[string]DecimalSpec, map[string]DecimalSpec, error) {
	decimalMap := make(map[string]map[string]DecimalSpec)

	for _, entry := range cfg.DecimalColumns {
		parts := strings.SplitN(entry, ":", 2)
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("invalid decimal column format: %s (expected 'measurement:col=precision,scale')", entry)
		}

		measurement := strings.TrimSpace(parts[0])
		if measurement == "" {
			return nil, nil, fmt.Errorf("empty measurement name in decimal config: %s", entry)
		}

		specs, err := parseDecimalSpecs(parts[1])
		if err != nil {
			return nil, nil, fmt.Errorf("invalid decimal config for measurement %s: %w", measurement, err)
		}

		decimalMap[measurement] = specs
	}

	// Parse defaults
	var defaultSpecs map[string]DecimalSpec
	if cfg.DefaultDecimalColumns != "" {
		var err error
		defaultSpecs, err = parseDecimalSpecs(cfg.DefaultDecimalColumns)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid default decimal config: %w", err)
		}
	}

	return decimalMap, defaultSpecs, nil
}

// parseDecimalSpecs parses "col=precision,scale;col2=p,s" into a map.
func parseDecimalSpecs(s string) (map[string]DecimalSpec, error) {
	specs := make(map[string]DecimalSpec)

	entries := strings.Split(s, ";")
	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		eqParts := strings.SplitN(entry, "=", 2)
		if len(eqParts) != 2 {
			return nil, fmt.Errorf("invalid spec %q (expected 'column=precision,scale')", entry)
		}

		colName := strings.TrimSpace(eqParts[0])
		if colName == "" {
			return nil, fmt.Errorf("empty column name in spec %q", entry)
		}

		psParts := strings.SplitN(eqParts[1], ",", 2)
		if len(psParts) != 2 {
			return nil, fmt.Errorf("invalid precision,scale in spec %q (expected 'precision,scale')", entry)
		}

		precision, err := strconv.ParseInt(strings.TrimSpace(psParts[0]), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid precision in spec %q: %w", entry, err)
		}
		scale, err := strconv.ParseInt(strings.TrimSpace(psParts[1]), 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid scale in spec %q: %w", entry, err)
		}

		if precision < 1 || precision > 38 {
			return nil, fmt.Errorf("precision must be 1-38, got %d in spec %q", precision, entry)
		}
		if scale < 0 || scale > precision {
			return nil, fmt.Errorf("scale must be 0-%d, got %d in spec %q", precision, scale, entry)
		}

		specs[colName] = DecimalSpec{
			Precision: int32(precision),
			Scale:     int32(scale),
		}
	}

	if len(specs) == 0 {
		return nil, fmt.Errorf("no decimal columns specified")
	}

	return specs, nil
}
