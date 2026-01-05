package config

import (
	"fmt"
	"strings"
)

// ParseSortKeys parses sort key configuration from IngestConfig.
// Format: ["measurement:col1,col2", ...]
//
// Users only configure ADDITIONAL sort columns - the "time" column is always
// appended automatically at the usage site (getSortKeys in arrow_writer.go).
// This simplifies configuration: users don't need to remember to include "time".
//
// Returns:
//   - map[measurement][]sortKeys: Per-measurement sort key configuration
//   - []string: Default sort keys (additional columns before time)
//   - error: If configuration is invalid
func ParseSortKeys(cfg IngestConfig) (map[string][]string, []string, error) {
	// Parse measurement-specific sort keys
	sortKeysMap := make(map[string][]string)

	for _, config := range cfg.SortKeys {
		parts := strings.SplitN(config, ":", 2)
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("invalid sort key format: %s (expected 'measurement:col1,col2')", config)
		}

		measurement := strings.TrimSpace(parts[0])
		if measurement == "" {
			return nil, nil, fmt.Errorf("empty measurement name in sort key: %s", config)
		}

		keys := strings.Split(parts[1], ",")
		parsedKeys := make([]string, 0, len(keys))
		for _, key := range keys {
			key = strings.TrimSpace(key)
			if key == "" {
				return nil, nil, fmt.Errorf("empty sort key in: %s", config)
			}
			parsedKeys = append(parsedKeys, key)
		}

		if len(parsedKeys) == 0 {
			return nil, nil, fmt.Errorf("no sort keys specified for measurement %s", measurement)
		}

		sortKeysMap[measurement] = parsedKeys
	}

	// Parse default sort keys (additional columns before time)
	// Empty string means time-only sorting (no additional columns)
	var parsedDefaultKeys []string
	if cfg.DefaultSortKeys != "" {
		defaultKeys := strings.Split(cfg.DefaultSortKeys, ",")
		parsedDefaultKeys = make([]string, 0, len(defaultKeys))
		for _, key := range defaultKeys {
			key = strings.TrimSpace(key)
			if key != "" {
				parsedDefaultKeys = append(parsedDefaultKeys, key)
			}
		}
	}

	// Note: We intentionally don't add "time" here.
	// The "time" column is always appended at the usage site (getSortKeys).
	// Empty parsedDefaultKeys means "time-only sorting".

	return sortKeysMap, parsedDefaultKeys, nil
}
