package config

import (
	"reflect"
	"testing"
)

func TestParseSortKeys(t *testing.T) {
	// Note: ParseSortKeys returns ADDITIONAL sort columns only.
	// The "time" column is always appended at the usage site (getSortKeys in arrow_writer.go).
	// Users don't need to specify "time" in their configuration.
	tests := []struct {
		name            string
		config          IngestConfig
		wantSortKeys    map[string][]string
		wantDefaultKeys []string
		wantErr         bool
	}{
		{
			name: "empty config returns empty defaults (time-only sorting)",
			config: IngestConfig{
				SortKeys:        []string{},
				DefaultSortKeys: "",
			},
			wantSortKeys:    map[string][]string{},
			wantDefaultKeys: nil, // Empty means time-only; "time" is appended at usage site
			wantErr:         false,
		},
		{
			name: "single measurement with additional sort key",
			config: IngestConfig{
				SortKeys:        []string{"temperature:tag_sensor_id"},
				DefaultSortKeys: "",
			},
			wantSortKeys: map[string][]string{
				"temperature": {"tag_sensor_id"},
			},
			wantDefaultKeys: nil,
			wantErr:         false,
		},
		{
			name: "multiple measurements with additional sort keys",
			config: IngestConfig{
				SortKeys: []string{
					"temperature:tag_sensor_id",
					"cpu:tag_host",
				},
				DefaultSortKeys: "",
			},
			wantSortKeys: map[string][]string{
				"temperature": {"tag_sensor_id"},
				"cpu":         {"tag_host"},
			},
			wantDefaultKeys: nil,
			wantErr:         false,
		},
		{
			name: "custom default sort keys (additional columns)",
			config: IngestConfig{
				SortKeys:        []string{},
				DefaultSortKeys: "tag_host",
			},
			wantSortKeys:    map[string][]string{},
			wantDefaultKeys: []string{"tag_host"},
			wantErr:         false,
		},
		{
			name: "multiple default sort keys",
			config: IngestConfig{
				SortKeys:        []string{},
				DefaultSortKeys: "tag_host,tag_region",
			},
			wantSortKeys:    map[string][]string{},
			wantDefaultKeys: []string{"tag_host", "tag_region"},
			wantErr:         false,
		},
		{
			name: "default with spaces trimmed",
			config: IngestConfig{
				SortKeys:        []string{},
				DefaultSortKeys: " tag_host , tag_region ",
			},
			wantSortKeys:    map[string][]string{},
			wantDefaultKeys: []string{"tag_host", "tag_region"},
			wantErr:         false,
		},
		{
			name: "sort keys with spaces trimmed",
			config: IngestConfig{
				SortKeys:        []string{" temperature : tag_sensor_id , tag_location "},
				DefaultSortKeys: "",
			},
			wantSortKeys: map[string][]string{
				"temperature": {"tag_sensor_id", "tag_location"},
			},
			wantDefaultKeys: nil,
			wantErr:         false,
		},
		{
			name: "legacy config with time still works (time is just another column)",
			config: IngestConfig{
				SortKeys:        []string{"temperature:tag_sensor_id,time"},
				DefaultSortKeys: "time",
			},
			wantSortKeys: map[string][]string{
				"temperature": {"tag_sensor_id", "time"},
			},
			wantDefaultKeys: []string{"time"},
			wantErr:         false,
		},
		{
			name: "invalid format - no colon",
			config: IngestConfig{
				SortKeys:        []string{"temperature_tag_sensor_id"},
				DefaultSortKeys: "time",
			},
			wantSortKeys:    nil,
			wantDefaultKeys: nil,
			wantErr:         true,
		},
		{
			name: "invalid format - empty measurement",
			config: IngestConfig{
				SortKeys:        []string{":tag_sensor_id,time"},
				DefaultSortKeys: "time",
			},
			wantSortKeys:    nil,
			wantDefaultKeys: nil,
			wantErr:         true,
		},
		{
			name: "invalid format - empty sort key",
			config: IngestConfig{
				SortKeys:        []string{"temperature:,time"},
				DefaultSortKeys: "time",
			},
			wantSortKeys:    nil,
			wantDefaultKeys: nil,
			wantErr:         true,
		},
		{
			name: "invalid format - no keys after measurement",
			config: IngestConfig{
				SortKeys:        []string{"temperature:"},
				DefaultSortKeys: "time",
			},
			wantSortKeys:    nil,
			wantDefaultKeys: nil,
			wantErr:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSortKeys, gotDefaultKeys, err := ParseSortKeys(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSortKeys() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if !reflect.DeepEqual(gotSortKeys, tt.wantSortKeys) {
					t.Errorf("ParseSortKeys() gotSortKeys = %v, want %v", gotSortKeys, tt.wantSortKeys)
				}
				if !reflect.DeepEqual(gotDefaultKeys, tt.wantDefaultKeys) {
					t.Errorf("ParseSortKeys() gotDefaultKeys = %v, want %v", gotDefaultKeys, tt.wantDefaultKeys)
				}
			}
		})
	}
}
