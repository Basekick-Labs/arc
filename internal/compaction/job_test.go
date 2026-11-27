package compaction

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBuildOrderByClause tests the ORDER BY clause generation
func TestBuildOrderByClause(t *testing.T) {
	tests := []struct {
		name     string
		sortKeys []string
		want     string
	}{
		{
			name:     "empty sort keys",
			sortKeys: []string{},
			want:     "",
		},
		{
			name:     "nil sort keys",
			sortKeys: nil,
			want:     "",
		},
		{
			name:     "single key",
			sortKeys: []string{"time"},
			want:     `ORDER BY "time"`,
		},
		{
			name:     "two keys",
			sortKeys: []string{"tag_sensor_id", "time"},
			want:     `ORDER BY epoch_us("time") // 3600000000, "tag_sensor_id", "time"`,
		},
		{
			name:     "three keys",
			sortKeys: []string{"tag_region", "tag_host", "time"},
			want:     `ORDER BY epoch_us("time") // 3600000000, "tag_region", "tag_host", "time"`,
		},
		{
			// A legacy config that puts time first already orders by time;
			// the hour prefix would be redundant.
			name:     "time first (legacy config)",
			sortKeys: []string{"time", "host"},
			want:     `ORDER BY "time", "host"`,
		},
		{
			name:     "quote in key is escaped after the hour prefix",
			sortKeys: []string{`a"b`, "time"},
			want:     `ORDER BY epoch_us("time") // 3600000000, "a""b", "time"`,
		},
		{
			name:     "keys with special characters",
			sortKeys: []string{"my-column", "tag_host", "time"},
			want:     `ORDER BY epoch_us("time") // 3600000000, "my-column", "tag_host", "time"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildOrderByClause(tt.sortKeys)
			if got != tt.want {
				t.Errorf("buildOrderByClause() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestEscapeSQLPath tests SQL path escaping
func TestEscapeSQLPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "no quotes",
			path: "/tmp/file.parquet",
			want: "/tmp/file.parquet",
		},
		{
			name: "single quote",
			path: "/tmp/file's.parquet",
			want: "/tmp/file''s.parquet",
		},
		{
			name: "multiple quotes",
			path: "/tmp/'file'test'.parquet",
			want: "/tmp/''file''test''.parquet",
		},
		{
			name: "only quotes",
			path: "'''",
			want: "''''''",
		},
		{
			name: "backslash",
			path: `back\slash`,
			want: `back\slash`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := escapeSQLPath(tt.path)
			if got != tt.want {
				t.Errorf("escapeSQLPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestValidateParquetFile tests the lightweight parquet magic byte validation
func TestValidateParquetFile(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name      string
		content   []byte
		wantError bool
		errorMsg  string
	}{
		{
			name:      "valid parquet magic bytes",
			content:   append(append([]byte("PAR1"), make([]byte, 100)...), []byte("PAR1")...),
			wantError: false,
		},
		{
			name:      "file too small",
			content:   []byte("PAR1PAR1"),
			wantError: true,
			errorMsg:  "file too small",
		},
		{
			name:      "invalid header",
			content:   append(append([]byte("XXXX"), make([]byte, 100)...), []byte("PAR1")...),
			wantError: true,
			errorMsg:  "invalid parquet magic header",
		},
		{
			name:      "invalid footer",
			content:   append(append([]byte("PAR1"), make([]byte, 100)...), []byte("XXXX")...),
			wantError: true,
			errorMsg:  "invalid parquet magic footer",
		},
		{
			name:      "empty file",
			content:   []byte{},
			wantError: true,
			errorMsg:  "file too small",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp file with test content
			filePath := filepath.Join(tempDir, tt.name+".parquet")
			if err := os.WriteFile(filePath, tt.content, 0644); err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			err := validateParquetFile(filePath)

			if tt.wantError {
				if err == nil {
					t.Errorf("validateParquetFile() expected error containing %q, got nil", tt.errorMsg)
				} else if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("validateParquetFile() error = %q, want error containing %q", err.Error(), tt.errorMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateParquetFile() unexpected error: %v", err)
				}
			}
		})
	}
}

// TestValidateParquetFile_NonExistentFile tests validation of a non-existent file
func TestValidateParquetFile_NonExistentFile(t *testing.T) {
	err := validateParquetFile("/nonexistent/path/file.parquet")
	if err == nil {
		t.Error("validateParquetFile() expected error for non-existent file, got nil")
	}
}

// contains checks if s contains substr
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && searchString(s, substr)))
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestCompactionSortKeys covers the filter that keeps a configured sort key
// without a column out of the compaction ORDER BY (a key that cannot bind fails
// the partition's compaction on every cycle).
func TestCompactionSortKeys(t *testing.T) {
	cols := map[string]string{"fleet": "fleet", "name": "Name", "time": "time"}
	tests := []struct {
		name        string
		keys        []string
		columns     map[string]string
		wantKept    []string
		wantDropped []string
	}{
		{"all present", []string{"fleet", "name", "time"}, cols, []string{"fleet", "name", "time"}, nil},
		// DuckDB binds names case-insensitively, and the probe map is keyed
		// lower-case: a key spelled differently from its column is present.
		{"case-insensitive match", []string{"FLEET", "Name", "time"}, cols, []string{"FLEET", "Name", "time"}, nil},
		{"missing key dropped, order kept", []string{"site", "fleet", "time"}, cols, []string{"fleet", "time"}, []string{"site"}},
		{"only time left", []string{"site", "time"}, cols, []string{"time"}, []string{"site"}},
		// The probe failed: keep every key and let the COPY report the error.
		{"nil columns keeps all", []string{"site", "time"}, nil, []string{"site", "time"}, nil},
		// time is kept even when absent from the map; the caller already
		// refused a batch without a time column.
		{"time always kept", []string{"fleet", "time"}, map[string]string{"fleet": "fleet"}, []string{"fleet", "time"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kept, dropped := compactionSortKeys(tt.keys, tt.columns)
			if strings.Join(kept, ",") != strings.Join(tt.wantKept, ",") {
				t.Errorf("kept = %v, want %v", kept, tt.wantKept)
			}
			if strings.Join(dropped, ",") != strings.Join(tt.wantDropped, ",") {
				t.Errorf("dropped = %v, want %v", dropped, tt.wantDropped)
			}
		})
	}

	// The result never aliases the input: the input may be the shared config slice.
	in := []string{"fleet", "time"}
	kept, _ := compactionSortKeys(in, nil)
	kept[0] = "changed"
	if in[0] != "fleet" {
		t.Fatal("compactionSortKeys returned a slice sharing the input's backing array")
	}
}

// TestGetSortKeysDoesNotWriteIntoConfig pins that appending "time" never writes
// into the config slice's spare capacity, which concurrent compaction jobs share.
func TestGetSortKeysDoesNotWriteIntoConfig(t *testing.T) {
	shared := make([]string, 1, 4)
	shared[0] = "fleet"
	m := &Manager{SortKeysConfig: map[string][]string{"m": shared}, DefaultSortKeys: shared}
	a := m.GetSortKeys("m")
	b := m.GetSortKeys("other")
	a[1] = "x"
	if b[1] != "time" || shared[:2][1] == "x" {
		t.Fatalf("GetSortKeys appended into the shared config slice: a=%v b=%v", a, b)
	}
	if !m.hasMeasurementSortKeys("m") || m.hasMeasurementSortKeys("other") {
		t.Fatal("hasMeasurementSortKeys does not tell sort_keys from default_sort_keys")
	}
}
