package compaction

import (
	"os"
	"path/filepath"
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
			want:     `ORDER BY "tag_sensor_id", "time"`,
		},
		{
			name:     "three keys",
			sortKeys: []string{"tag_region", "tag_host", "time"},
			want:     `ORDER BY "tag_region", "tag_host", "time"`,
		},
		{
			name:     "keys with special characters",
			sortKeys: []string{"my-column", "tag_host", "time"},
			want:     `ORDER BY "my-column", "tag_host", "time"`,
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
