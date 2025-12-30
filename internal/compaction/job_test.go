package compaction

import (
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
