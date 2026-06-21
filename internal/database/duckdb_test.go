package database

import (
	"strings"
	"testing"
)

func TestEscapeSQLString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no quotes",
			input:    "simple_value",
			expected: "simple_value",
		},
		{
			name:     "single quote",
			input:    "value'with'quotes",
			expected: "value''with''quotes",
		},
		{
			name:     "sql injection attempt",
			input:    "test'; DROP TABLE data; --",
			expected: "test''; DROP TABLE data; --",
		},
		{
			name:     "multiple consecutive quotes",
			input:    "a'''b",
			expected: "a''''''b",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "only quotes",
			input:    "'''",
			expected: "''''''",
		},
		{
			name:     "realistic s3 secret key",
			input:    "wJalrXUtnFEMI/K7MDENG/bPxRfiCY'EXAMPLE",
			expected: "wJalrXUtnFEMI/K7MDENG/bPxRfiCY''EXAMPLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeSQLString(tt.input)
			if result != tt.expected {
				t.Errorf("escapeSQLString(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestStripURLScheme(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "http scheme",
			input:    "http://minio:9000",
			expected: "minio:9000",
		},
		{
			name:     "https scheme",
			input:    "https://s3.amazonaws.com",
			expected: "s3.amazonaws.com",
		},
		{
			name:     "no scheme passthrough",
			input:    "minio:9000",
			expected: "minio:9000",
		},
		{
			name:     "localhost no scheme",
			input:    "localhost:9000",
			expected: "localhost:9000",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "https with port",
			input:    "https://garage.example.com:3900",
			expected: "garage.example.com:3900",
		},
		{
			name:     "scheme not at start does not match",
			input:    "weird-host-http://name",
			expected: "weird-host-http://name",
		},
		{
			name:     "uppercase HTTP scheme",
			input:    "HTTP://minio:9000",
			expected: "minio:9000",
		},
		{
			name:     "uppercase HTTPS scheme",
			input:    "HTTPS://s3.amazonaws.com",
			expected: "s3.amazonaws.com",
		},
		{
			name:     "mixed case Http scheme",
			input:    "Http://minio:9000",
			expected: "minio:9000",
		},
		{
			name:     "mixed case Https scheme",
			input:    "Https://s3.amazonaws.com",
			expected: "s3.amazonaws.com",
		},
		{
			name:     "preserve case in remainder",
			input:    "http://MyBucket.example.com",
			expected: "MyBucket.example.com",
		},
		{
			name:     "trim trailing slash",
			input:    "http://minio:9000/",
			expected: "minio:9000",
		},
		{
			name:     "trim multiple trailing slashes",
			input:    "https://s3.amazonaws.com///",
			expected: "s3.amazonaws.com",
		},
		{
			name:     "trim trailing slash with no scheme",
			input:    "minio:9000/",
			expected: "minio:9000",
		},
		{
			name:     "trim leading and trailing whitespace",
			input:    "  http://minio:9000  ",
			expected: "minio:9000",
		},
		{
			name:     "trim whitespace and trailing slash combined",
			input:    "  https://s3.amazonaws.com/  ",
			expected: "s3.amazonaws.com",
		},
		{
			name:     "whitespace only is empty",
			input:    "   ",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripURLScheme(tt.input)
			if result != tt.expected {
				t.Errorf("stripURLScheme(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestBuildS3SecretSQL(t *testing.T) {
	t.Run("AWS minimal (region only)", func(t *testing.T) {
		got := buildS3SecretSQL("AKIA", "secretval", "us-east-1", "", false, true)
		mustContain(t, got, "CREATE OR REPLACE SECRET arc_s3")
		mustContain(t, got, "TYPE S3")
		mustContain(t, got, "KEY_ID 'AKIA'")
		mustContain(t, got, "SECRET 'secretval'")
		mustContain(t, got, "REGION 'us-east-1'")
		mustContain(t, got, "URL_STYLE 'vhost'")
		mustContain(t, got, "USE_SSL true")
		if strings.Contains(got, "ENDPOINT") {
			t.Errorf("empty endpoint should be omitted, got:\n%s", got)
		}
	})

	t.Run("MinIO (endpoint, path-style, no SSL)", func(t *testing.T) {
		got := buildS3SecretSQL("key", "sec", "", "http://minio.local:9000", true, false)
		// endpoint must be scheme-stripped
		mustContain(t, got, "ENDPOINT 'minio.local:9000'")
		mustContain(t, got, "URL_STYLE 'path'")
		mustContain(t, got, "USE_SSL false")
		if strings.Contains(got, "REGION") {
			t.Errorf("empty region should be omitted, got:\n%s", got)
		}
	})

	t.Run("single quotes are escaped", func(t *testing.T) {
		// A secret key or region containing a quote must not break out of the
		// SQL string literal.
		got := buildS3SecretSQL("ak'); DROP", "se'cret", "re'gion", "ep'host", false, true)
		mustContain(t, got, "KEY_ID 'ak''); DROP'")
		mustContain(t, got, "SECRET 'se''cret'")
		mustContain(t, got, "REGION 're''gion'")
		mustContain(t, got, "ENDPOINT 'ep''host'")
	})
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected SQL to contain %q, got:\n%s", needle, haystack)
	}
}
