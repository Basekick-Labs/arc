package database

import "testing"

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
