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
		got, err := buildS3SecretSQL("AKIA", "secretval", "us-east-1", "", false, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
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
		if strings.Contains(got, "CREDENTIAL_CHAIN") {
			t.Errorf("static keys must not use the credential chain, got:\n%s", got)
		}
	})

	t.Run("MinIO (endpoint, path-style, no SSL)", func(t *testing.T) {
		got, err := buildS3SecretSQL("key", "sec", "", "http://minio.local:9000", true, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// endpoint must be scheme-stripped
		mustContain(t, got, "ENDPOINT 'minio.local:9000'")
		mustContain(t, got, "URL_STYLE 'path'")
		mustContain(t, got, "USE_SSL false")
		if strings.Contains(got, "REGION") {
			t.Errorf("empty region should be omitted, got:\n%s", got)
		}
	})

	t.Run("no keys -> credential chain", func(t *testing.T) {
		// Both keys empty: defer to the AWS credential chain (IAM role / IRSA /
		// env). Verified separately against live DuckDB that CREDENTIAL_CHAIN
		// composes with the endpoint params.
		got, err := buildS3SecretSQL("", "", "us-east-1", "minio.local:9000", true, false)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "PROVIDER CREDENTIAL_CHAIN")
		mustContain(t, got, "REGION 'us-east-1'")
		mustContain(t, got, "ENDPOINT 'minio.local:9000'")
		if strings.Contains(got, "KEY_ID") || strings.Contains(got, "SECRET '") {
			t.Errorf("credential chain must not emit KEY_ID/SECRET, got:\n%s", got)
		}
	})

	t.Run("exactly one key set -> error", func(t *testing.T) {
		// Asymmetric config is a misconfiguration trap: silently routing to the
		// credential chain would discard the provided key.
		if _, err := buildS3SecretSQL("AKIA", "", "us-east-1", "", false, true); err == nil {
			t.Error("access key without secret key should error")
		}
		if _, err := buildS3SecretSQL("", "secretval", "us-east-1", "", false, true); err == nil {
			t.Error("secret key without access key should error")
		}
	})

	t.Run("malformed endpoint strips to empty -> omitted", func(t *testing.T) {
		// "http://" strips to "" and must not emit an empty ENDPOINT '' clause.
		got, err := buildS3SecretSQL("k", "s", "us-east-1", "http://", false, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.Contains(got, "ENDPOINT") {
			t.Errorf("endpoint that strips to empty should be omitted, got:\n%s", got)
		}
	})

	t.Run("single quotes are escaped", func(t *testing.T) {
		// A secret key or region containing a quote must not break out of the
		// SQL string literal.
		got, err := buildS3SecretSQL("ak'); DROP", "se'cret", "re'gion", "ep'host", false, true)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
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
