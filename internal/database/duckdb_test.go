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
	t.Run("AWS minimal (region only), named + scoped", func(t *testing.T) {
		got, err := buildS3SecretSQL(s3SecretParams{
			name: arcS3PrimarySecretName, scope: "s3://primary-bucket/",
			accessKey: "AKIA", secretKey: "secretval", region: "us-east-1", useSSL: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "CREATE OR REPLACE SECRET arc_s3_primary")
		mustContain(t, got, "TYPE S3")
		mustContain(t, got, "KEY_ID 'AKIA'")
		mustContain(t, got, "SECRET 'secretval'")
		mustContain(t, got, "REGION 'us-east-1'")
		mustContain(t, got, "URL_STYLE 'vhost'")
		mustContain(t, got, "USE_SSL true")
		mustContain(t, got, "SCOPE 's3://primary-bucket/'")
		if strings.Contains(got, "ENDPOINT") {
			t.Errorf("empty endpoint should be omitted, got:\n%s", got)
		}
		if strings.Contains(got, "CREDENTIAL_CHAIN") {
			t.Errorf("static keys must not use the credential chain, got:\n%s", got)
		}
	})

	t.Run("MinIO (endpoint, path-style, no SSL)", func(t *testing.T) {
		got, err := buildS3SecretSQL(s3SecretParams{
			name:      arcS3PrimarySecretName,
			accessKey: "key", secretKey: "sec", endpoint: "http://minio.local:9000", pathStyle: true,
		})
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
		if strings.Contains(got, "SCOPE") {
			t.Errorf("empty scope should be omitted, got:\n%s", got)
		}
	})

	t.Run("no keys -> credential chain", func(t *testing.T) {
		// Both keys empty: defer to the AWS credential chain (IAM role / IRSA /
		// env). Verified separately against live DuckDB that CREDENTIAL_CHAIN
		// composes with the endpoint params.
		got, err := buildS3SecretSQL(s3SecretParams{
			name: arcS3ColdSecretName, region: "us-east-1", endpoint: "minio.local:9000", pathStyle: true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "CREATE OR REPLACE SECRET arc_s3_cold")
		mustContain(t, got, "PROVIDER CREDENTIAL_CHAIN")
		mustContain(t, got, "REGION 'us-east-1'")
		mustContain(t, got, "ENDPOINT 'minio.local:9000'")
		if strings.Contains(got, "KEY_ID") || strings.Contains(got, "SECRET '") {
			t.Errorf("credential chain must not emit KEY_ID/SECRET, got:\n%s", got)
		}
	})

	t.Run("primary IRSA (no keys) -> scoped credential chain", func(t *testing.T) {
		// storage.backend=="s3" with empty keys (IRSA / IAM role): the widened
		// gate in configureDatabase now calls configureS3Access, which builds the
		// PRIMARY secret with no keys -> PROVIDER CREDENTIAL_CHAIN, scoped to the
		// primary bucket/prefix so it coexists with a cold-tier secret without
		// clobbering it. This is the bug fix: previously no primary secret was
		// created and s3:// query reads went unauthenticated.
		got, err := buildS3SecretSQL(s3SecretParams{
			name:  arcS3PrimarySecretName,
			scope: s3SecretScope("primary-bucket", "hot/"),
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "CREATE OR REPLACE SECRET "+arcS3PrimarySecretName)
		mustContain(t, got, "PROVIDER CREDENTIAL_CHAIN")
		mustContain(t, got, "SCOPE 's3://primary-bucket/hot/'")
		if strings.Contains(got, "KEY_ID") || strings.Contains(got, "SECRET '") {
			t.Errorf("IRSA primary secret must not emit KEY_ID/SECRET, got:\n%s", got)
		}
	})

	t.Run("exactly one key set -> error", func(t *testing.T) {
		// Asymmetric config is a misconfiguration trap: silently routing to the
		// credential chain would discard the provided key.
		if _, err := buildS3SecretSQL(s3SecretParams{name: "x", accessKey: "AKIA", region: "us-east-1"}); err == nil {
			t.Error("access key without secret key should error")
		}
		if _, err := buildS3SecretSQL(s3SecretParams{name: "x", secretKey: "secretval", region: "us-east-1"}); err == nil {
			t.Error("secret key without access key should error")
		}
	})

	t.Run("malformed endpoint strips to empty -> omitted", func(t *testing.T) {
		// "http://" strips to "" and must not emit an empty ENDPOINT '' clause.
		got, err := buildS3SecretSQL(s3SecretParams{name: "x", accessKey: "k", secretKey: "s", region: "us-east-1", endpoint: "http://"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if strings.Contains(got, "ENDPOINT") {
			t.Errorf("endpoint that strips to empty should be omitted, got:\n%s", got)
		}
	})

	t.Run("single quotes escaped (incl. scope)", func(t *testing.T) {
		// A secret key/region/scope containing a quote must not break out of the
		// SQL string literal.
		got, err := buildS3SecretSQL(s3SecretParams{
			name: "x", scope: "s3://b'k/",
			accessKey: "ak'); DROP", secretKey: "se'cret", region: "re'gion", endpoint: "ep'host",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "KEY_ID 'ak''); DROP'")
		mustContain(t, got, "SECRET 'se''cret'")
		mustContain(t, got, "REGION 're''gion'")
		mustContain(t, got, "ENDPOINT 'ep''host'")
		mustContain(t, got, "SCOPE 's3://b''k/'")
	})
}

func TestS3SecretScope(t *testing.T) {
	tests := []struct {
		bucket, prefix, want string
	}{
		{"", "", ""},                        // no bucket -> unscoped
		{"", "p", ""},                       // prefix without bucket -> unscoped
		{"bkt", "", "s3://bkt/"},            // bucket only
		{"bkt", "data", "s3://bkt/data/"},   // bucket + prefix
		{"bkt", "/data/", "s3://bkt/data/"}, // leading/trailing slashes normalized
	}
	for _, tt := range tests {
		if got := s3SecretScope(tt.bucket, tt.prefix); got != tt.want {
			t.Errorf("s3SecretScope(%q, %q) = %q, want %q", tt.bucket, tt.prefix, got, tt.want)
		}
	}
}

func TestBuildAzureSecretSQL(t *testing.T) {
	t.Run("account key -> connection string, named + scoped", func(t *testing.T) {
		got, err := buildAzureSecretSQL(azureSecretParams{
			name: arcAzurePrimarySecretName, scope: "azure://primary/",
			accountName: "acct", accountKey: "key==",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "CREATE OR REPLACE SECRET azure_secret_primary")
		mustContain(t, got, "TYPE AZURE")
		mustContain(t, got, "CONNECTION_STRING 'AccountName=acct;AccountKey=key=='")
		mustContain(t, got, "SCOPE 'azure://primary/'")
		if strings.Contains(got, "CREDENTIAL_CHAIN") {
			t.Errorf("account key must not use credential chain, got:\n%s", got)
		}
	})

	t.Run("no key -> credential chain", func(t *testing.T) {
		got, err := buildAzureSecretSQL(azureSecretParams{
			name: arcAzureColdSecretName, scope: "azure://cold/", accountName: "acct",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "CREATE OR REPLACE SECRET azure_secret_cold")
		mustContain(t, got, "PROVIDER CREDENTIAL_CHAIN")
		mustContain(t, got, "ACCOUNT_NAME 'acct'")
		mustContain(t, got, "SCOPE 'azure://cold/'")
		if strings.Contains(got, "CONNECTION_STRING") {
			t.Errorf("credential chain must not emit CONNECTION_STRING, got:\n%s", got)
		}
	})

	t.Run("connection string -> CONNECTION_STRING, no account name needed", func(t *testing.T) {
		// A connection string embeds the account identity, so account name may be
		// empty (mirrors the Go backend's first auth case).
		got, err := buildAzureSecretSQL(azureSecretParams{
			name: arcAzurePrimarySecretName, scope: "azure://primary/",
			connectionString: "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=key==;EndpointSuffix=core.windows.net",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "CONNECTION_STRING 'DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=key==;EndpointSuffix=core.windows.net'")
		if strings.Contains(got, "CREDENTIAL_CHAIN") || strings.Contains(got, "ACCOUNT_NAME") {
			t.Errorf("connection string must not emit CREDENTIAL_CHAIN/ACCOUNT_NAME, got:\n%s", got)
		}
	})

	t.Run("connection string takes precedence over account name/key", func(t *testing.T) {
		got, err := buildAzureSecretSQL(azureSecretParams{
			name: "x", connectionString: "ConnStr", accountName: "acct", accountKey: "key==",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "CONNECTION_STRING 'ConnStr'")
		if strings.Contains(got, "AccountName=acct") {
			t.Errorf("connection string must take precedence over synthesized conn string, got:\n%s", got)
		}
	})

	t.Run("no account name and no connection string -> error", func(t *testing.T) {
		if _, err := buildAzureSecretSQL(azureSecretParams{name: "x", accountKey: "k"}); err == nil {
			t.Error("missing both account name and connection string should error")
		}
	})

	t.Run("connection string single quotes escaped", func(t *testing.T) {
		// The operator-supplied connection string is interpolated into the
		// CREATE SECRET literal; an embedded quote must be doubled, not break out.
		got, err := buildAzureSecretSQL(azureSecretParams{
			name: "x", connectionString: "AccountName=a;SharedAccessSignature=sig'inject",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		mustContain(t, got, "CONNECTION_STRING 'AccountName=a;SharedAccessSignature=sig''inject'")
	})

	t.Run("single quotes escaped", func(t *testing.T) {
		got, err := buildAzureSecretSQL(azureSecretParams{
			name: "x", scope: "azure://c'/", accountName: "ac't", accountKey: "k'y",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// connection string embeds both escaped values inside one literal
		mustContain(t, got, "CONNECTION_STRING 'AccountName=ac''t;AccountKey=k''y'")
		mustContain(t, got, "SCOPE 'azure://c''/'")
	})
}

func TestAzureScope(t *testing.T) {
	tests := []struct{ container, want string }{
		{"", ""},
		{"c1", "azure://c1/"},
	}
	for _, tt := range tests {
		if got := azureScope(tt.container); got != tt.want {
			t.Errorf("azureScope(%q) = %q, want %q", tt.container, got, tt.want)
		}
	}
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected SQL to contain %q, got:\n%s", needle, haystack)
	}
}
