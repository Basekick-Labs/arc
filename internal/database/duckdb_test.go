package database

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
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

// hermeticAWSEnv makes a test's SDK credential resolution deterministic:
// scrubs ambient AWS_* credentials, points the shared config files at
// /dev/null, and disables the IMDS probe. Without this, any test that reaches
// newAWSCredProvider would resolve the developer's ~/.aws credentials (or, on
// an EC2/EKS CI runner, REAL instance credentials) and pay a measured 4-5s
// IMDS probe on machines without one (#601 review F10).
func hermeticAWSEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
		"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_PROFILE",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
	} {
		t.Setenv(k, "")
	}
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
	t.Setenv("AWS_CONFIG_FILE", "/dev/null")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
}

// TestS3CredentialMode pins the two-way routing (#601): configured keys emit
// directly; anything else is SDK-managed (the refresher decides the rest at
// resolve time).
func TestS3CredentialMode(t *testing.T) {
	if got := s3CredentialMode("AKIA", "sk"); got != s3ModeStaticKeys {
		t.Errorf("static keys => %q, want %q", got, s3ModeStaticKeys)
	}
	if got := s3CredentialMode("", ""); got != s3ModeSDKManaged {
		t.Errorf("no keys => %q, want %q", got, s3ModeSDKManaged)
	}
	// Half-configured pairs still route to direct emission, where
	// buildS3SecretSQL rejects them with a clear error (unchanged from #600).
	if got := s3CredentialMode("AKIA", ""); got != s3ModeStaticKeys {
		t.Errorf("half pair => %q, want %q", got, s3ModeStaticKeys)
	}
}

// TestBuildS3SecretSQL_SessionToken pins the static-credentials emission the
// refresher (s3refresh.go) depends on, and that no shape emits CHAIN/REFRESH
// (the DuckDB-side web_identity mechanism was removed after live testing proved
// it never refreshes for Arc's globbed reads, #600). VALIDATION 'none' is
// required on the chain fallback and forbidden on static shapes — see the
// sub-test.
func TestBuildS3SecretSQL_SessionToken(t *testing.T) {
	t.Run("keys + session token emit SESSION_TOKEN", func(t *testing.T) {
		got, err := buildS3SecretSQL(s3SecretParams{
			name: arcS3PrimarySecretName, accessKey: "ASIAKEY", secretKey: "sk",
			sessionToken: "tok'en//with=quirks", region: "us-gov-west-1",
			scope: "s3://bucket/", useSSL: true,
		})
		if err != nil {
			t.Fatalf("buildS3SecretSQL: %v", err)
		}
		mustContain(t, got, "KEY_ID 'ASIAKEY'")
		// single quote doubled by escapeSQLString
		mustContain(t, got, "SESSION_TOKEN 'tok''en//with=quirks'")
	})

	t.Run("session token without keys is rejected", func(t *testing.T) {
		if _, err := buildS3SecretSQL(s3SecretParams{
			name: "x", sessionToken: "tok", region: "us-east-1",
		}); err == nil {
			t.Fatal("want error for session token without static keys")
		}
	})

	t.Run("no shape ever emits DuckDB-side refresh clauses", func(t *testing.T) {
		// CHAIN pinning and REFRESH were the #600 dead end — verified live to
		// never refresh for Arc's globbed reads. Nothing may emit them.
		for _, p := range []s3SecretParams{
			{name: "a", accessKey: "k", secretKey: "s", sessionToken: "t", region: "r", useSSL: true},
			{name: "b", region: "r", useSSL: true}, // chain fallback
		} {
			got, err := buildS3SecretSQL(p)
			if err != nil {
				t.Fatalf("buildS3SecretSQL(%q): %v", p.name, err)
			}
			for _, forbidden := range []string{"CHAIN '", "REFRESH"} {
				if strings.Contains(got, forbidden) {
					t.Errorf("secret %q must not contain %q, got:\n%s", p.name, forbidden, got)
				}
			}
		}
		// Static secrets must not carry VALIDATION; the chain fallback MUST —
		// DuckDB fails a chain-secret CREATE when nothing resolves, and the
		// fallback exists precisely for that situation.
		staticSQL, _ := buildS3SecretSQL(s3SecretParams{name: "a", accessKey: "k", secretKey: "s", region: "r", useSSL: true})
		if strings.Contains(staticSQL, "VALIDATION") {
			t.Errorf("static secret must not carry VALIDATION:\n%s", staticSQL)
		}
		chainSQL, _ := buildS3SecretSQL(s3SecretParams{name: "b", region: "r", useSSL: true})
		if !strings.Contains(chainSQL, "VALIDATION 'none'") {
			t.Errorf("chain fallback must carry VALIDATION 'none':\n%s", chainSQL)
		}
	})
}

// TestSessionTokenSecretExecutesAndRedacts runs the refresher-shaped secret
// against real DuckDB: the statement must be accepted (incl. quirky token
// bytes), and SESSION_TOKEN must be redacted in duckdb_secrets() — the secret
// manager is readable by any authenticated query user, so a visible token
// would hand out live AWS credentials.
func TestSessionTokenSecretExecutesAndRedacts(t *testing.T) {
	db, err := sql.Open("duckdb", "?allow_persistent_secrets=false")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		t.Skipf("httpfs unavailable (offline?): %v", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		t.Skipf("httpfs unavailable (offline?): %v", err)
	}

	const token = "FwoGZXIvYXdzEBa//////////wEaDD'quote+slash=="
	stmt, err := buildS3SecretSQL(s3SecretParams{
		name: arcS3PrimarySecretName, accessKey: "ASIATEST", secretKey: "secretval",
		sessionToken: token, region: "us-east-1", scope: "s3://b/", useSSL: true,
	})
	if err != nil {
		t.Fatalf("buildS3SecretSQL: %v", err)
	}
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("DuckDB rejected refresher-shaped secret: %v\nSQL:\n%s", err, stmt)
	}

	var secretString string
	if err := db.QueryRow(
		"SELECT secret_string FROM duckdb_secrets() WHERE name = ?", arcS3PrimarySecretName,
	).Scan(&secretString); err != nil {
		t.Fatalf("secret not registered: %v", err)
	}
	if strings.Contains(secretString, "quote+slash") || strings.Contains(secretString, "secretval") {
		t.Fatalf("credential material visible in duckdb_secrets(): %s", secretString)
	}
	mustContain(t, secretString, "session_token=redacted")
}

// TestConfigureS3ColdTierStartsRefresher pins the cold-tier wiring for #600:
// under IRSA (no static keys + web-identity env), ConfigureS3 must route to the
// credential refresher, whose synchronous first resolve emits a static-key
// secret with a session token BEFORE ConfigureS3 returns. Without this test,
// deleting the refresher branch from ConfigureS3 compiles and passes the suite,
// silently leaving tiered-storage IRSA deployments on the expiring path.
func TestConfigureS3ColdTierStartsRefresher(t *testing.T) {
	hermeticAWSEnv(t)

	// Inject a deterministic provider; restore the SDK one afterwards.
	orig := newAWSCredProvider
	fake := &fakeCredProvider{creds: aws.Credentials{
		AccessKeyID: "ASIAFAKECOLD", SecretAccessKey: "fakesecret",
		SessionToken: "faketoken", CanExpire: true,
		Expires: time.Now().Add(time.Hour),
	}}
	newAWSCredProvider = func(ctx context.Context, region string) (awsCredentialsProvider, error) {
		return fake, nil
	}
	t.Cleanup(func() { newAWSCredProvider = orig })

	tmp := t.TempDir()
	storageRoot := filepath.Join(tmp, "data")
	if err := os.MkdirAll(storageRoot, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	db, err := New(&Config{
		MaxConnections:   2,
		MemoryLimit:      "256MB",
		LocalStorageRoot: storageRoot,
		TempDirectory:    tmp,
		ColdS3Bucket:     "cold-bucket",
	}, zerolog.Nop())
	if err != nil {
		if strings.Contains(err.Error(), "httpfs") {
			t.Skipf("httpfs unavailable (offline?): %v", err)
		}
		t.Fatalf("New: %v", err)
	}
	defer db.Close()

	if err := db.ConfigureS3(&S3Config{Region: "us-gov-west-1", Bucket: "cold-bucket"}); err != nil {
		t.Fatalf("ConfigureS3: %v", err)
	}

	// The refresher's synchronous first resolve must have emitted the secret
	// already — no waiting, no goroutine race.
	var secretString string
	if err := db.DB().QueryRow(
		"SELECT secret_string FROM duckdb_secrets() WHERE name = ?", arcS3ColdSecretName,
	).Scan(&secretString); err != nil {
		t.Fatalf("cold secret not registered after ConfigureS3: %v", err)
	}
	mustContain(t, secretString, "key_id=ASIAFAKECOLD")
	mustContain(t, secretString, "session_token=redacted")
	if fake.calls.Load() == 0 {
		t.Fatal("injected provider never called — refresher not wired")
	}
	if db.s3Refreshers[arcS3ColdSecretName] == nil {
		t.Fatal("refresher not registered on the DuckDB struct — Close cannot stop it")
	}
}

// fakeCredProvider is a deterministic awsCredentialsProvider for wiring tests.
type fakeCredProvider struct {
	creds aws.Credentials
	calls atomic.Int64
	err   error
}

func (f *fakeCredProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	f.calls.Add(1)
	if f.err != nil {
		return aws.Credentials{}, f.err
	}
	return f.creds, nil
}
