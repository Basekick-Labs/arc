package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/rs/zerolog"
)

// TestCredentialsCacheRequiresInvalidateForProactiveRefresh models the REAL
// refresher timeline against the real aws.CredentialsCache and pins the lesson
// the first live merge-gate run taught (2026-08-18): a proactive Retrieve
// BEFORE the cache considers the credentials expired returns the SAME session —
// an ExpiryWindow does not change that (it is applied once at store time,
// shifting the stored expiry; the cache still serves until that instant). The
// only way to make a proactive refresh produce a new session is Invalidate().
//
// The first version of this test set the fake's expiry already inside the
// window at store time, so it "proved" the window forced re-resolves — the
// wrong sub-case. This version stores an hour-long session and retrieves at
// the refresher's actual fire point.
func TestCredentialsCacheRequiresInvalidateForProactiveRefresh(t *testing.T) {
	fake := &fakeCredProvider{creds: aws.Credentials{
		AccessKeyID: "K1", SecretAccessKey: "S", SessionToken: "T1",
		CanExpire: true, Expires: time.Now().Add(time.Hour),
	}}
	cache := aws.NewCredentialsCache(fake)

	first, err := cache.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// A fresh session is available upstream (as it would be from STS)...
	fake.creds.SessionToken = "T2"
	fake.creds.Expires = time.Now().Add(2 * time.Hour)

	// ...but a plain proactive Retrieve (what the refresher does at
	// Expires−10min) still serves the cached session.
	again, err := cache.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if again.SessionToken != first.SessionToken || fake.calls.Load() != 1 {
		t.Fatalf("expected cached session on plain Retrieve, got token=%q inner_calls=%d",
			again.SessionToken, fake.calls.Load())
	}

	// Invalidate is what makes the proactive refresh real.
	cache.Invalidate()
	fresh, err := cache.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fresh.SessionToken != "T2" || !fresh.Expires.After(first.Expires) {
		t.Fatalf("Invalidate+Retrieve must yield the new session, got token=%q", fresh.SessionToken)
	}
}

// TestResolveAndEmitInvalidatesOnProactiveRefresh pins the wiring: the
// scheduled-refresh path must call Invalidate on providers that support it,
// and the initial fill must not.
func TestResolveAndEmitInvalidatesOnProactiveRefresh(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	fake := &invalidatingFakeProvider{}
	fake.current = aws.Credentials{
		AccessKeyID: "K1", SecretAccessKey: "S", SessionToken: "T1",
		CanExpire: true, Expires: time.Now().Add(time.Hour),
	}
	fake.next = aws.Credentials{
		AccessKeyID: "K2", SecretAccessKey: "S2", SessionToken: "T2",
		CanExpire: true, Expires: time.Now().Add(2 * time.Hour),
	}
	r := &s3CredentialRefresher{
		db: db, provider: fake, logger: zerolog.Nop(),
		params: s3SecretParams{name: "inv_test", region: "us-east-1", useSSL: true},
	}
	if _, err := r.resolveAndEmit(context.Background(), false); err != nil {
		t.Fatalf("initial fill: %v", err)
	}
	if fake.invalidations.Load() != 0 {
		t.Fatal("initial fill must not invalidate")
	}
	// Without invalidation the provider keeps serving the current session, so a
	// proactive resolve would be non-advancing; WITH it, the new session lands.
	creds, err := r.resolveAndEmit(context.Background(), true)
	if err != nil {
		t.Fatalf("proactive refresh: %v", err)
	}
	if fake.invalidations.Load() != 1 || creds.SessionToken != "T2" {
		t.Fatalf("proactive refresh must invalidate then get the new session; invalidations=%d token=%q",
			fake.invalidations.Load(), creds.SessionToken)
	}
}

// invalidatingFakeProvider models a cache: serves `current` until Invalidate,
// then promotes `next` — the same observable behavior as aws.CredentialsCache.
type invalidatingFakeProvider struct {
	current, next aws.Credentials
	invalidations atomic.Int64
}

func (f *invalidatingFakeProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return f.current, nil
}

func (f *invalidatingFakeProvider) Invalidate() {
	f.invalidations.Add(1)
	f.current = f.next
}

// TestResolveAndEmitRejectsNonAdvancingExpiry: a provider handing back the same
// session (mis-cached) must be treated as a refresh FAILURE, not re-emitted.
func TestResolveAndEmitRejectsNonAdvancingExpiry(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	exp := time.Now().Add(30 * time.Minute)
	fake := &fakeCredProvider{creds: aws.Credentials{
		AccessKeyID: "K1", SecretAccessKey: "S", SessionToken: "T",
		CanExpire: true, Expires: exp,
	}}
	r := &s3CredentialRefresher{
		db: db, provider: fake, logger: zerolog.Nop(),
		params: s3SecretParams{name: "adv_test", region: "us-east-1", useSSL: true},
	}
	if _, err := r.resolveAndEmit(context.Background(), false); err != nil {
		t.Fatalf("first emit: %v", err)
	}
	if _, err := r.resolveAndEmit(context.Background(), false); err == nil {
		t.Fatal("second emit with identical expiry must fail (non-advancing)")
	}
	fake.creds.Expires = exp.Add(time.Hour)
	if _, err := r.resolveAndEmit(context.Background(), false); err != nil {
		t.Fatalf("emit with advanced expiry: %v", err)
	}
}

func TestRefreshDelay(t *testing.T) {
	if d := refreshDelay(time.Now().Add(time.Hour)); d < 49*time.Minute || d > 51*time.Minute {
		t.Errorf("1h session: delay = %v, want ~50m (margin %v)", d, s3RefreshMargin)
	}
	// Sessions shorter than the margin floor at the minimum delay.
	if d := refreshDelay(time.Now().Add(2 * time.Minute)); d != s3RefreshMinDelay {
		t.Errorf("2m session: delay = %v, want floor %v", d, s3RefreshMinDelay)
	}
	if d := refreshDelay(time.Now().Add(-time.Minute)); d != s3RefreshMinDelay {
		t.Errorf("expired session: delay = %v, want floor %v", d, s3RefreshMinDelay)
	}
}

// TestRefresherStartFailureAndStop: an erroring provider must not block start
// (Warn + background retry), and stop() must return promptly.
func TestRefresherStartFailureAndStop(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	fake := &fakeCredProvider{err: fmt.Errorf("token file not yet projected")}
	start := time.Now()
	r := startS3CredentialRefresher(db, s3SecretParams{name: "fail_test", region: "us-east-1", useSSL: true}, fake, zerolog.Nop())
	if elapsed := time.Since(start); elapsed > s3FirstResolveTimeout+2*time.Second {
		t.Fatalf("start blocked %v; must degrade fast", elapsed)
	}
	done := make(chan struct{})
	go func() { r.stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stop() did not return within 5s")
	}
	var n int
	if err := db.QueryRow("SELECT count(*) FROM duckdb_secrets() WHERE name='fail_test'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("no secret should exist after failed resolves, got %d", n)
	}
}

// TestSecretRotationReachesRequests is the CI stand-in for the live IRSA rig:
// a mock S3 endpoint captures the SigV4 Authorization header (which embeds the
// KEY_ID) of every request DuckDB signs. Rotating the secret via
// resolveAndEmit must change the key used by SUBSEQUENT queries — including on
// pool connections that already signed with the old key. This is the property
// the whole #600 fix rests on.
func TestSecretRotationReachesRequests(t *testing.T) {
	var mu sync.Mutex
	var authHeaders []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		authHeaders = append(authHeaders, req.Header.Get("Authorization"))
		mu.Unlock()
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()
	endpoint := strings.TrimPrefix(srv.URL, "http://")

	db := openTestDuckDBWithHTTPFS(t)
	db.SetMaxOpenConns(4)

	fake := &fakeCredProvider{creds: aws.Credentials{
		AccessKeyID: "AKIAOLDKEY", SecretAccessKey: "s1", SessionToken: "t1",
		CanExpire: true, Expires: time.Now().Add(time.Hour),
	}}
	r := &s3CredentialRefresher{
		db: db, provider: fake, logger: zerolog.Nop(),
		params: s3SecretParams{
			name: "rot_test", region: "us-east-1",
			endpoint: endpoint, pathStyle: true, useSSL: false,
		},
	}
	if _, err := r.resolveAndEmit(context.Background(), false); err != nil {
		t.Fatal(err)
	}

	query := func() {
		// The 404 error is expected; the signed request is what we're after.
		_, _ = db.Exec("SELECT * FROM read_parquet('s3://rotbucket/x.parquet')")
	}
	query()

	fake.creds = aws.Credentials{
		AccessKeyID: "AKIANEWKEY", SecretAccessKey: "s2", SessionToken: "t2",
		CanExpire: true, Expires: time.Now().Add(2 * time.Hour),
	}
	if _, err := r.resolveAndEmit(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	query()
	query()

	mu.Lock()
	defer mu.Unlock()
	if len(authHeaders) < 3 {
		t.Fatalf("mock endpoint saw %d requests, want >=3", len(authHeaders))
	}
	if !strings.Contains(authHeaders[0], "AKIAOLDKEY") {
		t.Errorf("first request not signed with old key: %s", authHeaders[0])
	}
	last := authHeaders[len(authHeaders)-1]
	if !strings.Contains(last, "AKIANEWKEY") {
		t.Errorf("post-rotation request still signed with old key: %s", last)
	}
	for _, h := range authHeaders[1:] {
		if strings.Contains(h, "AKIAOLDKEY") && strings.Contains(last, "AKIAOLDKEY") {
			t.Errorf("rotation never took effect: %v", authHeaders)
		}
	}
}

// openTestDuckDBWithHTTPFS opens the bundled engine with Arc's DSN and httpfs
// loaded, skipping when the extension cache is cold (offline CI).
func openTestDuckDBWithHTTPFS(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "?allow_persistent_secrets=false")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("INSTALL httpfs"); err != nil {
		t.Skipf("httpfs unavailable (offline?): %v", err)
	}
	if _, err := db.Exec("LOAD httpfs"); err != nil {
		t.Skipf("httpfs unavailable (offline?): %v", err)
	}
	return db
}

// TestNewStartsPrimaryRefresherUnderIRSA pins the primary-tier wiring: with an
// S3 primary backend and IRSA detected, New must start the refresher (whose
// synchronous first resolve emits the secret before New returns) and register
// it for Close. Deleting the startRefresher call in New would otherwise compile
// and pass the suite while leaving IRSA primaries with NO secret at all —
// configureS3Access intentionally defers emission for this mode.
func TestNewStartsPrimaryRefresherUnderIRSA(t *testing.T) {
	t.Setenv(envAWSRoleARN, "arn:aws:iam::123456789012:role/arc-irsa")
	t.Setenv(envAWSWebIdentityTokenFile, "/var/run/secrets/eks.amazonaws.com/serviceaccount/token")
	t.Setenv(envAWSAccessKeyID, "")
	t.Setenv(envAWSSecretAccessKey, "")

	orig := newAWSCredProvider
	fake := &fakeCredProvider{creds: aws.Credentials{
		AccessKeyID: "ASIAFAKEPRIMARY", SecretAccessKey: "fs", SessionToken: "ft",
		CanExpire: true, Expires: time.Now().Add(time.Hour),
	}}
	newAWSCredProvider = func(ctx context.Context, region string) (awsCredentialsProvider, error) {
		return fake, nil
	}
	t.Cleanup(func() { newAWSCredProvider = orig })

	tmp := t.TempDir()
	db, err := New(&Config{
		MaxConnections:     2,
		MemoryLimit:        "256MB",
		TempDirectory:      tmp,
		S3IsPrimaryBackend: true,
		S3Bucket:           "primary-bucket",
		S3Region:           "us-gov-west-1",
		S3UseSSL:           true,
	}, zerolog.Nop())
	if err != nil {
		if strings.Contains(err.Error(), "httpfs") {
			t.Skipf("httpfs unavailable (offline?): %v", err)
		}
		t.Fatalf("New: %v", err)
	}
	defer db.Close()

	var secretString string
	if err := db.DB().QueryRow(
		"SELECT secret_string FROM duckdb_secrets() WHERE name = ?", arcS3PrimarySecretName,
	).Scan(&secretString); err != nil {
		t.Fatalf("primary secret not registered after New: %v", err)
	}
	mustContain(t, secretString, "key_id=ASIAFAKEPRIMARY")
	mustContain(t, secretString, "session_token=redacted")
	if db.s3Refreshers[arcS3PrimarySecretName] == nil {
		t.Fatal("primary refresher not registered — Close cannot stop it")
	}
}

// TestResolveAndEmitRejectsControlBytes: credential material containing control
// bytes must be rejected with a FIXED error before reaching SQL construction —
// DuckDB's parser truncates at a NUL and echoes the credential prefix in its
// error, which the refresher's callers log (security review finding).
func TestResolveAndEmitRejectsControlBytes(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	fake := &fakeCredProvider{creds: aws.Credentials{
		AccessKeyID: "AKIA", SecretAccessKey: "s", SessionToken: "SUPERSECRET\x00trunc",
		CanExpire: true, Expires: time.Now().Add(time.Hour),
	}}
	r := &s3CredentialRefresher{
		db: db, provider: fake, logger: zerolog.Nop(),
		params: s3SecretParams{name: "nul_test", region: "us-east-1", useSSL: true},
	}
	_, err := r.resolveAndEmit(context.Background(), false)
	if err == nil {
		t.Fatal("control-byte credential must be rejected")
	}
	if strings.Contains(err.Error(), "SUPERSECRET") {
		t.Fatalf("error leaks credential material: %v", err)
	}
}
