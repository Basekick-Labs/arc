package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	r := &credentialRefresher{
		resolver: &s3Resolver{db: db, provider: fake, params: s3SecretParams{name: "inv_test", region: "us-east-1", useSSL: true}},
		logger:   zerolog.Nop(),
	}
	if _, err := r.refreshOnce(context.Background(), false); err != nil {
		t.Fatalf("initial fill: %v", err)
	}
	if fake.invalidations.Load() != 0 {
		t.Fatal("initial fill must not invalidate")
	}
	// Without invalidation the provider keeps serving the current session, so a
	// proactive resolve would be non-advancing; WITH it, the new session lands.
	m, err := r.refreshOnce(context.Background(), true)
	if err != nil {
		t.Fatalf("proactive refresh: %v", err)
	}
	if got := m.payload.(aws.Credentials).SessionToken; fake.invalidations.Load() != 1 || got != "T2" {
		t.Fatalf("proactive refresh must invalidate then get the new session; invalidations=%d token=%q",
			fake.invalidations.Load(), got)
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

// TestResolveAndEmitFlagsNonAdvancingExpiry: a provider handing back the same
// session must surface the errNonAdvancingExpiry SENTINEL — the loop routes on
// errors.Is to the flat-poll path (normal for server-rotated sources); a
// wrapped or generic error here would silently reclassify normal IMDS-tail
// polling as Error + exponential backoff, whose steps can straddle the
// rotation (#601 M3).
func TestResolveAndEmitFlagsNonAdvancingExpiry(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	exp := time.Now().Add(30 * time.Minute)
	fake := &fakeCredProvider{creds: aws.Credentials{
		AccessKeyID: "K1", SecretAccessKey: "S", SessionToken: "T",
		CanExpire: true, Expires: exp,
	}}
	r := &credentialRefresher{
		resolver: &s3Resolver{db: db, provider: fake, params: s3SecretParams{name: "adv_test", region: "us-east-1", useSSL: true}},
		logger:   zerolog.Nop(),
	}
	if _, err := r.refreshOnce(context.Background(), false); err != nil {
		t.Fatalf("first emit: %v", err)
	}
	if _, err := r.refreshOnce(context.Background(), false); !errors.Is(err, errNonAdvancingExpiry) {
		t.Fatalf("second emit with identical expiry must return errNonAdvancingExpiry, got: %v", err)
	}
	fake.creds.Expires = exp.Add(time.Hour)
	if _, err := r.refreshOnce(context.Background(), false); err != nil {
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
// (the caller's onFirstFailure hook runs, then background retry), and stop()
// must return promptly. The hook must fire synchronously during start — that
// is the structural guarantee that a caller-emitted fallback secret lands
// before the retry loop can emit over it (#601 M1).
func TestRefresherStartFailureAndStop(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	fake := &fakeCredProvider{err: fmt.Errorf("token file not yet projected")}
	start := time.Now()
	hookFired := false
	r := startS3CredentialRefresher(db, s3SecretParams{name: "fail_test", region: "us-east-1", useSSL: true}, fake, zerolog.Nop(),
		func(err error) { hookFired = true })
	if !hookFired {
		t.Fatal("onFirstFailure hook must fire synchronously during start")
	}
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
	r := &credentialRefresher{
		resolver: &s3Resolver{db: db, provider: fake, params: s3SecretParams{
			name: "rot_test", region: "us-east-1",
			endpoint: endpoint, pathStyle: true, useSSL: false,
		}},
		logger: zerolog.Nop(),
	}
	// F8 sub-case: the FALLBACK-to-managed upgrade path replaces a
	// CREDENTIAL_CHAIN secret with a static one. Emit the chain shape first
	// (resolving to nothing under the hermetic env); the initial managed emit
	// below then replaces it, and the first query must sign with the managed
	// key — proving chain→static replacement takes effect like static→static.
	hermeticAWSEnv(t)
	chainSQL, err := buildS3SecretSQL(s3SecretParams{
		name: "rot_test", region: "us-east-1",
		endpoint: endpoint, pathStyle: true, useSSL: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(chainSQL); err != nil {
		t.Fatalf("chain fallback emit: %v", err)
	}

	if _, err := r.refreshOnce(context.Background(), false); err != nil {
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
	if _, err := r.refreshOnce(context.Background(), false); err != nil {
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
	hermeticAWSEnv(t)

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
	r := &credentialRefresher{
		resolver: &s3Resolver{db: db, provider: fake, params: s3SecretParams{name: "nul_test", region: "us-east-1", useSSL: true}},
		logger:   zerolog.Nop(),
	}
	_, err := r.refreshOnce(context.Background(), false)
	if err == nil {
		t.Fatal("control-byte credential must be rejected")
	}
	if strings.Contains(err.Error(), "SUPERSECRET") {
		t.Fatalf("error leaks credential material: %v", err)
	}
}

// TestNewAWSCredProviderIMDSStub drives the REAL provider chain against a local
// IMDSv2 stub (AWS_EC2_METADATA_SERVICE_ENDPOINT) and pins the SDK behavior the
// EC2 cell depends on: ec2rolecreds caps reported Expires at now+1h even for a
// ~6h IMDS session, so on EC2 the refresher fires HOURLY re-emitting the same
// material under an advancing cap — expected behavior, not a bug (#601 F4).
func TestNewAWSCredProviderIMDSStub(t *testing.T) {
	hermeticAWSEnv(t)
	sixHours := time.Now().Add(6 * time.Hour).UTC().Format("2006-01-02T15:04:05Z")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == "PUT" && r.URL.Path == "/latest/api/token":
			w.Write([]byte("stub-token"))
		case r.URL.Path == "/latest/meta-data/iam/security-credentials/":
			w.Write([]byte("stub-role"))
		case r.URL.Path == "/latest/meta-data/iam/security-credentials/stub-role":
			fmt.Fprintf(w, `{"Code":"Success","AccessKeyId":"ASIAIMDSSTUB","SecretAccessKey":"s","Token":"t","Expiration":"%s"}`, sixHours)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	t.Setenv("AWS_EC2_METADATA_DISABLED", "false")
	t.Setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", srv.URL)

	provider, err := newAWSCredProvider(context.Background(), "us-east-1")
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if credSourceLabel(creds.Source) != "EC2RoleProvider" {
		t.Errorf("source = %q, want EC2RoleProvider", creds.Source)
	}
	if !creds.CanExpire {
		t.Fatal("IMDS creds must be expiring")
	}
	// The 1h cap: reported expiry must be ~1h out, NOT ~6h.
	until := time.Until(creds.Expires)
	if until > 65*time.Minute {
		t.Errorf("expiry %v out — ec2rolecreds 1h cap not in effect; hourly scheduling assumption broken", until.Round(time.Minute))
	}
	// Consequently the refresher schedules ~hourly.
	if d := refreshDelay(creds.Expires); d > 55*time.Minute {
		t.Errorf("refreshDelay = %v, want <=55m under the 1h cap", d.Round(time.Minute))
	}
}

// TestNewAWSCredProviderPodIdentityStub drives the real chain against a local
// container-credentials stub (the provider EKS Pod Identity uses) and pins the
// config package's baked-in ExpiryWindow=5min: reported expiry is real-5min,
// so non-advancing responses are possible in this cell too (#601 F4).
func TestNewAWSCredProviderPodIdentityStub(t *testing.T) {
	hermeticAWSEnv(t)
	oneHour := time.Now().Add(time.Hour).UTC().Format("2006-01-02T15:04:05Z")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "stub-auth" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		fmt.Fprintf(w, `{"AccessKeyId":"ASIAPODSTUB","SecretAccessKey":"s","Token":"t","Expiration":"%s"}`, oneHour)
	}))
	defer srv.Close()
	t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", srv.URL)
	t.Setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN", "stub-auth")

	provider, err := newAWSCredProvider(context.Background(), "us-east-1")
	if err != nil {
		t.Fatalf("provider: %v", err)
	}
	creds, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if credSourceLabel(creds.Source) != "CredentialsEndpointProvider" {
		t.Errorf("source = %q, want CredentialsEndpointProvider", creds.Source)
	}
	until := time.Until(creds.Expires)
	// real-5min window: expect ~55min, definitely under the served 60.
	if until > 57*time.Minute || until < 50*time.Minute {
		t.Errorf("reported expiry %v out, want ~55m (endpointcreds baked-in 5min ExpiryWindow)", until.Round(time.Minute))
	}
}

// TestPlanOutcomes pins the scheduling/severity state machine (#601 F2/F6)
// without real waits.
func TestPlanOutcomes(t *testing.T) {
	r := &credentialRefresher{logger: zerolog.Nop()}

	t.Run("non-advancing polls flat at MinDelay", func(t *testing.T) {
		r.lastExpiry = time.Now().Add(30 * time.Minute)
		if d := r.planNonAdvancing(); d != s3RefreshMinDelay {
			t.Errorf("delay = %v, want flat %v (backoff could straddle the rotation)", d, s3RefreshMinDelay)
		}
	})

	t.Run("never-succeeded errors demote after threshold with longer cap", func(t *testing.T) {
		rr := &credentialRefresher{logger: zerolog.Nop()} // everSucceeded=false
		var last time.Duration
		for i := 0; i < 10; i++ {
			last = rr.planError(fmt.Errorf("no creds"))
		}
		if last != s3RefreshBackoffMaxUnproven {
			t.Errorf("unproven cap = %v, want %v", last, s3RefreshBackoffMaxUnproven)
		}
	})

	t.Run("previously-succeeded errors keep the tight cap", func(t *testing.T) {
		rr := &credentialRefresher{logger: zerolog.Nop(), everSucceeded: true}
		var last time.Duration
		for i := 0; i < 10; i++ {
			last = rr.planError(fmt.Errorf("sts down"))
		}
		if last != s3RefreshBackoffMax {
			t.Errorf("proven cap = %v, want %v (credentials WILL die at expiry)", last, s3RefreshBackoffMax)
		}
	})

	t.Run("success resets the error streak", func(t *testing.T) {
		rr := &credentialRefresher{logger: zerolog.Nop()}
		rr.planError(fmt.Errorf("x"))
		rr.planError(fmt.Errorf("x"))
		rr.planSuccess(credMaterial{canExpire: true, expires: time.Now().Add(time.Hour)})
		if rr.consecutiveErrors != 0 || !rr.everSucceeded {
			t.Errorf("success must reset streak and mark proven: errors=%d proven=%v", rr.consecutiveErrors, rr.everSucceeded)
		}
		if d := rr.planError(fmt.Errorf("x")); d != s3RefreshBackoffMin {
			t.Errorf("post-success first error delay = %v, want %v", d, s3RefreshBackoffMin)
		}
	})
}

// TestUnresolvableFallsBackToChainSecret pins the resolve-fails cell (#601):
// New with an S3 primary and NOTHING resolvable must still produce a secret —
// the plain CREDENTIAL_CHAIN fallback — post-lockdown, and register a refresher
// that keeps retrying. This is also the post-lockdown CHAIN-secret variant the
// plan review asked for (F9): the fallback is created after
// enable_external_access=false, which only works because ensureHTTPFSLoaded
// pre-loaded the aws extension and buildDSN disabled persistent secrets.
func TestUnresolvableFallsBackToChainSecret(t *testing.T) {
	hermeticAWSEnv(t)
	// Real provider chain, nothing resolvable, IMDS disabled -> fails in <1ms.
	tmp := t.TempDir()
	db, err := New(&Config{
		MaxConnections:     2,
		MemoryLimit:        "256MB",
		TempDirectory:      tmp,
		S3IsPrimaryBackend: true,
		S3Bucket:           "fallback-bucket",
		S3Region:           "us-east-1",
		S3UseSSL:           true,
	}, zerolog.Nop())
	if err != nil {
		if strings.Contains(err.Error(), "httpfs") {
			t.Skipf("httpfs unavailable (offline?): %v", err)
		}
		t.Fatalf("New must not fail on unresolvable credentials: %v", err)
	}
	defer db.Close()

	var secretString string
	if err := db.DB().QueryRow(
		"SELECT secret_string FROM duckdb_secrets() WHERE name = ?", arcS3PrimarySecretName,
	).Scan(&secretString); err != nil {
		t.Fatalf("fallback secret not registered: %v", err)
	}
	if !strings.Contains(secretString, "credential_chain") {
		t.Errorf("fallback must be a CREDENTIAL_CHAIN secret, got: %s", secretString)
	}
	if db.s3Refreshers[arcS3PrimarySecretName] == nil {
		t.Fatal("refresher must stay registered (background retry picks up late-arriving credentials)")
	}
}

// TestPlanLogLevels pins the severity choreography (#601 M4) — the headline
// operational claims: a never-succeeded refresher logs Error for the first
// failures, exactly ONE demotion Warn (naming the escape hatch), then Debug;
// and non-advancing polls log Debug with runway, Warn inside the final window.
func TestPlanLogLevels(t *testing.T) {
	levelsOf := func(buf *strings.Builder) []string {
		var out []string
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if line == "" {
				continue
			}
			for _, lvl := range []string{"error", "warn", "debug", "info"} {
				if strings.Contains(line, `"level":"`+lvl+`"`) {
					out = append(out, lvl)
					break
				}
			}
		}
		return out
	}

	t.Run("never-succeeded demotion: N errors, one warn, then debug", func(t *testing.T) {
		var buf strings.Builder
		r := &credentialRefresher{logger: zerolog.New(&buf), demotionHint: s3DemotionHint}
		for i := 0; i < s3RefreshErrorsBeforeDemotion+3; i++ {
			r.planError(fmt.Errorf("no creds"))
		}
		got := levelsOf(&buf)
		want := []string{"error", "error", "error", "warn", "debug", "debug"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("levels = %v, want %v", got, want)
		}
		if !strings.Contains(buf.String(), "AWS_EC2_METADATA_DISABLED") {
			t.Error("demotion warn must name the escape hatch")
		}
		if n := strings.Count(buf.String(), `"level":"warn"`); n != 1 {
			t.Errorf("demotion warn must fire exactly once, got %d", n)
		}
	})

	t.Run("previously-succeeded failures stay at error", func(t *testing.T) {
		var buf strings.Builder
		r := &credentialRefresher{logger: zerolog.New(&buf), everSucceeded: true}
		for i := 0; i < 6; i++ {
			r.planError(fmt.Errorf("sts down"))
		}
		for _, lvl := range levelsOf(&buf) {
			if lvl != "error" {
				t.Fatalf("proven refresher must keep Error on every failure, saw %v", levelsOf(&buf))
			}
		}
	})

	t.Run("non-advancing: debug with runway, warn in final window", func(t *testing.T) {
		var buf strings.Builder
		r := &credentialRefresher{logger: zerolog.New(&buf)}
		r.lastExpiry = time.Now().Add(30 * time.Minute)
		r.planNonAdvancing()
		r.lastExpiry = time.Now().Add(s3RefreshFinalWarnWindow - time.Minute)
		r.planNonAdvancing()
		got := levelsOf(&buf)
		want := []string{"debug", "warn"}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("levels = %v, want %v", got, want)
		}
	})
}

// TestDeriveState covers the full state grid — the pure function is the only
// practical coverage for "expired" (unforceable against real STS) and pins the
// precedence: fallback > non-expiring ok > expired-even-with-errors > degraded.
func TestDeriveState(t *testing.T) {
	now := time.Now()
	past, future := now.Add(-time.Minute), now.Add(time.Hour)
	cases := []struct {
		name string
		snap *storageCredSnapshot
		want string
	}{
		{"nil snapshot", nil, CredStateFallback},
		{"never succeeded", &storageCredSnapshot{everSucceeded: false, consecutiveErrors: 9}, CredStateFallback},
		{"non-expiring", &storageCredSnapshot{everSucceeded: true, canExpire: false}, CredStateOK},
		{"healthy", &storageCredSnapshot{everSucceeded: true, canExpire: true, lastExpiry: future}, CredStateOK},
		{"failing but valid", &storageCredSnapshot{everSucceeded: true, canExpire: true, lastExpiry: future, consecutiveErrors: 2}, CredStateDegraded},
		{"expired", &storageCredSnapshot{everSucceeded: true, canExpire: true, lastExpiry: past}, CredStateExpired},
		// The field incident: refreshes failing AND creds dead — expired wins.
		{"expired with error streak", &storageCredSnapshot{everSucceeded: true, canExpire: true, lastExpiry: past, consecutiveErrors: 7}, CredStateExpired},
	}
	for _, tc := range cases {
		if got := deriveState(tc.snap, now); got != tc.want {
			t.Errorf("%s: deriveState = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestStatusPublishSites pins that every refresher outcome publishes a
// snapshot status() can see — most importantly the constructor-success site,
// whose omission would report "fallback" for ~refreshDelay on a healthy
// refresher (#603 review F1).
func TestStatusPublishSites(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	now := time.Now()

	t.Run("constructor success publishes ok with expiry+source", func(t *testing.T) {
		fake := &fakeCredProvider{creds: aws.Credentials{
			AccessKeyID: "K", SecretAccessKey: "S", SessionToken: "T", Source: "WebIdentityCredentials",
			CanExpire: true, Expires: now.Add(time.Hour),
		}}
		r := startS3CredentialRefresher(db, s3SecretParams{name: "st_ok", region: "r", useSSL: true}, fake, zerolog.Nop(), nil)
		defer r.stop()
		st := r.status(now)
		if st.State != CredStateOK || st.ExpiresAt == nil || st.Source != "WebIdentityCredentials" {
			t.Fatalf("constructor success status = %+v", st)
		}
	})

	t.Run("constructor failure publishes fallback", func(t *testing.T) {
		fake := &fakeCredProvider{err: fmt.Errorf("nope")}
		r := startS3CredentialRefresher(db, s3SecretParams{name: "st_fb", region: "r", useSSL: true}, fake, zerolog.Nop(), nil)
		defer r.stop()
		if st := r.status(now); st.State != CredStateFallback {
			t.Fatalf("constructor failure status = %+v", st)
		}
	})

	t.Run("emit-once publishes non-expiring ok", func(t *testing.T) {
		fake := &fakeCredProvider{creds: aws.Credentials{
			AccessKeyID: "K", SecretAccessKey: "S", Source: "EnvConfigCredentials", CanExpire: false,
		}}
		r := startS3CredentialRefresher(db, s3SecretParams{name: "st_once", region: "r", useSSL: true}, fake, zerolog.Nop(), nil)
		r.stop()
		st := r.status(now)
		if st.State != CredStateOK || st.ExpiresAt != nil {
			t.Fatalf("emit-once status = %+v", st)
		}
	})

	t.Run("planError publishes degraded; planSuccess recovers", func(t *testing.T) {
		r := &credentialRefresher{logger: zerolog.Nop()}
		r.everSucceeded = true
		r.lastExpiry = now.Add(time.Hour)
		r.publishStatus(true)
		r.planError(fmt.Errorf("sts down"))
		if st := r.status(now); st.State != CredStateDegraded || st.ConsecutiveErrors != 1 {
			t.Fatalf("post-error status = %+v", st)
		}
		r.planSuccess(credMaterial{canExpire: true, expires: now.Add(2 * time.Hour)})
		if st := r.status(now); st.State != CredStateOK || st.ConsecutiveErrors != 0 {
			t.Fatalf("post-recovery status = %+v", st)
		}
	})

	t.Run("expired derives at READ time with no new outcome", func(t *testing.T) {
		r := &credentialRefresher{logger: zerolog.Nop()}
		r.everSucceeded = true
		r.lastExpiry = now.Add(30 * time.Millisecond)
		r.publishStatus(true)
		if st := r.status(now); st.State != CredStateOK {
			t.Fatalf("pre-expiry: %+v", st)
		}
		if st := r.status(now.Add(time.Minute)); st.State != CredStateExpired {
			t.Fatalf("post-expiry (same snapshot!): %+v", st)
		}
	})
}

// TestAzureCredentialsLabel pins the SAS honesty rule (#603 review F3): a
// connection string embedding a SharedAccessSignature EXPIRES and must not
// report static/ok.
func TestAzureCredentialsLabel(t *testing.T) {
	cases := []struct {
		conn, key, sas, wantCreds, wantState string
	}{
		{"DefaultEndpointsProtocol=https;AccountName=a;AccountKey=abc==", "", "", credModeStaticKeys, CredStateOK},
		{"BlobEndpoint=https://a.blob.core.windows.net;SharedAccessSignature=sv=2024&sig=xyz", "", "", credModeSAS, CredStateUnknown},
		{"BlobEndpoint=https://a.blob.core.windows.net;sig=xyz", "", "", credModeSAS, CredStateUnknown},
		{"BlobEndpoint=https://a.blob.core.windows.net?sv=2024&sig=xyz", "", "", credModeSAS, CredStateUnknown},
		{"", "accountkey==", "", credModeStaticKeys, CredStateOK},
		// A configured SAS token (#605 M6) must never route to the managed
		// refresher — that would acquire a broader identity than the
		// operator's deliberately-scoped SAS.
		{"", "", "sv=2024&sig=xyz", credModeSAS, CredStateUnknown},
		// Managed identity / SP env: Arc-managed refresher as of #605.
		{"", "", "", s3ModeSDKManaged, CredStateOK},
	}
	for _, tc := range cases {
		creds, state := azureCredentialsLabel(tc.conn, tc.key, tc.sas)
		if creds != tc.wantCreds || state != tc.wantState {
			t.Errorf("azureCredentialsLabel(%q,%q,%q) = %s/%s, want %s/%s", tc.conn, tc.key, tc.sas, creds, state, tc.wantCreds, tc.wantState)
		}
	}
}

// TestStorageCredentialStatusTiers pins the per-tier aggregation across the
// matrix cells.
func TestStorageCredentialStatusTiers(t *testing.T) {
	hermeticAWSEnv(t)

	t.Run("local hot only", func(t *testing.T) {
		tmp := t.TempDir()
		db, err := New(&Config{MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: tmp, LocalStorageRoot: filepath.Join(tmp, "d")}, zerolog.Nop())
		if err != nil {
			t.Skipf("New: %v", err)
		}
		defer db.Close()
		st := db.StorageCredentialStatus()
		if len(st) != 1 || st["hot"].Backend != "local" || st["hot"].State != CredStateOK {
			t.Fatalf("local-only status = %+v", st)
		}
	})

	t.Run("s3 hot managed + s3 cold managed", func(t *testing.T) {
		orig := newAWSCredProvider
		newAWSCredProvider = func(ctx context.Context, region string) (awsCredentialsProvider, error) {
			return &fakeCredProvider{creds: aws.Credentials{
				AccessKeyID: "K", SecretAccessKey: "S", SessionToken: "T", Source: "CredentialsEndpointProvider",
				CanExpire: true, Expires: time.Now().Add(time.Hour),
			}}, nil
		}
		t.Cleanup(func() { newAWSCredProvider = orig })

		tmp := t.TempDir()
		db, err := New(&Config{
			MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: tmp,
			S3IsPrimaryBackend: true, S3Bucket: "hotb", S3Region: "us-east-1", S3UseSSL: true,
		}, zerolog.Nop())
		if err != nil {
			t.Skipf("New: %v", err)
		}
		defer db.Close()
		if err := db.ConfigureS3(&S3Config{Region: "us-east-1", Bucket: "coldb"}); err != nil {
			t.Fatalf("ConfigureS3: %v", err)
		}
		st := db.StorageCredentialStatus()
		hot, cold := st["hot"], st["cold"]
		if hot.Backend != "s3" || hot.Credentials != s3ModeSDKManaged || hot.State != CredStateOK || hot.ExpiresAt == nil {
			t.Fatalf("hot = %+v", hot)
		}
		if cold.Backend != "s3" || cold.Credentials != s3ModeSDKManaged || cold.State != CredStateOK {
			t.Fatalf("cold = %+v", cold)
		}
	})

	t.Run("s3 static hot", func(t *testing.T) {
		tmp := t.TempDir()
		db, err := New(&Config{
			MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: tmp,
			S3IsPrimaryBackend: true, S3Bucket: "b", S3Region: "us-east-1", S3UseSSL: true,
			S3AccessKey: "AKIA", S3SecretKey: "sk",
		}, zerolog.Nop())
		if err != nil {
			t.Skipf("New: %v", err)
		}
		defer db.Close()
		hot := db.StorageCredentialStatus()["hot"]
		if hot.Credentials != credModeStaticKeys || hot.State != CredStateOK || hot.ExpiresAt != nil {
			t.Fatalf("static hot = %+v", hot)
		}
	})
}

// TestColdTierAbsentWhenConfigurationFails (#603 review H1): a cold tier whose
// secret creation failed must be ABSENT from /health — recording it before
// emission would affirmatively report a working tier with no secret, the exact
// green-but-dead shape this feature exists to kill.
func TestColdTierAbsentWhenConfigurationFails(t *testing.T) {
	hermeticAWSEnv(t)
	tmp := t.TempDir()
	db, err := New(&Config{MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: tmp, LocalStorageRoot: filepath.Join(tmp, "d")}, zerolog.Nop())
	if err != nil {
		t.Skipf("New: %v", err)
	}
	defer db.Close()

	// Half-configured key pair: buildS3SecretSQL rejects it, ConfigureS3 errors.
	if err := db.ConfigureS3(&S3Config{Region: "us-east-1", Bucket: "coldb", AccessKey: "AKIA"}); err == nil {
		t.Fatal("half-configured cold tier must error")
	}
	st := db.StorageCredentialStatus()
	if _, ok := st["cold"]; ok {
		t.Fatalf("failed cold tier must be absent from /health, got %+v", st["cold"])
	}
}
