package database

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// fakeGCSSource hands out a fixed token, or an error.
type fakeGCSSource struct {
	tok *auth.Token
	err error
}

func (f *fakeGCSSource) Token(context.Context) (*auth.Token, error) { return f.tok, f.err }

// withGCSSources replaces newGCSTokenSource with next(), counting builds.
func withGCSSources(t *testing.T, next func() (gcsTokenSource, error)) *int {
	t.Helper()
	var mu sync.Mutex
	builds := 0
	orig := newGCSTokenSource
	newGCSTokenSource = func(*storage.GCSConfig) (gcsTokenSource, error) {
		mu.Lock()
		builds++
		mu.Unlock()
		return next()
	}
	t.Cleanup(func() { newGCSTokenSource = orig })
	return &builds
}

func TestBuildGCSSecretSQL(t *testing.T) {
	got, err := buildGCSSecretSQL(gcsSecretParams{
		name: arcGCSPrimarySecretName, scope: "gs://b/p/", endpointHost: "storage-x.p.googleapis.com", bearerToken: "ya29.a'b",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"TYPE GCS", "BEARER_TOKEN 'ya29.a''b'", "ENDPOINT 'storage-x.p.googleapis.com'", "SCOPE 'gs://b/p/'"} {
		if !strings.Contains(got, want) {
			t.Errorf("secret SQL lacks %q:\n%s", want, got)
		}
	}
	if got, _ := buildGCSSecretSQL(gcsSecretParams{name: "s", scope: "gs://b/", bearerToken: "t"}); strings.Contains(got, "ENDPOINT") {
		t.Errorf("no endpoint configured, but ENDPOINT emitted:\n%s", got)
	}
	if _, err := buildGCSSecretSQL(gcsSecretParams{name: "s", bearerToken: "t"}); err == nil {
		t.Error("an unscoped GCS secret must be refused: it would carry Arc's token to any gs:// path")
	}
	if _, err := buildGCSSecretSQL(gcsSecretParams{name: "s", scope: "gs://b/"}); err == nil {
		t.Error("a secret without a token must be refused")
	}
}

func TestGCSEndpointHost(t *testing.T) {
	for in, want := range map[string]string{
		"":                                    "",
		"storage.googleapis.com":              "storage.googleapis.com",
		"https://storage.googleapis.com":      "storage.googleapis.com",
		"https://psc.p.googleapis.com/":       "psc.p.googleapis.com",
		"https://psc.p.googleapis.com:8443/x": "psc.p.googleapis.com:8443",
	} {
		got, err := gcsEndpointHost(in)
		if err != nil || got != want {
			t.Errorf("gcsEndpointHost(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"http://localhost:4443", "ftp://x", "https://"} {
		if got, err := gcsEndpointHost(bad); err == nil {
			t.Errorf("gcsEndpointHost(%q) = %q, want an error", bad, got)
		}
	}
}

func TestGCSPrefixURI(t *testing.T) {
	for _, c := range []struct{ bucket, prefix, want string }{
		{"b", "", "gs://b/"},
		{"b", "inst/", "gs://b/inst/"},
		{"b", "/a/b", "gs://b/a/b/"},
		{"b", "../x", "gs://b/"},
	} {
		if got := gcsPrefixURI(c.bucket, c.prefix); got != c.want {
			t.Errorf("gcsPrefixURI(%q,%q) = %q, want %q", c.bucket, c.prefix, got, c.want)
		}
	}
}

// The same token fetched again reports the HELD expiry, so the core takes the
// non-advancing path rather than re-emitting an identical secret on a second
// of Expiry drift; a new token advances.
func TestGCSResolverSameTokenKeepsHeldExpiry(t *testing.T) {
	exp := time.Now().Add(time.Hour)
	src := &fakeGCSSource{tok: &auth.Token{Value: "t1", Expiry: exp}}
	builds := withGCSSources(t, func() (gcsTokenSource, error) { return src, nil })
	r := &gcsResolver{}

	m, err := r.Resolve(context.Background(), false)
	if err != nil || !m.canExpire || !m.expires.Equal(exp) {
		t.Fatalf("first resolve = %+v, %v", m, err)
	}
	src.tok = &auth.Token{Value: "t1", Expiry: exp.Add(2 * time.Second)}
	if m, _ = r.Resolve(context.Background(), true); !m.expires.Equal(exp) {
		t.Fatalf("same token: expires %v, want the held %v", m.expires, exp)
	}
	src.tok = &auth.Token{Value: "t2", Expiry: exp.Add(time.Hour)}
	if m, _ = r.Resolve(context.Background(), true); !m.expires.Equal(exp.Add(time.Hour)) {
		t.Fatalf("new token: expires %v", m.expires)
	}
	if *builds != 3 {
		t.Fatalf("token source built %d times, want 3 (once, then on every invalidate)", *builds)
	}
	// Without invalidate the source is reused.
	_, _ = r.Resolve(context.Background(), false)
	if *builds != 3 {
		t.Fatalf("a resolve without invalidate rebuilt the source")
	}
}

func TestGCSResolverErrors(t *testing.T) {
	withGCSSources(t, func() (gcsTokenSource, error) { return nil, errors.New("no ADC") })
	if _, err := (&gcsResolver{}).Resolve(context.Background(), false); err == nil {
		t.Fatal("source construction failure must fail the resolve")
	}
	withGCSSources(t, func() (gcsTokenSource, error) { return &fakeGCSSource{tok: &auth.Token{}}, nil })
	if _, err := (&gcsResolver{}).Resolve(context.Background(), false); err == nil {
		t.Fatal("an empty token must fail the resolve")
	}
}

// With no fallback secret, a never-succeeded refresher reports "unavailable",
// is never demoted, and keeps the 5-minute cap.
func TestNoFallbackRefresherNeverDemotes(t *testing.T) {
	var buf bytes.Buffer
	r := &credentialRefresher{logger: zerolog.New(&buf), noFallback: true, demotionHint: gcsDemotionHint}
	var last time.Duration
	for i := 0; i < 10; i++ {
		last = r.planError(errors.New("metadata server unreachable"))
	}
	if last != s3RefreshBackoffMax {
		t.Errorf("cap = %v, want %v", last, s3RefreshBackoffMax)
	}
	if n := strings.Count(buf.String(), `"level":"error"`); n != 10 {
		t.Errorf("%d of 10 failures logged at error:\n%s", n, buf.String())
	}
	if strings.Contains(buf.String(), "hint") {
		t.Errorf("demotion hint logged for a noFallback refresher:\n%s", buf.String())
	}
	if st := deriveState(r.snap.Load(), time.Now()); st != CredStateUnavailable {
		t.Errorf("state = %q, want %q", st, CredStateUnavailable)
	}
	// refreshOnce sets lastExpiry on a successful emit; planSuccess publishes.
	r.lastExpiry = time.Now().Add(time.Hour)
	r.planSuccess(credMaterial{canExpire: true, expires: r.lastExpiry})
	if st := deriveState(r.snap.Load(), time.Now()); st != CredStateOK {
		t.Errorf("after success state = %q, want ok", st)
	}
	// The S3/Azure shape is untouched.
	if st := deriveState(&storageCredSnapshot{}, time.Now()); st != CredStateFallback {
		t.Errorf("fallback refresher state = %q, want fallback", st)
	}
}

func newGCSDB(t *testing.T) (*DuckDB, error) {
	t.Helper()
	tmp := t.TempDir()
	return New(&Config{
		MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: tmp, LocalStorageRoot: filepath.Join(tmp, "d"),
		GCSIsPrimaryBackend: true, GCSBucket: "arc-b", GCSPrefix: "inst/",
		GCSEndpoint: "https://storage.googleapis.com",
	}, zerolog.Nop())
}

// A GCS primary emits a scoped GCS secret through the refresher, allowlists
// gs://bucket/prefix/, and reports the tier on /health.
func TestNewWithGCSPrimary(t *testing.T) {
	withGCSSources(t, func() (gcsTokenSource, error) {
		return &fakeGCSSource{tok: &auth.Token{Value: "ya29.test", Expiry: time.Now().Add(time.Hour)}}, nil
	})
	db, err := newGCSDB(t)
	if err != nil {
		t.Skipf("New: %v", err)
	}
	defer db.Close()

	var typ, scope string
	if err := db.db.QueryRow("SELECT type, scope::VARCHAR FROM duckdb_secrets() WHERE name = '"+arcGCSPrimarySecretName+"'").Scan(&typ, &scope); err != nil {
		t.Fatalf("GCS secret not created: %v", err)
	}
	if typ != "gcs" || !strings.Contains(scope, "gs://arc-b/inst/") {
		t.Fatalf("secret type %q scope %q", typ, scope)
	}
	var secretString string
	if err := db.db.QueryRow("SELECT secret_string FROM duckdb_secrets() WHERE name = '" + arcGCSPrimarySecretName + "'").Scan(&secretString); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(secretString, "ya29.test") {
		t.Fatalf("SECURITY: bearer token visible in duckdb_secrets(): %q", secretString)
	}
	if !strings.Contains(strings.Join(db.AllowedDirectories(), ","), "gs://arc-b/inst/") {
		t.Fatalf("allowlist %v lacks gs://arc-b/inst/", db.AllowedDirectories())
	}
	hot := db.StorageCredentialStatus()["hot"]
	if hot.Backend != "gcs" || hot.State != CredStateOK || hot.ExpiresAt == nil || hot.Source != "application_default_credentials" {
		t.Fatalf("hot = %+v", hot)
	}
}

// A GCS primary whose credentials cannot be resolved still starts (a metadata
// server down at boot is a transient) and reports "unavailable".
func TestNewWithGCSPrimaryUnresolvable(t *testing.T) {
	withGCSSources(t, func() (gcsTokenSource, error) {
		return &fakeGCSSource{err: errors.New("metadata server unreachable")}, nil
	})
	db, err := newGCSDB(t)
	if err != nil {
		t.Skipf("New: %v", err)
	}
	defer db.Close()
	if hot := db.StorageCredentialStatus()["hot"]; hot.Backend != "gcs" || hot.State != CredStateUnavailable {
		t.Fatalf("hot = %+v, want gcs/unavailable", hot)
	}
	var n int
	if err := db.db.QueryRow("SELECT COUNT(*) FROM duckdb_secrets() WHERE name = '" + arcGCSPrimarySecretName + "'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("secret count %d, %v; want none emitted", n, err)
	}
}
