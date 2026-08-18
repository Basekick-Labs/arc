package database

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/rs/zerolog"
)

// fakeAzureCred is a deterministic azureTokenCredential.
type fakeAzureCred struct {
	tok   azcore.AccessToken
	err   error
	calls atomic.Int64
}

func (f *fakeAzureCred) GetToken(ctx context.Context, o policy.TokenRequestOptions) (azcore.AccessToken, error) {
	f.calls.Add(1)
	if f.err != nil {
		return azcore.AccessToken{}, f.err
	}
	return f.tok, nil
}

// TestBuildAzureSecretSQL_AccessToken pins the refresher emission shape (#605):
// PROVIDER ACCESS_TOKEN + ACCOUNT_NAME (+ ENDPOINT when configured), escaping,
// and the mutual-exclusion/template rules.
func TestBuildAzureSecretSQL_AccessToken(t *testing.T) {
	got, err := buildAzureSecretSQL(azureSecretParams{
		name: arcAzurePrimarySecretName, accountName: "myacct",
		accessToken: "eyJ0token'with'quotes", scope: "azure://cont/",
		endpoint: "blob.core.usgovcloudapi.net",
	})
	if err != nil {
		t.Fatalf("buildAzureSecretSQL: %v", err)
	}
	mustContain(t, got, "PROVIDER ACCESS_TOKEN")
	mustContain(t, got, "ACCESS_TOKEN 'eyJ0token''with''quotes'")
	mustContain(t, got, "ACCOUNT_NAME 'myacct'")
	mustContain(t, got, "ENDPOINT 'blob.core.usgovcloudapi.net'")
	mustContain(t, got, "SCOPE 'azure://cont/'")

	if _, err := buildAzureSecretSQL(azureSecretParams{name: "x", accessToken: "t"}); err == nil {
		t.Error("access token without account name must error")
	}
	if _, err := buildAzureSecretSQL(azureSecretParams{name: "x", accountName: "a", accessToken: "t", accountKey: "k"}); err == nil {
		t.Error("access token + static credentials must error")
	}
}

// TestAzureAccessTokenSecretExecutesAndRedacts runs the refresher-shaped azure
// secret against real DuckDB: accepted, and ACCESS_TOKEN redacted in
// duckdb_secrets() — the manager is readable by any authenticated query user.
func TestAzureAccessTokenSecretExecutesAndRedacts(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	if _, err := db.Exec("INSTALL azure"); err != nil {
		t.Skipf("azure extension unavailable (offline?): %v", err)
	}
	if _, err := db.Exec("LOAD azure"); err != nil {
		t.Skipf("azure extension unavailable (offline?): %v", err)
	}
	const token = "SUPERSECRETBEARERxyz"
	stmt, err := buildAzureSecretSQL(azureSecretParams{
		name: arcAzurePrimarySecretName, accountName: "myacct", accessToken: token,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("DuckDB rejected access_token secret: %v\nSQL:\n%s", err, stmt)
	}
	var secretString string
	if err := db.QueryRow("SELECT secret_string FROM duckdb_secrets() WHERE name = ?", arcAzurePrimarySecretName).Scan(&secretString); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(secretString, "SUPERSECRETBEARER") {
		t.Fatalf("token visible in duckdb_secrets(): %s", secretString)
	}
	mustContain(t, secretString, "access_token=redacted")
}

// TestAzureEndpointSuffix pins the full-URL→suffix normalization (#605 M4).
func TestAzureEndpointSuffix(t *testing.T) {
	cases := []struct {
		in, acct, want string
		ok             bool
	}{
		{"", "a", "", true},
		{"https://myacct.blob.core.windows.net", "myacct", "blob.core.windows.net", true},
		{"https://myacct.blob.core.usgovcloudapi.net", "myacct", "blob.core.usgovcloudapi.net", true},
		{"blob.core.windows.net", "myacct", "blob.core.windows.net", true},
		{"http://127.0.0.1:10000/devstoreaccount1", "devstoreaccount1", "", false}, // Azurite path-style
	}
	for _, tc := range cases {
		got, ok := azureEndpointSuffix(tc.in, tc.acct)
		if got != tc.want || ok != tc.ok {
			t.Errorf("azureEndpointSuffix(%q,%q) = %q,%v want %q,%v", tc.in, tc.acct, got, ok, tc.want, tc.ok)
		}
	}
}

// TestAzureResolverThroughCore exercises the azure resolver via the shared
// core against real DuckDB: first fill emits, a same-token resolve is
// non-advancing (Emit skipped), an advanced token re-emits.
func TestAzureResolverThroughCore(t *testing.T) {
	db := openTestDuckDBWithHTTPFS(t)
	if _, err := db.Exec("INSTALL azure; LOAD azure"); err != nil {
		t.Skipf("azure extension unavailable: %v", err)
	}
	now := time.Now()
	fake := &fakeAzureCred{tok: azcore.AccessToken{Token: "tok1", ExpiresOn: now.Add(time.Hour)}}
	r := &credentialRefresher{
		resolver: &azureResolver{db: db, cred: fake,
			params: azureSecretParams{name: "az_core_test", accountName: "acct"}},
		logger: zerolog.Nop(),
	}
	if _, err := r.refreshOnce(context.Background(), false); err != nil {
		t.Fatalf("first fill: %v", err)
	}
	if _, err := r.refreshOnce(context.Background(), true); err != errNonAdvancingExpiry {
		t.Fatalf("same token must be non-advancing, got %v", err)
	}
	fake.tok = azcore.AccessToken{Token: "tok2", ExpiresOn: now.Add(2 * time.Hour)}
	if _, err := r.refreshOnce(context.Background(), true); err != nil {
		t.Fatalf("advanced token: %v", err)
	}
	var provider string
	if err := db.QueryRow("SELECT provider FROM duckdb_secrets() WHERE name='az_core_test'").Scan(&provider); err != nil {
		t.Fatal(err)
	}
	if provider != "access_token" {
		t.Fatalf("provider = %q", provider)
	}
}

// TestNewStartsAzureRefresherWhenManaged pins the hot-tier wiring (#605): an
// Azure primary with no static credentials must start the refresher, whose
// synchronous first resolve emits the access_token secret before New returns —
// and /health must report it sdk_managed/ok.
func TestNewStartsAzureRefresherWhenManaged(t *testing.T) {
	hermeticAWSEnv(t)
	orig := newAzureCredProvider
	fake := &fakeAzureCred{tok: azcore.AccessToken{Token: "hotTok", ExpiresOn: time.Now().Add(time.Hour)}}
	newAzureCredProvider = func() (azureTokenCredential, error) { return fake, nil }
	t.Cleanup(func() { newAzureCredProvider = orig })

	tmp := t.TempDir()
	db, err := New(&Config{
		MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: tmp,
		LocalStorageRoot:      filepath.Join(tmp, "d"),
		AzureIsPrimaryBackend: true, AzureAccountName: "hotacct", AzureContainer: "cont",
	}, zerolog.Nop())
	if err != nil {
		if strings.Contains(err.Error(), "azure") || strings.Contains(err.Error(), "httpfs") {
			t.Skipf("extension unavailable: %v", err)
		}
		t.Fatalf("New: %v", err)
	}
	defer db.Close()

	var provider string
	if err := db.DB().QueryRow("SELECT provider FROM duckdb_secrets() WHERE name = ?", arcAzurePrimarySecretName).Scan(&provider); err != nil {
		t.Fatalf("primary azure secret not registered: %v", err)
	}
	if provider != "access_token" {
		t.Fatalf("provider = %q, want access_token (refresher-managed)", provider)
	}
	hot := db.StorageCredentialStatus()["hot"]
	if hot.Backend != "azure" || hot.Credentials != s3ModeSDKManaged || hot.State != CredStateOK || hot.ExpiresAt == nil {
		t.Fatalf("hot = %+v", hot)
	}
	if fake.calls.Load() == 0 {
		t.Fatal("injected azure credential never used")
	}
}

// TestAzureSASNeverStartsRefresher (#605 review M6): a SAS-scoped deployment
// must keep today's chain secret and report sas/unknown — a managed refresher
// would acquire a BROADER identity than the operator deliberately scoped.
func TestAzureSASNeverStartsRefresher(t *testing.T) {
	hermeticAWSEnv(t)
	orig := newAzureCredProvider
	newAzureCredProvider = func() (azureTokenCredential, error) {
		t.Fatal("SAS deployment must never construct an azure credential provider")
		return nil, nil
	}
	t.Cleanup(func() { newAzureCredProvider = orig })

	tmp := t.TempDir()
	db, err := New(&Config{
		MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: tmp,
		LocalStorageRoot:      filepath.Join(tmp, "d"),
		AzureIsPrimaryBackend: true, AzureAccountName: "sasacct",
		AzureSASToken: "sv=2024&sig=xyz",
	}, zerolog.Nop())
	if err != nil {
		if strings.Contains(err.Error(), "azure") || strings.Contains(err.Error(), "httpfs") {
			t.Skipf("extension unavailable: %v", err)
		}
		t.Fatalf("New: %v", err)
	}
	defer db.Close()
	hot := db.StorageCredentialStatus()["hot"]
	if hot.Credentials != credModeSAS || hot.State != CredStateUnknown {
		t.Fatalf("SAS hot = %+v, want sas/unknown", hot)
	}
	if db.s3Refreshers[arcAzurePrimarySecretName] != nil {
		t.Fatal("SAS deployment must not register a refresher")
	}
}

// TestConfigureAzureColdManagedStartsRefresher pins cold-tier routing +
// the record-after-success rule for azure (#603 H1 / #605).
func TestConfigureAzureColdManagedStartsRefresher(t *testing.T) {
	hermeticAWSEnv(t)
	orig := newAzureCredProvider
	fake := &fakeAzureCred{tok: azcore.AccessToken{Token: "coldTok", ExpiresOn: time.Now().Add(time.Hour)}}
	newAzureCredProvider = func() (azureTokenCredential, error) { return fake, nil }
	t.Cleanup(func() { newAzureCredProvider = orig })

	tmp := t.TempDir()
	db, err := New(&Config{
		MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: tmp,
		LocalStorageRoot: filepath.Join(tmp, "d"),
		// azure cold requires the extension pre-loaded at startup:
		ColdAzureContainer: "coldcont",
	}, zerolog.Nop())
	if err != nil {
		if strings.Contains(err.Error(), "azure") || strings.Contains(err.Error(), "httpfs") {
			t.Skipf("extension unavailable: %v", err)
		}
		t.Fatalf("New: %v", err)
	}
	defer db.Close()

	if err := db.ConfigureAzure(&AzureConfig{AccountName: "coldacct", Container: "coldcont"}); err != nil {
		t.Fatalf("ConfigureAzure: %v", err)
	}
	cold := db.StorageCredentialStatus()["cold"]
	if cold.Backend != "azure" || cold.Credentials != s3ModeSDKManaged || cold.State != CredStateOK {
		t.Fatalf("cold = %+v", cold)
	}
	var provider string
	if err := db.DB().QueryRow("SELECT provider FROM duckdb_secrets() WHERE name = ?", arcAzureColdSecretName).Scan(&provider); err != nil {
		t.Fatal(err)
	}
	if provider != "access_token" {
		t.Fatalf("cold provider = %q", provider)
	}

	// Record-after-success: a failing ConfigureAzure must leave no cold entry.
	db2, err := New(&Config{
		MaxConnections: 2, MemoryLimit: "256MB", TempDirectory: t.TempDir(),
		LocalStorageRoot: filepath.Join(t.TempDir(), "d"),
	}, zerolog.Nop())
	if err != nil {
		t.Skipf("New: %v", err)
	}
	defer db2.Close()
	if err := db2.ConfigureAzure(&AzureConfig{ /* no account name, no conn string */ }); err == nil {
		t.Fatal("ConfigureAzure without identity must error")
	}
	if _, ok := db2.StorageCredentialStatus()["cold"]; ok {
		t.Fatal("failed cold azure tier must be absent from /health")
	}
}
