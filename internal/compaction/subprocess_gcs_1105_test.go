package compaction

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/basekick-labs/arc/internal/storage"
	"github.com/rs/zerolog"
)

// gcsKeyFile writes a service-account key whose token endpoint is a local
// stub, so a GCS backend built from it mints its token offline.
func gcsKeyFile(t *testing.T) (string, *atomic.Int32) {
	t.Helper()
	var mints atomic.Int32
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mints.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"ya29.stub","expires_in":3600,"token_type":"Bearer"}`))
	}))
	t.Cleanup(tokenSrv.Close)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "p",
		"private_key_id": "k1",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "arc@p.iam.gserviceaccount.com",
		"client_id":      "1",
		"token_uri":      tokenSrv.URL,
	})
	p := filepath.Join(t.TempDir(), "key.json")
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return p, &mints
}

// #1105: the compaction subprocess rebuilds a GCS parent's backend with the
// parent's bucket, NON-DEFAULT prefix, endpoint and identity, and skips the
// per-job bucket probe. A dropped prefix would reroot compaction at the bucket
// root (the #1102 class); a dropped key would silently switch identity to ADC.
func TestSubprocessGetsTheParentsGCSBackend(t *testing.T) {
	endpoint := "https://storage-psc.p.googleapis.com"
	keyPath, mints := gcsKeyFile(t)
	adc := filepath.Join(t.TempDir(), "adc.json")
	if err := os.WriteFile(adc, []byte(`{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"r"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", adc)

	for _, tc := range []struct {
		name     string
		cfg      storage.GCSConfig
		wantFile string
	}{
		{"service-account key file", storage.GCSConfig{CredentialsFile: keyPath}, keyPath},
		{"application default credentials", storage.GCSConfig{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			// SkipProbe keeps the parent offline (no bucket read).
			cfg.Bucket, cfg.Prefix, cfg.Endpoint, cfg.SkipProbe = "arc-b", "inst/a", endpoint, true
			parent, err := storage.NewGCSBackend(&cfg, zerolog.Nop())
			if err != nil {
				t.Fatalf("parent: %v", err)
			}
			defer parent.Close()

			spec := roundTrip(t, parent, noEnv)
			got := spec.GCS
			if spec.Type != "gcs" || got.Bucket != "arc-b" || got.Prefix != "inst/a/" || got.Endpoint != endpoint {
				t.Fatalf("spec = %+v", spec)
			}
			if got.CredentialsFile != tc.wantFile || got.CredentialsJSON != "" {
				t.Fatalf("credentials file %q json set=%v, want file %q", got.CredentialsFile, got.CredentialsJSON != "", tc.wantFile)
			}
			if !got.SkipProbe {
				t.Fatal("the subprocess must skip the per-job bucket probe")
			}
			before := mints.Load()
			child, err := storage.NewBackend(spec, zerolog.Nop())
			if err != nil {
				t.Fatalf("child: %v", err)
			}
			if n := mints.Load() - before; n != 0 {
				t.Fatalf("the child minted %d tokens at construction; the parent already proved the key", n)
			}
			defer child.Close()
			if child.ConfigJSON() != parent.ConfigJSON() {
				t.Fatalf("child %s != parent %s", child.ConfigJSON(), parent.ConfigJSON())
			}
		})
	}
}

// The job JSON never carries a key path or content outside the credentials
// field, and the field's String form stays redacted.
func TestGCSSubprocessCredentialsStayRedacted(t *testing.T) {
	c := SubprocessCredentials{GCSCredentialsJSON: `{"private_key":"secret"}`}
	if s := c.String(); strings.Contains(s, "secret") {
		t.Fatalf("String() leaked: %s", s)
	}
}
