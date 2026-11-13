package storage

// The GCS configuration and identity, shared by the native GCS backend
// (gcs.go, not in the FIPS build) and DuckDB's GCS query secret
// (internal/database/gcsrefresh.go). Nothing here imports Google's API client,
// which pulls golang.org/x/crypto/hkdf through S2A (#1587), so this file is in
// every build.

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
)

// ErrGCSNotInFIPSBuild is the FIPS build's answer to a native GCS backend
// (#1587). Config validation refuses storage.backend, a cold tier and a backup
// target of type "gcs" before anything is built; NewGCSBackend returns this as
// the backstop.
var ErrGCSNotInFIPSBuild = errors.New("the native GCS backend is not available in the FIPS build: the Google API client links non-FIPS crypto (golang.org/x/crypto/hkdf); use S3, Azure, or GCS through its S3-compatible endpoint (storage.googleapis.com with HMAC keys)")

// gcsScope is the OAuth scope every GCS credential is minted for: object
// reads, writes and deletes, and the bucket metadata read of the startup probe.
const gcsScope = "https://www.googleapis.com/auth/devstorage.read_write"

// GCSConfig holds Google Cloud Storage backend configuration.
type GCSConfig struct {
	// Bucket is the bucket name (required).
	Bucket string

	// Prefix is an object-name prefix within the bucket, applied to every key
	// the backend touches. Empty means the bucket root. Same contract as
	// S3Config.Prefix and AzureBlobConfig.Prefix.
	Prefix string

	// CredentialsFile is the path of a service-account key file. Only the
	// service-account type is accepted: an arbitrary credential configuration
	// from a file is the shape Google deprecated for loading unvalidated
	// external credentials.
	CredentialsFile string

	// CredentialsJSON is a service-account key's contents. It takes precedence
	// over CredentialsFile. The compaction subprocess receives the key this way
	// so that no key path or content ever appears on its command line.
	CredentialsJSON string

	// With neither of the above, the backend uses Application Default
	// Credentials: GOOGLE_APPLICATION_CREDENTIALS, the gcloud ADC file, or the
	// metadata server (GCE, GKE Workload Identity, Cloud Run).

	// Endpoint overrides the JSON API endpoint (Private Service Connect, an
	// emulator): a host ("https://storage-psc.p.googleapis.com", which gets
	// the JSON API path) or the full base URL. Empty means Google's default.
	// See GCSEndpointURL.
	Endpoint string

	// Role is what Arc uses this bucket for. It picks the wording of the
	// startup warnings (a colder storage class is a mistake for primary data
	// and often the point for a backup) and how Application Default
	// Credentials are resolved: eagerly for the primary, whose failure is a
	// failed start, lazily for a cold tier or backup target, as the S3 and
	// Azure SDK chains do. Empty means GCSRolePrimary.
	Role GCSRole

	// SkipProbe skips the construction-time checks: the bucket metadata read,
	// and minting a token to prove an explicit key. The compaction subprocess
	// sets it: its parent already ran both at startup, and repeating them per
	// job would add requests and failure points to every job for no new
	// information.
	SkipProbe bool
}

// GCSRole is what Arc uses a GCS bucket for; see GCSConfig.Role.
type GCSRole string

const (
	GCSRolePrimary GCSRole = ""
	GCSRoleCold    GCSRole = "cold"
	GCSRoleBackup  GCSRole = "backup"
)

// GCSEndpointURL turns a configured endpoint into the base URL the JSON API
// client resolves every call against.
//
// The client uses the string as given and resolves "b/{bucket}/o" against it,
// while uploads go to the absolute "/upload/storage/v1/...". So a spelling
// without the trailing "/storage/v1/" left uploads working and sent every other
// call to the wrong path, where the 404 read as a missing bucket. And a URL
// the client cannot parse panicked inside the first call instead of failing
// construction. So: a scheme is required, a bare host gets the JSON API path,
// and the result always ends in a separator.
func GCSEndpointURL(endpoint string) (string, error) {
	e := strings.TrimSpace(endpoint)
	if !strings.Contains(e, "://") {
		e = "https://" + e
	}
	u, err := url.Parse(e)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("GCS endpoint is not a usable http(s) URL")
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/storage/v1/"
	}
	if !strings.HasSuffix(u.Path, "/") {
		u.Path += "/"
	}
	return u.String(), nil
}

// GCSCredentials resolves the identity cfg names: an explicit service-account
// key, or Application Default Credentials. The backend and DuckDB's query
// secret (internal/database) both resolve through it, so the identity that
// writes the bucket is the identity that reads it.
func GCSCredentials(cfg *GCSConfig) (*auth.Credentials, error) {
	opts := &credentials.DetectOptions{Scopes: []string{gcsScope}}
	key := []byte(cfg.CredentialsJSON)
	if len(key) == 0 && cfg.CredentialsFile != "" {
		data, err := os.ReadFile(cfg.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read GCS credentials file: %w", err)
		}
		key = data
	}
	if len(key) > 0 {
		// The error is not wrapped: a parse failure of a key file can quote
		// the file's contents.
		creds, err := credentials.NewCredentialsFromJSON(credentials.ServiceAccount, key, opts)
		if err != nil {
			return nil, fmt.Errorf("GCS credentials are not a usable service-account key")
		}
		return creds, nil
	}
	if cfg.Role != GCSRolePrimary {
		// A cold tier or backup target that cannot find credentials at boot
		// still builds, as an S3 or Azure one does; its first request then
		// fails loudly. Built eagerly instead, it failed construction and
		// left the node without that backend at all, which for a cold tier
		// meant reads answered without the cold data (#1105).
		return auth.NewCredentials(&auth.CredentialsOptions{
			TokenProvider: &lazyADC{opts: opts},
		}), nil
	}
	creds, err := credentials.DetectDefault(opts)
	if err != nil {
		return nil, fmt.Errorf("no GCS credentials: set a service-account key or configure Application Default Credentials: %w", err)
	}
	return creds, nil
}

// lazyADC resolves Application Default Credentials on the first token
// request, then delegates to them (they cache and refresh their own token).
// A failed detection is retried on the next request.
type lazyADC struct {
	opts *credentials.DetectOptions
	mu   sync.Mutex
	tp   auth.TokenProvider
}

func (l *lazyADC) Token(ctx context.Context) (*auth.Token, error) {
	l.mu.Lock()
	if l.tp == nil {
		creds, err := credentials.DetectDefault(l.opts)
		if err != nil {
			l.mu.Unlock()
			return nil, fmt.Errorf("no GCS credentials: set a service-account key or configure Application Default Credentials: %w", err)
		}
		l.tp = creds
	}
	tp := l.tp
	l.mu.Unlock()
	// Outside the lock: the credentials serialise their own refresh, and a
	// slow token fetch must not queue every other request behind it.
	return tp.Token(ctx)
}
