package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"github.com/basekick-labs/arc/internal/storage"
)

// Google Cloud Storage side of Arc's credential management for DuckDB (#1105);
// shared core in credrefresh.go, S3 and Azure siblings in s3refresh.go and
// azurerefresh.go.
//
// DuckDB's GCS secret takes a static bearer token (TYPE GCS, BEARER_TOKEN) and
// sends it to the XML API for gs:// and gcs:// paths. It has no credential
// chain of its own for GCS, so unlike S3 and Azure there is no fallback secret:
// until the first token is resolved, every gs:// read is refused (HTTP 403
// without a secret, 401 with a dead token — both errors, never "no files").
// The refresher therefore runs with noFallback, and /health reports
// "unavailable" in that window.
//
// Tokens come from storage.GCSCredentials, the function the GCS backend itself
// resolves through, so the identity that writes the bucket is the identity
// DuckDB reads it with.

// arcGCSPrimarySecretName names the primary-storage GCS secret.
const arcGCSPrimarySecretName = "gcs_secret_primary"

// gcsDemotionHint is never shown: a noFallback refresher is never demoted (see
// credentialRefresher.planError). Kept non-empty so a future change to that
// rule does not log an empty hint.
const gcsDemotionHint = "no resolvable GCS credentials: set storage.gcs_credentials_file or configure Application Default Credentials"

// gcsTokenSource is the seam between the resolver and cloud.google.com/go/auth,
// so tests can inject deterministic tokens.
type gcsTokenSource interface {
	Token(ctx context.Context) (*auth.Token, error)
}

// newGCSTokenSource builds the token source for a GCS identity. Package-level
// so tests can substitute a fake.
var newGCSTokenSource = func(cfg *storage.GCSConfig) (gcsTokenSource, error) {
	return storage.GCSCredentials(cfg)
}

// gcsSecretParams describes one DuckDB GCS secret.
type gcsSecretParams struct {
	name  string // secret name
	scope string // gs://bucket/prefix/ this secret applies to
	// endpointHost is DuckDB's ENDPOINT: a bare host[:port]. DuckDB prepends
	// the scheme itself, so "https://host" becomes "https://https://host"
	// (verified on 1.5.6, #1105). Empty means storage.googleapis.com.
	endpointHost string
	bearerToken  string // set per emission by the refresher
}

// buildGCSSecretSQL builds `CREATE OR REPLACE SECRET <name> (TYPE GCS, ...)`.
func buildGCSSecretSQL(p gcsSecretParams) (string, error) {
	if p.bearerToken == "" {
		return "", fmt.Errorf("gcs secret %q: bearer token is required", p.name)
	}
	if p.scope == "" {
		// An unscoped GCS secret would also apply to any gs:// path a query
		// names, carrying Arc's token to buckets that are not Arc's.
		return "", fmt.Errorf("gcs secret %q: scope is required", p.name)
	}
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE SECRET ")
	b.WriteString(p.name)
	b.WriteString(" (\n\tTYPE GCS,\n\tBEARER_TOKEN '")
	b.WriteString(escapeSQLString(p.bearerToken))
	b.WriteString("'")
	if p.endpointHost != "" {
		b.WriteString(",\n\tENDPOINT '")
		b.WriteString(escapeSQLString(p.endpointHost))
		b.WriteString("'")
	}
	b.WriteString(",\n\tSCOPE '")
	b.WriteString(escapeSQLString(p.scope))
	b.WriteString("'\n)")
	return b.String(), nil
}

// gcsPrefixURI is s3PrefixURI for GCS: a normalized "gs://<bucket>/<prefix>/"
// entry, with the same path.Clean handling and bare-bucket fallback. Used for
// both the sandbox allowlist and the secret SCOPE so the two cannot differ.
func gcsPrefixURI(bucket, prefix string) string {
	prefix = strings.TrimLeft(prefix, "/")
	if prefix == "" {
		return "gs://" + bucket + "/"
	}
	prefix = path.Clean(prefix)
	if prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") {
		return "gs://" + bucket + "/"
	}
	return "gs://" + bucket + "/" + prefix + "/"
}

// gcsEndpointHost turns storage.gcs_endpoint into DuckDB's ENDPOINT: the host
// of the normalized JSON API URL. DuckDB speaks the XML API, path style, on
// that host; Private Service Connect endpoints serve both APIs on one host. A
// path in the configured URL cannot be carried over and is dropped.
func gcsEndpointHost(endpoint string) (string, error) {
	if strings.TrimSpace(endpoint) == "" {
		return "", nil
	}
	u, err := storage.GCSEndpointURL(endpoint)
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(u)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "https" {
		// DuckDB would be sent to an https:// URL on this host regardless;
		// config.Load refuses http, so this is a belt for direct callers.
		return "", fmt.Errorf("GCS endpoint %q must use https", endpoint)
	}
	return parsed.Host, nil
}

// primaryGCSSecretParams builds the primary-storage secret template.
func primaryGCSSecretParams(cfg *Config) (gcsSecretParams, error) {
	host, err := gcsEndpointHost(cfg.GCSEndpoint)
	if err != nil {
		return gcsSecretParams{}, err
	}
	return gcsSecretParams{
		name:         arcGCSPrimarySecretName,
		scope:        gcsPrefixURI(cfg.GCSBucket, cfg.GCSPrefix),
		endpointHost: host,
	}, nil
}

// gcsResolver implements credentialResolver for GCS secrets.
//
// invalidate rebuilds the credentials: each auth.Credentials caches its token
// and refreshes it only shortly before expiry, so a fresh one is what makes a
// resolve inside the refresh margin fetch a new token. (The core passes
// invalidate=true on every background resolve.) A metadata server keeps its
// own cache and may return the same token until close to expiry; the core's
// non-advancing poll bridges that, as for Azure.
type gcsResolver struct {
	db        *sql.DB
	params    gcsSecretParams // template; bearerToken filled per emission
	cfg       storage.GCSConfig
	source    string
	src       gcsTokenSource
	lastToken string
	lastExp   time.Time
}

func (g *gcsResolver) Resolve(ctx context.Context, invalidate bool) (credMaterial, error) {
	if g.src == nil || invalidate {
		src, err := newGCSTokenSource(&g.cfg)
		if err != nil {
			return credMaterial{}, fmt.Errorf("resolve GCS credentials: %w", err)
		}
		g.src = src
	}
	tok, err := g.src.Token(ctx)
	if err != nil {
		return credMaterial{}, fmt.Errorf("resolve GCS token: %w", err)
	}
	if tok == nil || tok.Value == "" {
		return credMaterial{}, fmt.Errorf("resolve GCS token: empty token")
	}
	expires := tok.Expiry
	if tok.Value == g.lastToken && !g.lastExp.IsZero() {
		// The same token again. Its Expiry is computed as now+expires_in on
		// every fetch, so it drifts by a second or so and would read as a new
		// session and re-emit the same secret. Report the held expiry so the
		// core takes the non-advancing path instead.
		expires = g.lastExp
	}
	g.lastToken, g.lastExp = tok.Value, expires
	return credMaterial{
		// A zero Expiry is how the auth library marks a token that never
		// expires; an OAuth access token always carries one.
		canExpire:    !expires.IsZero(),
		expires:      expires,
		source:       g.source,
		secretValues: []string{tok.Value},
		payload:      tok.Value,
	}, nil
}

func (g *gcsResolver) Emit(ctx context.Context, m credMaterial) error {
	p := g.params
	p.bearerToken = m.payload.(string)
	secretSQL, err := buildGCSSecretSQL(p)
	if err != nil {
		return err
	}
	_, err = g.db.ExecContext(ctx, secretSQL)
	return err
}

// gcsCredentialSource labels the identity for logs and /health. Never the
// key's path or contents.
func gcsCredentialSource(cfg *Config) string {
	if cfg.GCSCredentialsFile != "" {
		return "service_account_key"
	}
	return "application_default_credentials"
}

// startGCSRefresher validates the secret template and starts — or
// stop-and-replaces — the refresher for params.name. Only a template error is
// returned (startup-fatal, as for S3 and Azure). A credential that cannot be
// resolved yet is logged at Error and retried in the background; there is no
// fallback secret, so gs:// reads fail until it resolves.
func (d *DuckDB) startGCSRefresher(params gcsSecretParams, cfg *Config) error {
	probe := params
	probe.bearerToken = "VALIDATE"
	if _, err := buildGCSSecretSQL(probe); err != nil {
		return fmt.Errorf("invalid gcs secret template for %q: %w", params.name, err)
	}
	resolver := &gcsResolver{
		db:     d.db,
		params: params,
		cfg: storage.GCSConfig{
			Bucket:          cfg.GCSBucket,
			Prefix:          cfg.GCSPrefix,
			CredentialsFile: cfg.GCSCredentialsFile,
		},
		source: gcsCredentialSource(cfg),
	}
	firstFailure := func(err error) {
		d.logger.Error().Err(err).
			Str("secret", params.name).
			Str("source", resolver.source).
			Msg("GCS credentials not resolvable yet; queries of GCS storage fail until they are (retrying in the background)")
	}

	d.refresherMu.Lock()
	defer d.refresherMu.Unlock()
	if old := d.s3Refreshers[params.name]; old != nil {
		old.stop()
	}
	d.s3Refreshers[params.name] = startCredentialRefresher(resolver, params.name, d.logger, gcsDemotionHint, true, firstFailure)
	d.logger.Info().Str("secret", params.name).Str("scope", params.scope).
		Str("endpoint_host", params.endpointHost).
		Msg("GCS query secret managed by Arc (bearer token, refreshed before expiry)")
	return nil
}

// gcsTierStatus describes a GCS tier from the refresher registry.
func gcsTierStatus(secretName string, refreshers map[string]*credentialRefresher, now time.Time) StorageTierStatus {
	if r := refreshers[secretName]; r != nil {
		st := r.status(now)
		st.Backend, st.Credentials = "gcs", s3ModeSDKManaged
		return st
	}
	// Only reachable if startGCSRefresher was never called for a GCS primary,
	// which New prevents; report the truth rather than "ok".
	return StorageTierStatus{Backend: "gcs", Credentials: s3ModeSDKManaged, State: CredStateUnavailable}
}
