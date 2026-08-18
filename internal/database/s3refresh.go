package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/rs/zerolog"
)

// This file owns Arc-side S3 credential management for DuckDB (#600).
//
// DuckDB's own credential resolution is a dead end for temporary credentials:
// PROVIDER CREDENTIAL_CHAIN resolves once at CREATE SECRET time, and the 1.5.5
// CHAIN 'web_identity' + REFRESH auto path was verified live (2026-08-18,
// real AWS STS, 1h session) to never refresh for Arc's workload — proactive
// refresh skips globbed reads, and the reactive path arms only on HTTP 401/403
// while an expired STS token surfaces as HTTP 400.
//
// So Arc resolves credentials itself with the Go AWS SDK — the same chain the
// ingest path uses, which is why writes never suffered from #600 — and hands
// DuckDB plain session credentials via CREATE OR REPLACE SECRET, re-issued
// before expiry. Replacing the secret takes effect for subsequent requests
// across the whole pool (verified against the bundled engine: request signing
// looks the secret up per request; a live process dead with ExpiredToken
// recovers on the next query after re-emission, no restart).

const (
	// s3RefreshMargin is how long before credential expiry the refresher
	// re-resolves. It must comfortably exceed the default query timeout (300s)
	// so credentials stay valid for the lifetime of queries started just before
	// a rotation. Queries longer than the margin may still see one retryable
	// ExpiredToken at rotation.
	s3RefreshMargin = 10 * time.Minute
	// s3RefreshMinDelay floors the delay between refreshes so pathologically
	// short sessions cannot spin the loop.
	s3RefreshMinDelay = time.Minute
	// s3FirstResolveTimeout bounds the synchronous resolve during startup /
	// ConfigureS3. STS unreachable (GovCloud endpoints, proxy misconfig) must
	// degrade to the background retry loop, not stall startup — database.New
	// failures are fatal in main.go.
	s3FirstResolveTimeout = 10 * time.Second
	// s3ResolveTimeout bounds each background resolve+emit.
	s3ResolveTimeout = 30 * time.Second

	s3RefreshBackoffMin = 30 * time.Second
	s3RefreshBackoffMax = 5 * time.Minute
)

// awsCredentialsProvider is the seam between the refresher and the AWS SDK,
// so tests can inject deterministic providers.
type awsCredentialsProvider interface {
	Retrieve(ctx context.Context) (aws.Credentials, error)
}

// newAWSCredProvider builds the SDK default credential chain (web identity /
// IRSA, env, shared config, IMDS) for the refresher. Package-level so tests can
// substitute a fake.
//
// The returned provider is an aws.CredentialsCache. A PROACTIVE refresh (before
// expiry) must call Invalidate() first: the cache serves cached credentials
// until it considers them expired, so a plain Retrieve at Expires−margin
// returns the SAME session and the refresh accomplishes nothing. (An
// ExpiryWindow does NOT fix this — the SDK applies the window once at store
// time, shifting the reported/stored Expires earlier, and still serves the
// cache until that shifted instant. Verified live 2026-08-18: with
// ExpiryWindow=11m a 1h session was reported as expires=+49m and the +39m
// proactive Retrieve returned it unchanged, tripping the non-advancing guard
// until the cache itself relented at +49m.)
var newAWSCredProvider = func(ctx context.Context, region string) (awsCredentialsProvider, error) {
	var opts []func(*config.LoadOptions) error
	// Only pin the region when Arc has one configured; otherwise let the SDK's
	// own resolution win (AWS_REGION is injected by the IRSA webhook).
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return cfg.Credentials, nil
}

// s3CredentialRefresher keeps one DuckDB S3 secret stocked with fresh session
// credentials. One instance per managed secret (primary, cold); owned by the
// DuckDB struct, which stops it on Close.
type s3CredentialRefresher struct {
	db       *sql.DB
	params   s3SecretParams // template; key/secret/session token filled per emission
	provider awsCredentialsProvider
	logger   zerolog.Logger
	cancel   context.CancelFunc
	done     chan struct{}

	// lastExpiry guards against a mis-cached provider handing back the same
	// credentials: every successful refresh must advance the expiry.
	lastExpiry time.Time
}

// startS3CredentialRefresher performs one synchronous resolve+emit (bounded by
// s3FirstResolveTimeout) and then maintains the secret in the background. It
// never fails: an unreachable STS or an unprojected token degrades to a Warn
// and background retries, matching the principle that a transient credential
// race must not turn into a startup failure.
func startS3CredentialRefresher(db *sql.DB, params s3SecretParams, provider awsCredentialsProvider, logger zerolog.Logger) *s3CredentialRefresher {
	ctx, cancel := context.WithCancel(context.Background())
	r := &s3CredentialRefresher{
		db:       db,
		params:   params,
		provider: provider,
		logger: logger.With().
			Str("component", "s3-cred-refresher").
			Str("secret", params.name).Logger(),
		cancel: cancel,
		done:   make(chan struct{}),
	}

	fctx, fcancel := context.WithTimeout(ctx, s3FirstResolveTimeout)
	creds, err := r.resolveAndEmit(fctx, false)
	fcancel()

	switch {
	case err != nil:
		r.logger.Warn().Err(err).
			Msg("S3 credentials not yet resolvable; S3 queries will fail until a background refresh succeeds")
		go r.loop(ctx, s3RefreshBackoffMin)
	case !creds.CanExpire:
		// Static credentials resolved through the chain — nothing to refresh.
		r.logger.Info().Msg("resolved non-expiring S3 credentials; refresh loop not needed")
		close(r.done)
	default:
		go r.loop(ctx, refreshDelay(creds.Expires))
	}
	return r
}

// stop cancels the refresher and waits for the background goroutine to finish,
// so no Exec can race the pool's Close.
func (r *s3CredentialRefresher) stop() {
	r.cancel()
	<-r.done
}

func (r *s3CredentialRefresher) loop(ctx context.Context, initialDelay time.Duration) {
	defer close(r.done)
	backoff := s3RefreshBackoffMin
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		rctx, rcancel := context.WithTimeout(ctx, s3ResolveTimeout)
		creds, err := r.resolveAndEmit(rctx, true)
		rcancel()

		switch {
		case ctx.Err() != nil:
			// Shutdown, not a failure.
			r.logger.Debug().Msg("credential refresh stopped")
			return
		case err != nil:
			r.logger.Error().Err(err).Dur("retry_in", backoff).
				Msg("S3 credential refresh failed; existing credentials remain until expiry")
			timer.Reset(backoff)
			backoff = min(backoff*2, s3RefreshBackoffMax)
		case !creds.CanExpire:
			r.logger.Info().Msg("credentials became non-expiring; refresh loop exiting")
			return
		default:
			backoff = s3RefreshBackoffMin
			timer.Reset(refreshDelay(creds.Expires))
		}
	}
}

// resolveAndEmit retrieves credentials from the chain and re-emits the DuckDB
// secret with them. Credential values are never logged.
//
// invalidate must be true for every PROACTIVE resolve (the scheduled refresh
// and its retries): it drops the provider's cached session so Retrieve performs
// a real STS exchange instead of returning the credentials we already hold.
// The initial fill passes false — there is nothing cached yet, and on a
// provider without Invalidate (tests) the assertion is simply skipped.
func (r *s3CredentialRefresher) resolveAndEmit(ctx context.Context, invalidate bool) (aws.Credentials, error) {
	if invalidate {
		if inv, ok := r.provider.(interface{ Invalidate() }); ok {
			inv.Invalidate()
		}
	}
	creds, err := r.provider.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, fmt.Errorf("resolve AWS credentials: %w", err)
	}
	// Refuse credential material containing control bytes BEFORE it reaches SQL
	// construction. escapeSQLString handles quotes, but a NUL truncates the C
	// string inside DuckDB's parser, whose "unterminated quoted string" error
	// echoes the credential prefix — which resolveAndEmit's callers log. Real
	// AWS credentials are ASCII and never trip this; the check exists so the
	// no-credentials-in-logs guarantee doesn't rest on that assumption. The
	// error is a fixed string on purpose: never include the values.
	for _, v := range []string{creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken} {
		if strings.ContainsFunc(v, func(c rune) bool { return c < 0x20 || c == 0x7f }) {
			return aws.Credentials{}, fmt.Errorf("resolved credential material contains control bytes; refusing to emit secret")
		}
	}
	if creds.CanExpire && !r.lastExpiry.IsZero() && !creds.Expires.After(r.lastExpiry) {
		// The provider handed back the same session. With a correctly configured
		// ExpiryWindow this cannot happen (see newAWSCredProvider); treat it as a
		// failure so backoff applies rather than re-emitting a dying credential.
		return aws.Credentials{}, fmt.Errorf(
			"credential provider returned non-advancing expiry %s (cached credentials?)",
			creds.Expires.UTC().Format(time.RFC3339))
	}

	p := r.params
	p.accessKey = creds.AccessKeyID
	p.secretKey = creds.SecretAccessKey
	p.sessionToken = creds.SessionToken
	secretSQL, err := buildS3SecretSQL(p)
	if err != nil {
		return aws.Credentials{}, err
	}
	if _, err := r.db.ExecContext(ctx, secretSQL); err != nil {
		return aws.Credentials{}, fmt.Errorf("emit S3 secret: %w", err)
	}
	if creds.CanExpire {
		r.lastExpiry = creds.Expires
		r.logger.Info().Time("expires", creds.Expires.UTC()).
			Msg("DuckDB S3 secret refreshed with new session credentials")
	} else {
		r.logger.Info().Msg("DuckDB S3 secret emitted (non-expiring credentials)")
	}
	return creds, nil
}

// refreshDelay schedules the next refresh s3RefreshMargin before `expires`,
// floored at s3RefreshMinDelay. With no ExpiryWindow configured (see
// newAWSCredProvider) `expires` is the real STS session expiry, so the margin
// is the true headroom; the proactive resolve Invalidates the cache first,
// which is what makes firing before expiry actually produce a new session.
func refreshDelay(expires time.Time) time.Duration {
	d := time.Until(expires) - s3RefreshMargin
	if d < s3RefreshMinDelay {
		d = s3RefreshMinDelay
	}
	return d
}
