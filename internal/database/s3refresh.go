package database

import (
	"context"
	"database/sql"
	"errors"
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
	// s3RefreshBackoffMaxUnproven caps retries for a refresher that has NEVER
	// succeeded and is running behind the fallback CREDENTIAL_CHAIN secret
	// (e.g. anonymous MinIO with no resolvable AWS credentials). Such a
	// deployment is healthy; polling it every 5 minutes forever at Error level
	// would produce ~288 spurious error lines/day. See planNext.
	s3RefreshBackoffMaxUnproven = 15 * time.Minute
	// s3RefreshErrorsBeforeDemotion: a never-succeeded refresher logs its first
	// failures at Error (a genuinely broken IMDS/STS at boot must be visible),
	// then demotes to Debug with one Warn marking the transition.
	s3RefreshErrorsBeforeDemotion = 3
	// s3RefreshFinalWarnWindow: a non-advancing resolve (the upstream source
	// has not rotated yet — normal for IMDS and Pod Identity, which rotate
	// server-side on their own schedule) logs Debug while there is plenty of
	// runway, escalating to Warn inside this window before the held
	// credentials' expiry so an operator has reaction time.
	s3RefreshFinalWarnWindow = 5 * time.Minute
)

// errNonAdvancingExpiry marks a resolve that succeeded but returned the same
// session we already hold — the upstream source has not rotated yet. Not a
// failure: the held credentials remain valid; the loop polls at a flat
// s3RefreshMinDelay until the source rotates.
var errNonAdvancingExpiry = errors.New("credential provider returned a non-advancing expiry (upstream not rotated yet)")

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

	// lastExpiry drives the non-advancing check: every successful refresh must
	// advance the expiry, else the upstream source has not rotated yet.
	lastExpiry time.Time

	// Scheduling/severity state, owned by the loop goroutine (and the sync
	// first resolve before the goroutine starts).
	everSucceeded     bool
	consecutiveErrors int
}

// startS3CredentialRefresher performs one synchronous resolve+emit (bounded by
// s3FirstResolveTimeout) and then maintains the secret in the background. It
// never fails: an unreachable STS or an unprojected token degrades to a Warn
// and background retries, matching the principle that a transient credential
// race must not turn into a startup failure.
// onFirstFailure, when non-nil, runs synchronously after a failed first
// resolve and BEFORE the retry loop starts — so a caller-emitted fallback
// secret is structurally guaranteed to land before the loop's first managed
// emit can replace it (#601 review M1; without the hook that ordering rested
// on the 30s initial backoff being longer than the fallback Exec).
func startS3CredentialRefresher(db *sql.DB, params s3SecretParams, provider awsCredentialsProvider, logger zerolog.Logger, onFirstFailure func(error)) *s3CredentialRefresher {
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
		if onFirstFailure != nil {
			onFirstFailure(err)
		}
		go r.loop(ctx, s3RefreshBackoffMin)
	case !creds.CanExpire:
		// Static credentials resolved through the chain — nothing to refresh.
		r.everSucceeded = true
		r.logger.Info().Msg("resolved non-expiring S3 credentials; refresh loop not needed")
		close(r.done)
	default:
		r.everSucceeded = true
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

		if ctx.Err() != nil {
			// Shutdown, not a failure.
			r.logger.Debug().Msg("credential refresh stopped")
			return
		}

		var delay time.Duration
		switch {
		case err == nil && !creds.CanExpire:
			r.logger.Info().Msg("credentials became non-expiring; refresh loop exiting")
			return
		case err == nil:
			delay = r.planSuccess(creds)
		case errors.Is(err, errNonAdvancingExpiry):
			delay = r.planNonAdvancing()
		default:
			delay = r.planError(err)
		}
		timer.Reset(delay)
	}
}

// planSuccess, planNonAdvancing and planError decide the wait before the next
// attempt and do the outcome's logging. They are separated from loop so the
// scheduling/severity policy is unit-testable without real waits.

func (r *s3CredentialRefresher) planSuccess(creds aws.Credentials) time.Duration {
	r.everSucceeded = true
	r.consecutiveErrors = 0
	return refreshDelay(creds.Expires)
}

// planNonAdvancing: the upstream source has not rotated yet. The held secret
// stays valid; poll at the flat minimum until it rotates. Debug while there is
// runway, Warn inside the final window. NOTE: this outcome is expected in the
// tail of every IMDS / Pod Identity session — do not "fix" it back into an
// error, and do not add backoff (a backoff step could straddle the rotation).
func (r *s3CredentialRefresher) planNonAdvancing() time.Duration {
	r.consecutiveErrors = 0
	remaining := time.Until(r.lastExpiry)
	// Deliberate: if the source NEVER rotates and the held credentials expire,
	// this keeps warning once a minute indefinitely. Queries are failing in
	// that state; unlike the never-succeeded error loop (demoted — the
	// deployment there is healthy on the fallback), this noise is proportionate.
	ev := r.logger.Debug()
	if remaining < s3RefreshFinalWarnWindow {
		ev = r.logger.Warn()
	}
	ev.Dur("held_credentials_expire_in", remaining.Round(time.Second)).
		Msg("credential source has not rotated yet; polling")
	return s3RefreshMinDelay
}

// planError: a real resolve/emit failure. A refresher that has succeeded
// before keeps loud Error + 30s→5m backoff — its credentials WILL die at
// expiry. A refresher that has NEVER succeeded is running behind the fallback
// CREDENTIAL_CHAIN secret (see startRefresher): the deployment may simply have
// no resolvable AWS credentials (anonymous MinIO), so after
// s3RefreshErrorsBeforeDemotion failures it demotes to Debug with a longer cap
// — one Warn marks the demotion so the state is diagnosable from logs. It
// never gives up: an EC2 host whose IMDS was down at boot is picked up by a
// later retry.
func (r *s3CredentialRefresher) planError(err error) time.Duration {
	r.consecutiveErrors++
	n := r.consecutiveErrors
	backoffCap := s3RefreshBackoffMax
	ev := r.logger.Error()
	if !r.everSucceeded {
		backoffCap = s3RefreshBackoffMaxUnproven
		switch {
		case n == s3RefreshErrorsBeforeDemotion+1:
			ev = r.logger.Warn()
			ev = ev.Str("hint", "no resolvable AWS credentials; running on DuckDB's credential chain — configure keys, or set AWS_EC2_METADATA_DISABLED=true to fast-fail the probe")
		case n > s3RefreshErrorsBeforeDemotion+1:
			ev = r.logger.Debug()
		}
	}
	delay := s3RefreshBackoffMin << (n - 1)
	if delay > backoffCap || delay <= 0 {
		delay = backoffCap
	}
	ev.Err(err).Dur("retry_in", delay).
		Msg("S3 credential refresh failed; existing secret remains in place")
	return delay
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
		// Same session as we already hold. For server-rotated sources (IMDS,
		// Pod Identity) this is NORMAL near the end of a session — rotation
		// happens on the server's schedule, not ours. The held secret stays
		// valid; the caller polls until the source rotates.
		return aws.Credentials{}, errNonAdvancingExpiry
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
		// Wording note: on EC2 the SDK caps reported Expires at now+1h even for
		// ~6h IMDS sessions (ec2rolecreds), so hourly re-emits of the SAME
		// material under an advancing cap are expected — hence "refreshed", not
		// "new session".
		r.logger.Info().Time("expires", creds.Expires.UTC()).
			Str("source", credSourceLabel(creds.Source)).
			Msg("DuckDB S3 secret refreshed")
	} else {
		r.logger.Info().Str("source", credSourceLabel(creds.Source)).
			Msg("DuckDB S3 secret emitted (non-expiring credentials)")
	}
	return creds, nil
}

// credSourceLabel sanitizes aws.Credentials.Source for logging: the
// SharedConfigCredentials value embeds a filesystem path
// ("SharedConfigCredentials: /home/user/.aws/credentials") which does not
// belong in logs that may ship to external collectors. Keep the provider name,
// drop everything after the first colon.
func credSourceLabel(source string) string {
	if source == "" {
		return "unknown"
	}
	if i := strings.IndexByte(source, ':'); i > 0 {
		return source[:i]
	}
	return source
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
