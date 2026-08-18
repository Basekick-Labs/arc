package database

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

// This file owns the storage-credential refresher CORE, shared by every
// backend (S3: s3refresh.go, Azure: azurerefresh.go). It grew out of #600/#601
// (S3/IRSA ExpiredToken — see s3refresh.go for that incident history) and was
// generalized for #605 (Azure managed identity, same resolve-once class).
//
// Shape: a credentialResolver produces credential material and emits the
// DuckDB secret; the core owns everything behavior-critical — scheduling, the
// non-advancing detection (server-rotated sources return the same material
// near session end; the core skips Emit and flat-polls), the control-byte
// guard, retry/demotion severity, and the /health status snapshot (#603).
// Keeping those in the core means a new backend cannot forget them.

const (
	// s3RefreshMargin is how long before credential expiry the refresher
	// re-resolves. It must comfortably exceed the default query timeout (300s)
	// so credentials stay valid for the lifetime of queries started just before
	// a rotation. Queries longer than the margin may still see one retryable
	// failure at rotation. (Named s3* for history; shared by all backends. For
	// Azure, MSAL's internal 5-minute hard cache buffer sits inside this margin:
	// polls between margin and buffer return the cached token — the
	// non-advancing path — and a real acquisition happens inside the buffer.)
	s3RefreshMargin = 10 * time.Minute
	// s3RefreshMinDelay floors the delay between refreshes so pathologically
	// short sessions cannot spin the loop.
	s3RefreshMinDelay = time.Minute
	// s3FirstResolveTimeout bounds the synchronous resolve during startup /
	// ConfigureS3. An unreachable STS/AAD must degrade to the background retry
	// loop, not stall startup — database.New failures are fatal in main.go.
	s3FirstResolveTimeout = 10 * time.Second
	// s3ResolveTimeout bounds each background resolve+emit.
	s3ResolveTimeout = 30 * time.Second

	s3RefreshBackoffMin = 30 * time.Second
	s3RefreshBackoffMax = 5 * time.Minute
	// s3RefreshBackoffMaxUnproven caps retries for a refresher that has NEVER
	// succeeded and is running behind the fallback CREDENTIAL_CHAIN secret
	// (e.g. anonymous MinIO with no resolvable credentials). Such a deployment
	// is healthy; polling it every 5 minutes forever at Error level would
	// produce ~288 spurious error lines/day. See planError.
	s3RefreshBackoffMaxUnproven = 15 * time.Minute
	// s3RefreshErrorsBeforeDemotion: a never-succeeded refresher logs its first
	// failures at Error (a genuinely broken IMDS/STS/AAD at boot must be
	// visible), then demotes to Debug with one Warn marking the transition.
	s3RefreshErrorsBeforeDemotion = 3
	// s3RefreshFinalWarnWindow: a non-advancing resolve (the upstream source
	// has not rotated yet — normal for IMDS, Pod Identity and AAD, which
	// rotate server-side on their own schedule) logs Debug while there is
	// plenty of runway, escalating to Warn inside this window before the held
	// credentials' expiry so an operator has reaction time.
	s3RefreshFinalWarnWindow = 5 * time.Minute
)

// errNonAdvancingExpiry marks a resolve that succeeded but returned the same
// session we already hold — the upstream source has not rotated yet. Not a
// failure: the held credentials remain valid; the loop polls at a flat
// s3RefreshMinDelay until the source rotates. Core-internal: resolvers never
// see or produce it.
var errNonAdvancingExpiry = errors.New("credential provider returned a non-advancing expiry (upstream not rotated yet)")

// credMaterial is what a resolver hands the core per resolve.
type credMaterial struct {
	canExpire bool
	expires   time.Time
	source    string // sanitized provider label, for logs and /health
	// secretValues is every credential string that will be interpolated into
	// the CREATE SECRET statement; the CORE runs the control-byte guard over
	// them so no backend can forget it (a NUL truncates DuckDB's parser string
	// and its error echoes the credential prefix into logs).
	secretValues []string
	// payload carries backend-private material from Resolve to Emit.
	payload any
}

// credentialResolver is the backend seam (#605 review M1): Resolve produces
// material, Emit writes the DuckDB secret. The core decides IF Emit runs —
// on a non-advancing resolve it does not (no pointless re-emission, no
// misleading "refreshed" log). invalidate requests a real re-resolution:
// the AWS SDK cache needs it (aws.CredentialsCache.Invalidate); azidentity
// has no equivalent and its resolver documents it as a no-op.
type credentialResolver interface {
	Resolve(ctx context.Context, invalidate bool) (credMaterial, error)
	Emit(ctx context.Context, m credMaterial) error
}

// credentialRefresher keeps one DuckDB storage secret stocked with fresh
// credentials. One instance per managed secret; owned by the DuckDB struct,
// which stops it on Close.
type credentialRefresher struct {
	resolver credentialResolver
	logger   zerolog.Logger
	cancel   context.CancelFunc
	done     chan struct{}

	// demotionHint names the backend-appropriate way to silence a
	// never-succeeded retry loop (see planError).
	demotionHint string

	// lastExpiry drives the non-advancing check: every successful refresh must
	// advance the expiry, else the upstream source has not rotated yet.
	// Core-owned: resolvers never track expiry state.
	lastExpiry time.Time

	// Scheduling/severity state, owned by the loop goroutine (and the sync
	// first resolve before the goroutine starts).
	everSucceeded     bool
	consecutiveErrors int

	// lastRefresh / source describe the last successful emission; loop-owned
	// like the fields above.
	lastRefresh time.Time
	source      string

	// snap is the read side for /health (#603): an immutable snapshot swapped
	// atomically at every outcome. status() reads ONLY this — never the
	// loop-owned raw fields above. Publication of the refresher in the DuckDB
	// registry (under refresherMu) is what orders the constructor's writes
	// before any reader; the done channel plays no part in that.
	snap atomic.Pointer[storageCredSnapshot]
}

// storageCredSnapshot is the raw material for state derivation. It stores
// facts, not a state string: state is derived at READ time by deriveState so
// an expiry crossing between refresher outcomes (up to ~refreshDelay apart)
// becomes visible within one probe interval, not one refresh interval.
type storageCredSnapshot struct {
	everSucceeded     bool
	canExpire         bool
	consecutiveErrors int
	lastExpiry        time.Time
	lastRefresh       time.Time
	source            string
}

// publishStatus snapshots the loop-owned fields. Called at every outcome —
// the three constructor branches, the three plan* outcomes, and the loop's
// non-expiring exit. Missing a site is not cosmetic: skipping the
// constructor-success site would report "fallback" for ~refreshDelay on a
// perfectly healthy refresher (#603 review F1).
func (r *credentialRefresher) publishStatus(canExpire bool) {
	r.snap.Store(&storageCredSnapshot{
		everSucceeded:     r.everSucceeded,
		canExpire:         canExpire,
		consecutiveErrors: r.consecutiveErrors,
		lastExpiry:        r.lastExpiry,
		lastRefresh:       r.lastRefresh,
		source:            r.source,
	})
}

// Credential states surfaced via /health (#603). Exported: they are a
// published API contract (release notes) and internal/api compares against
// CredStateExpired for the readiness knob — a raw string there would let a
// respelling here silently disable the knob (#603 review M3).
const (
	CredStateOK       = "ok"
	CredStateDegraded = "degraded"
	CredStateExpired  = "expired"
	CredStateFallback = "fallback"
	CredStateUnknown  = "unknown"
)

// deriveState maps a snapshot to a state at time `now`. Pure so the full grid
// — including "expired", impossible to force against real STS/AAD in a test —
// is unit-testable.
//
// Precedence (#603 review F4): fallback (never succeeded — cannot be expired,
// lastExpiry is only ever set by a successful emit) > non-expiring ok >
// expired (EVEN with a concurrent error streak: in the field incident both
// held at once and expired is the actionable truth) > degraded > ok.
// Non-advancing rotation-waits stay "ok" deliberately: they occur in the tail
// of every healthy IMDS/Pod-Identity/AAD session; pre-expiry alerting belongs
// to the payload's expires_at, not a state flap. Note: for Azure managed
// identity, MSAL swallows proactive-refresh failures and serves the cached
// token, so "degraded" can appear only inside the final ~5 minutes there
// (vs ~10 for S3) — #605 review M3.
func deriveState(snap *storageCredSnapshot, now time.Time) string {
	switch {
	case snap == nil || !snap.everSucceeded:
		return CredStateFallback
	case !snap.canExpire:
		return CredStateOK
	case !now.Before(snap.lastExpiry):
		return CredStateExpired
	case snap.consecutiveErrors > 0:
		return CredStateDegraded
	default:
		return CredStateOK
	}
}

// status returns this refresher's contribution to the /health payload.
func (r *credentialRefresher) status(now time.Time) StorageTierStatus {
	snap := r.snap.Load()
	st := StorageTierStatus{State: deriveState(snap, now)}
	if snap == nil {
		return st
	}
	st.Source = snap.source
	st.ConsecutiveErrors = snap.consecutiveErrors
	if snap.canExpire && !snap.lastExpiry.IsZero() {
		t := snap.lastExpiry.UTC()
		st.ExpiresAt = &t
	}
	if !snap.lastRefresh.IsZero() {
		t := snap.lastRefresh.UTC()
		st.LastRefresh = &t
	}
	return st
}

// startCredentialRefresher performs one synchronous resolve+emit (bounded by
// s3FirstResolveTimeout) and then maintains the secret in the background. It
// never fails: an unreachable STS/AAD or an unprojected token degrades to the
// caller's onFirstFailure hook and background retries — a transient credential
// race must not turn into a startup failure.
//
// onFirstFailure, when non-nil, runs synchronously after a failed first
// resolve and BEFORE the retry loop starts — so a caller-emitted fallback
// secret is structurally guaranteed to land before the loop's first managed
// emit can replace it (#601 review M1).
func startCredentialRefresher(resolver credentialResolver, secretName string, logger zerolog.Logger, demotionHint string, onFirstFailure func(error)) *credentialRefresher {
	ctx, cancel := context.WithCancel(context.Background())
	r := &credentialRefresher{
		resolver: resolver,
		logger: logger.With().
			Str("component", "storage-cred-refresher").
			Str("secret", secretName).Logger(),
		cancel:       cancel,
		done:         make(chan struct{}),
		demotionHint: demotionHint,
	}

	fctx, fcancel := context.WithTimeout(ctx, s3FirstResolveTimeout)
	m, err := r.refreshOnce(fctx, false)
	fcancel()

	switch {
	case err != nil:
		r.publishStatus(true)
		if onFirstFailure != nil {
			onFirstFailure(err)
		}
		go r.loop(ctx, s3RefreshBackoffMin)
	case !m.canExpire:
		// Static credentials resolved through the chain — nothing to refresh.
		r.everSucceeded = true
		r.publishStatus(false)
		r.logger.Info().Msg("resolved non-expiring credentials; refresh loop not needed")
		close(r.done)
	default:
		r.everSucceeded = true
		r.publishStatus(true)
		go r.loop(ctx, refreshDelay(m.expires))
	}
	return r
}

// stop cancels the refresher and waits for the background goroutine to finish,
// so no Exec can race the pool's Close.
func (r *credentialRefresher) stop() {
	r.cancel()
	<-r.done
}

func (r *credentialRefresher) loop(ctx context.Context, initialDelay time.Duration) {
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
		m, err := r.refreshOnce(rctx, true)
		rcancel()

		if ctx.Err() != nil {
			// Shutdown, not a failure.
			r.logger.Debug().Msg("credential refresh stopped")
			return
		}

		var delay time.Duration
		switch {
		case err == nil && !m.canExpire:
			// Publish before exiting or the stale ExpiresAt would eventually
			// derive "expired" forever while queries work (#603 review F1).
			r.everSucceeded = true
			r.consecutiveErrors = 0
			r.publishStatus(false)
			r.logger.Info().Msg("credentials became non-expiring; refresh loop exiting")
			return
		case err == nil:
			delay = r.planSuccess(m)
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

func (r *credentialRefresher) planSuccess(m credMaterial) time.Duration {
	r.everSucceeded = true
	r.consecutiveErrors = 0
	r.publishStatus(true)
	return refreshDelay(m.expires)
}

// planNonAdvancing: the upstream source has not rotated yet. The held secret
// stays valid; poll at the flat minimum until it rotates. Debug while there is
// runway, Warn inside the final window. NOTE: this outcome is expected in the
// tail of every IMDS / Pod Identity / AAD session — do not "fix" it back into
// an error, and do not add backoff (a backoff step could straddle the
// rotation).
func (r *credentialRefresher) planNonAdvancing() time.Duration {
	r.consecutiveErrors = 0
	r.publishStatus(true)
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
// no resolvable credentials, so after s3RefreshErrorsBeforeDemotion failures
// it demotes to Debug with a longer cap — one Warn (carrying demotionHint)
// marks the demotion so the state is diagnosable from logs. It never gives
// up: a host whose metadata service was down at boot is picked up by a later
// retry.
func (r *credentialRefresher) planError(err error) time.Duration {
	r.consecutiveErrors++
	r.publishStatus(true)
	n := r.consecutiveErrors
	backoffCap := s3RefreshBackoffMax
	ev := r.logger.Error()
	if !r.everSucceeded {
		backoffCap = s3RefreshBackoffMaxUnproven
		switch {
		case n == s3RefreshErrorsBeforeDemotion+1:
			ev = r.logger.Warn()
			ev = ev.Str("hint", r.demotionHint)
		case n > s3RefreshErrorsBeforeDemotion+1:
			ev = r.logger.Debug()
		}
	}
	delay := s3RefreshBackoffMin << (n - 1)
	if delay > backoffCap || delay <= 0 {
		delay = backoffCap
	}
	ev.Err(err).Dur("retry_in", delay).
		Msg("storage credential refresh failed; existing secret remains in place")
	return delay
}

// refreshOnce resolves credential material and — unless the source has not
// rotated yet — emits the secret. Credential values are never logged.
func (r *credentialRefresher) refreshOnce(ctx context.Context, invalidate bool) (credMaterial, error) {
	m, err := r.resolver.Resolve(ctx, invalidate)
	if err != nil {
		return credMaterial{}, err
	}
	// Refuse credential material containing control bytes BEFORE it reaches
	// SQL construction. escapeSQLString handles quotes, but a NUL truncates
	// the C string inside DuckDB's parser, whose "unterminated quoted string"
	// error echoes the credential prefix — which our callers log. Real
	// AWS/AAD credentials are ASCII and never trip this; the guard exists so
	// the no-credentials-in-logs guarantee doesn't rest on that assumption.
	// Core-owned so no backend can forget it. Fixed error string on purpose.
	for _, v := range m.secretValues {
		if strings.ContainsFunc(v, func(c rune) bool { return c < 0x20 || c == 0x7f }) {
			return credMaterial{}, fmt.Errorf("resolved credential material contains control bytes; refusing to emit secret")
		}
	}
	if m.canExpire && !r.lastExpiry.IsZero() && !m.expires.After(r.lastExpiry) {
		// Same session as we already hold. For server-rotated sources this is
		// NORMAL near the end of a session — rotation happens on the server's
		// schedule, not ours. Emit is SKIPPED: re-emitting identical material
		// would only produce a misleading "refreshed" log line.
		return credMaterial{}, errNonAdvancingExpiry
	}
	if err := r.resolver.Emit(ctx, m); err != nil {
		return credMaterial{}, fmt.Errorf("emit storage secret: %w", err)
	}
	r.source = m.source
	r.lastRefresh = time.Now().Truncate(time.Second)
	if m.canExpire {
		r.lastExpiry = m.expires
		// Wording note: on EC2 the SDK caps reported Expires at now+1h even
		// for ~6h IMDS sessions (ec2rolecreds), so hourly re-emits of the SAME
		// material under an advancing cap are expected — hence "refreshed",
		// not "new session".
		r.logger.Info().Time("expires", m.expires.UTC()).
			Str("source", m.source).
			Msg("DuckDB storage secret refreshed")
	} else {
		r.logger.Info().Str("source", m.source).
			Msg("DuckDB storage secret emitted (non-expiring credentials)")
	}
	return m, nil
}

// refreshDelay schedules the next refresh s3RefreshMargin before `expires`,
// floored at s3RefreshMinDelay. `expires` is whatever the provider REPORTS —
// for AWS that is the real session expiry (no ExpiryWindow configured; the
// proactive resolve Invalidates the cache, which is what makes firing before
// expiry actually produce a new session); for Azure it is the AAD token
// expiry (MSAL serves cache until its 5-min internal buffer; the
// non-advancing flat-poll bridges the gap).
func refreshDelay(expires time.Time) time.Duration {
	d := time.Until(expires) - s3RefreshMargin
	if d < s3RefreshMinDelay {
		d = s3RefreshMinDelay
	}
	return d
}
