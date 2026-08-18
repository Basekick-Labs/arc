package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/rs/zerolog"
)

// This file owns the S3 side of Arc's credential management for DuckDB
// (#600/#601); the shared refresher core lives in credrefresh.go.
//
// Incident history, kept here on purpose:
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
//
// The first merge-gate run also taught the ExpiryWindow lesson now encoded in
// newAWSCredProvider and the core's non-advancing machinery: a proactive
// Retrieve before the cache considers credentials expired returns the SAME
// session; only Invalidate() makes an early refresh real.

// s3DemotionHint is what a never-succeeded S3 refresher's demotion Warn tells
// the operator (see credentialRefresher.planError).
const s3DemotionHint = "no resolvable AWS credentials; running on DuckDB's credential chain — configure keys, or set AWS_EC2_METADATA_DISABLED=true to fast-fail the probe"

// awsCredentialsProvider is the seam between the S3 resolver and the AWS SDK,
// so tests can inject deterministic providers.
type awsCredentialsProvider interface {
	Retrieve(ctx context.Context) (aws.Credentials, error)
}

// newAWSCredProvider builds the SDK default credential chain (web identity /
// IRSA, env, shared config, IMDS, container credentials / Pod Identity) for
// the refresher. Package-level so tests can substitute a fake.
//
// The returned provider is an aws.CredentialsCache. A PROACTIVE refresh
// (before expiry) must call Invalidate() first: the cache serves cached
// credentials until it considers them expired, so a plain Retrieve at
// Expires−margin returns the SAME session and the refresh accomplishes
// nothing. (An ExpiryWindow does NOT fix this — the SDK applies the window
// once at store time, shifting the reported/stored Expires earlier, and still
// serves the cache until that shifted instant. Verified live 2026-08-18: with
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

// s3Resolver implements credentialResolver for S3 secrets: Resolve pulls from
// the SDK chain (Invalidating its cache on proactive refreshes), Emit writes
// the static KEY_ID/SECRET/SESSION_TOKEN secret.
type s3Resolver struct {
	db       *sql.DB
	params   s3SecretParams // template; credentials filled per emission
	provider awsCredentialsProvider
}

// Resolve retrieves credentials from the chain. invalidate must be true for
// every PROACTIVE resolve (the scheduled refresh and its retries): it drops
// the provider's cached session so Retrieve performs a real STS exchange
// instead of returning the credentials we already hold. The initial fill
// passes false — there is nothing cached yet, and on a provider without
// Invalidate (tests) the assertion is simply skipped.
func (s *s3Resolver) Resolve(ctx context.Context, invalidate bool) (credMaterial, error) {
	if invalidate {
		if inv, ok := s.provider.(interface{ Invalidate() }); ok {
			inv.Invalidate()
		}
	}
	creds, err := s.provider.Retrieve(ctx)
	if err != nil {
		return credMaterial{}, fmt.Errorf("resolve AWS credentials: %w", err)
	}
	return credMaterial{
		canExpire:    creds.CanExpire,
		expires:      creds.Expires,
		source:       credSourceLabel(creds.Source),
		secretValues: []string{creds.AccessKeyID, creds.SecretAccessKey, creds.SessionToken},
		payload:      creds,
	}, nil
}

// Emit writes the S3 secret with the resolved credentials.
func (s *s3Resolver) Emit(ctx context.Context, m credMaterial) error {
	creds := m.payload.(aws.Credentials)
	p := s.params
	p.accessKey = creds.AccessKeyID
	p.secretKey = creds.SecretAccessKey
	p.sessionToken = creds.SessionToken
	secretSQL, err := buildS3SecretSQL(p)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, secretSQL)
	return err
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

// startS3CredentialRefresher wires an S3 resolver into the shared core.
func startS3CredentialRefresher(db *sql.DB, params s3SecretParams, provider awsCredentialsProvider, logger zerolog.Logger, onFirstFailure func(error)) *credentialRefresher {
	return startCredentialRefresher(
		&s3Resolver{db: db, params: params, provider: provider},
		params.name, logger, s3DemotionHint, onFirstFailure)
}
