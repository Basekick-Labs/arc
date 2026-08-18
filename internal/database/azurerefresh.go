package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/rs/zerolog"
)

// Azure side of Arc's credential management for DuckDB (#605); shared core in
// credrefresh.go, S3 sibling (and the incident history that shaped all of
// this) in s3refresh.go.
//
// Azure managed-identity credentials had the same resolve-once class as #600:
// DuckDB's azure CREDENTIAL_CHAIN secret materializes an AAD token once at
// CREATE SECRET time and never refreshes, while AAD access tokens live ~60-90
// minutes (SP) — so query reads died about an hour in, exactly like IRSA did.
// Arc now resolves tokens itself via azidentity (the same
// DefaultAzureCredential chain ingest uses — identity symmetry, like S3) and
// emits PROVIDER ACCESS_TOKEN secrets, re-issued before expiry (probed
// against the bundled engine: the provider exists, ACCESS_TOKEN is redacted
// in duckdb_secrets(), chain→access_token replacement takes effect).

// azureStorageScope is the AAD resource scope for Azure Storage in the public
// cloud — the same value the ingest path's azblob client requests. Sovereign
// AUDIENCE selection is a pre-existing, ingest-shared limitation (documented);
// sovereign AUTHORITY is covered by azidentity's AZURE_AUTHORITY_HOST env.
const azureStorageScope = "https://storage.azure.com/.default"

// azureDemotionHint is what a never-succeeded Azure refresher's demotion Warn
// tells the operator (see credentialRefresher.planError).
const azureDemotionHint = "no resolvable Azure credentials; running on DuckDB's credential chain — configure an account key/connection string, or fix the managed identity / service principal environment"

// azureTokenCredential is the seam between the Azure resolver and azidentity,
// so tests can inject deterministic credentials.
type azureTokenCredential interface {
	GetToken(ctx context.Context, options policy.TokenRequestOptions) (azcore.AccessToken, error)
}

// newAzureCredProvider builds the azidentity default chain (env service
// principal, workload identity, managed identity, az CLI). Package-level so
// tests can substitute a fake.
var newAzureCredProvider = func() (azureTokenCredential, error) {
	return azidentity.NewDefaultAzureCredential(nil)
}

// azureResolver implements credentialResolver for Azure secrets.
//
// invalidate is a documented NO-OP here: azidentity/MSAL expose no public
// cache invalidation. MSAL serves its cached token until an internal 5-minute
// hard buffer before expiry (plus proactive refresh_on where the server sends
// one), so proactive resolves inside our 10-minute margin legitimately return
// the SAME token for up to ~5 minutes — the core's non-advancing flat-poll is
// the mechanism that bridges that gap, and it was built for exactly this
// shape (#601). One observability note: MSAL swallows managed-identity
// proactive-refresh failures (serves cache), so a broken AAD surfaces as
// "degraded" only inside the final ~5 minutes (#605 review M3).
type azureResolver struct {
	db     *sql.DB
	params azureSecretParams // template; accessToken filled per emission
	cred   azureTokenCredential
}

func (a *azureResolver) Resolve(ctx context.Context, invalidate bool) (credMaterial, error) {
	_ = invalidate // no azidentity equivalent; see type comment
	tok, err := a.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{azureStorageScope}})
	if err != nil {
		return credMaterial{}, fmt.Errorf("resolve Azure credentials: %w", err)
	}
	return credMaterial{
		canExpire:    true, // AAD access tokens always expire
		expires:      tok.ExpiresOn,
		source:       "DefaultAzureCredential",
		secretValues: []string{tok.Token},
		payload:      tok,
	}, nil
}

func (a *azureResolver) Emit(ctx context.Context, m credMaterial) error {
	tok := m.payload.(azcore.AccessToken)
	p := a.params
	p.accessToken = tok.Token
	secretSQL, err := buildAzureSecretSQL(p)
	if err != nil {
		return err
	}
	_, err = a.db.ExecContext(ctx, secretSQL)
	return err
}

// startAzureCredentialRefresher wires an Azure resolver into the shared core.
func startAzureCredentialRefresher(db *sql.DB, params azureSecretParams, cred azureTokenCredential, logger zerolog.Logger, onFirstFailure func(error)) *credentialRefresher {
	return startCredentialRefresher(
		&azureResolver{db: db, params: params, cred: cred},
		params.name, logger, azureDemotionHint, onFirstFailure)
}
