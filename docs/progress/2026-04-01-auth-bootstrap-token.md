# Auth Bootstrap Token - Implementation Plan

**Date:** 2026-04-01

## Context

Users reported that Arc's auth bootstrap experience is painful:
- Admin token is shown only once in logs on first start — if missed, the only recovery is deleting the auth DB and redeploying
- No way to pre-configure a known token value for automated/reproducible deployments

## Solution

Two new env vars:

### `ARC_AUTH_BOOTSTRAP_TOKEN`
When set, Arc uses this value as the initial admin token on first run instead of generating a random one.
- **First run (no tokens in DB)**: creates admin token with the provided value
- **Subsequent restarts (tokens already exist)**: no-op (idempotent)
- Minimum 32 characters required

### `ARC_AUTH_FORCE_BOOTSTRAP`
When set to `true` alongside `ARC_AUTH_BOOTSTRAP_TOKEN`, deletes ALL existing tokens and recreates admin with the provided value.
- Recovery path for when admin token has been lost
- Logs a prominent WARN message for audit trail
- Should be removed from deployment config after recovery

## Files Modified

- `internal/config/config.go` — added `BootstrapToken` and `ForceBootstrap` to `AuthConfig`
- `internal/auth/auth.go` — added `CreateTokenWithValue`, `EnsureInitialTokenWithValue`, `ForceResetWithToken`
- `cmd/arc/main.go` — updated bootstrap logic to use new methods based on config

## Security Considerations

- Token value set via env var, not config file (won't appear in `arc.toml`)
- Token stored as bcrypt hash — plaintext never persists
- `ARC_AUTH_FORCE_BOOTSTRAP` requires explicit opt-in, preventing accidental token wipes
- Min 32-char validation prevents weak tokens
- ForceBootstrap logs WARN level for audit trail
