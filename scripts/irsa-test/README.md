# IRSA credential-refresh test rig

Reproduces the EKS/IRSA credential path **without EKS**, to verify the fix for
[#600](https://github.com/Basekick-Labs/arc/issues/600) — where S3 queries failed
with `ExpiredToken` about an hour after process start.

## Why not a simulator

LocalStack-based IRSA simulations (and similar) do not verify the token signature
against a real OIDC issuer and offer no way to observe credential expiry. Expiry
*is* the bug, so they cannot test it. This rig uses **real AWS STS**.

## What it builds

| Resource | Purpose |
|---|---|
| Public S3 bucket | Hosts the OIDC discovery doc + JWKS (public keys only) |
| IAM OIDC provider | Makes STS trust that issuer |
| IAM role | What Arc assumes via `AssumeRoleWithWebIdentity` |
| Private S3 bucket | Parquet data, in Arc's `{db}/{measurement}/{y}/{m}/{d}/{h}/` layout |

A locally generated RSA key signs tokens shaped like kubelet-projected service
account tokens. STS validates them against the published JWKS — the same exchange
EKS performs.

## Usage

```bash
./setup.sh        # creates everything, writes env.sh
./mktoken.sh      # mints a signed token
./teardown.sh     # removes everything (safe to re-run)
```

Then run Arc with **no static keys**:

```bash
source env.sh
docker run -d --name arc-irsa -p 8100:8000 \
  -v "$HOME/irsa-secrets:/var/run/secrets:ro" \
  -e AWS_ROLE_ARN="$ROLE_ARN" \
  -e AWS_WEB_IDENTITY_TOKEN_FILE=/var/run/secrets/token \
  -e AWS_REGION="$REGION" \
  -e ARC_STORAGE_BACKEND=s3 -e ARC_STORAGE_S3_BUCKET="$DATA_BUCKET" \
  -e ARC_STORAGE_S3_REGION="$REGION" \
  -e ARC_STORAGE_S3_USE_SSL=true -e ARC_STORAGE_S3_PATH_STYLE=false \
  arc:irsa-test
```

Confirm `credential_mode=web_identity` in the logs, then query past the one-hour
mark. A globbed read (`SELECT count(*) FROM cpu`) is the case that matters — see
the caveat in duckdb-aws#136 that auto-refresh may only apply to un-globbed paths.

## Gotchas

- **`MaxSessionDuration` minimum is 3600s.** AWS rejects 900. Expiry testing means
  waiting an hour; there is no shortcut.
- **The stock `arc.toml` ships MinIO defaults** (`s3_use_ssl=false`,
  `s3_path_style=true`). Against real AWS that yields `403 AccessDenied`. Override
  both, as above.
- **Rancher Desktop does not bind-mount `/tmp`.** Put the token under `$HOME`, or
  Docker silently creates a *directory* at the mount path.
