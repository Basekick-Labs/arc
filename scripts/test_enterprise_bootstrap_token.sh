#!/usr/bin/env bash
# Arc refuses a bootstrap token under 32 characters, logs one error and starts
# with no admin token, so the Enterprise chart must refuse to render one.
set -euo pipefail

chart="${1:-helm/arc-enterprise}"

render() {
  helm template arc "$chart" \
    --set license.key=not-a-real-key \
    --set cluster.sharedSecret.value=not-a-real-secret \
    --set seaweedfs.credentials.accessKey=not-a-real-user \
    --set seaweedfs.credentials.secretKey=not-a-real-password \
    "$@"
}

if out=$(render --set-string auth.bootstrapToken.value=0123456789012345678901234567890 2>&1); then
  echo 'chart rendered a 31-character bootstrap token' >&2
  exit 1
fi
if ! grep -q 'must be at least 32 characters' <<<"$out"; then
  echo "unexpected render error for a short bootstrap token: $out" >&2
  exit 1
fi

# Exactly 32 is accepted, and the value is not checked when an existing
# Secret supplies the token.
render --set-string auth.bootstrapToken.value=01234567890123456789012345678901 > /dev/null
render --set auth.bootstrapToken.existingSecret=precreated-admin-token \
  --set-string auth.bootstrapToken.value=short > /dev/null
