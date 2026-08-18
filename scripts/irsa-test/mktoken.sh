#!/usr/bin/env bash
# Mints a signed OIDC token like a kubelet-projected EKS service-account token.
# Long-lived (24h) on purpose: the kubelet rotates these, and the thing we are
# testing is the STS SESSION expiring (1h), not the token expiring.
set -euo pipefail
source /tmp/irsa-test/env.sh
b64() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
NOW=$(date +%s); EXP=$((NOW + 86400))
HDR=$(printf '{"alg":"RS256","kid":"arc-test-key","typ":"JWT"}' | b64)
PAY=$(printf '{"iss":"%s","sub":"%s","aud":["sts.amazonaws.com"],"exp":%d,"iat":%d,"nbf":%d}' \
      "$ISSUER" "$SUB" "$EXP" "$NOW" "$NOW" | b64)
SIG=$(printf '%s.%s' "$HDR" "$PAY" | openssl dgst -sha256 -sign /tmp/irsa-test/key.pem | b64)
printf '%s.%s.%s' "$HDR" "$PAY" "$SIG" > /tmp/irsa-test/token
echo "token written ($(wc -c < /tmp/irsa-test/token) bytes)"
