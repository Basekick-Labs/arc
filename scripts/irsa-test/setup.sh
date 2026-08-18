#!/usr/bin/env bash
# Creates a self-hosted OIDC issuer + IAM role so AssumeRoleWithWebIdentity
# (the mechanism behind EKS IRSA) can be exercised WITHOUT EKS.
# Teardown: ./teardown.sh
set -euo pipefail

REGION=us-east-1
ACCT=$(aws sts get-caller-identity --query Account --output text)
SUFFIX=$(openssl rand -hex 4)
OIDC_BUCKET="arc-irsa-oidc-${SUFFIX}"     # PUBLIC: hosts discovery doc + JWKS only
DATA_BUCKET="arc-irsa-data-${SUFFIX}"     # PRIVATE: parquet data Arc queries
ROLE="arc-irsa-test-${SUFFIX}"
ISSUER="https://${OIDC_BUCKET}.s3.${REGION}.amazonaws.com"
SUB="system:serviceaccount:arc:arc"       # mimics an EKS SA subject
echo "ACCT=$ACCT REGION=$REGION OIDC_BUCKET=$OIDC_BUCKET DATA_BUCKET=$DATA_BUCKET ROLE=$ROLE" > env.sh
echo "ISSUER=$ISSUER SUB=$SUB SUFFIX=$SUFFIX" >> env.sh

# ---- 1. RSA keypair; the private key signs our tokens, the public half goes in the JWKS
openssl genrsa -out key.pem 2048 2>/dev/null
openssl rsa -in key.pem -pubout -out pub.pem 2>/dev/null
KID="arc-test-key"

# ---- 2. JWKS from the public key (n = modulus, e = exponent, both base64url)
MOD=$(openssl rsa -in key.pem -noout -modulus | sed 's/Modulus=//' | \
      xxd -r -p | openssl base64 -A | tr '+/' '-_' | tr -d '=')
cat > jwks.json <<EOF
{"keys":[{"kty":"RSA","alg":"RS256","use":"sig","kid":"${KID}","n":"${MOD}","e":"AQAB"}]}
EOF
cat > openid-configuration <<EOF
{"issuer":"${ISSUER}","jwks_uri":"${ISSUER}/.well-known/jwks.json",
"authorization_endpoint":"urn:kubernetes:programmatic_authorization",
"response_types_supported":["id_token"],"subject_types_supported":["public"],
"id_token_signing_alg_values_supported":["RS256"],"claims_supported":["sub","iss"]}
EOF

# ---- 3. Public bucket for OIDC discovery (public keys only — no secrets)
aws s3api create-bucket --bucket "$OIDC_BUCKET" --region "$REGION" >/dev/null
aws s3api put-public-access-block --bucket "$OIDC_BUCKET" \
  --public-access-block-configuration \
  "BlockPublicAcls=false,IgnorePublicAcls=false,BlockPublicPolicy=false,RestrictPublicBuckets=false" >/dev/null
aws s3api put-bucket-policy --bucket "$OIDC_BUCKET" --policy "{
  \"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",
  \"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::${OIDC_BUCKET}/*\"}]}" >/dev/null
aws s3 cp openid-configuration "s3://${OIDC_BUCKET}/.well-known/openid-configuration" \
  --content-type application/json >/dev/null
aws s3 cp jwks.json "s3://${OIDC_BUCKET}/.well-known/jwks.json" \
  --content-type application/json >/dev/null

# ---- 4. Register the issuer with IAM; thumbprint is of the S3 endpoint TLS cert
THUMB=$(echo | openssl s_client -servername "${OIDC_BUCKET}.s3.${REGION}.amazonaws.com" \
  -connect "${OIDC_BUCKET}.s3.${REGION}.amazonaws.com":443 2>/dev/null | \
  openssl x509 -fingerprint -sha1 -noout | cut -d= -f2 | tr -d ':')
aws iam create-open-id-connect-provider --url "$ISSUER" \
  --client-id-list sts.amazonaws.com --thumbprint-list "$THUMB" >/dev/null
OIDC_ARN="arn:aws:iam::${ACCT}:oidc-provider/${OIDC_BUCKET}.s3.${REGION}.amazonaws.com"
echo "OIDC_ARN=$OIDC_ARN" >> env.sh

# ---- 5. Role Arc assumes. MaxSessionDuration=900 (15 min) is the SHORTEST AWS
#         allows, so credential expiry — the whole point — happens fast.
aws iam create-role --role-name "$ROLE" --max-session-duration 3600 \
  --assume-role-policy-document "{
    \"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",
    \"Principal\":{\"Federated\":\"${OIDC_ARN}\"},
    \"Action\":\"sts:AssumeRoleWithWebIdentity\",
    \"Condition\":{\"StringEquals\":{\"${OIDC_BUCKET}.s3.${REGION}.amazonaws.com:sub\":\"${SUB}\",
    \"${OIDC_BUCKET}.s3.${REGION}.amazonaws.com:aud\":\"sts.amazonaws.com\"}}}]}" >/dev/null

# ---- 6. Private data bucket + read/write for the role
aws s3api create-bucket --bucket "$DATA_BUCKET" --region "$REGION" >/dev/null
aws iam put-role-policy --role-name "$ROLE" --policy-name s3access \
  --policy-document "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",
  \"Action\":[\"s3:GetObject\",\"s3:PutObject\",\"s3:ListBucket\",\"s3:DeleteObject\"],
  \"Resource\":[\"arn:aws:s3:::${DATA_BUCKET}\",\"arn:aws:s3:::${DATA_BUCKET}/*\"]}]}" >/dev/null

echo "ROLE_ARN=arn:aws:iam::${ACCT}:role/${ROLE}" >> env.sh
echo "--- created ---"; cat env.sh
