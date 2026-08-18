#!/usr/bin/env bash
# Removes everything setup.sh created. Safe to re-run.
set -uo pipefail
source /tmp/irsa-test/env.sh
aws iam delete-role-policy --role-name "$ROLE" --policy-name s3access 2>/dev/null
aws iam delete-role --role-name "$ROLE" 2>/dev/null
aws iam delete-open-id-connect-provider --open-id-connect-provider-arn "$OIDC_ARN" 2>/dev/null
aws s3 rm "s3://${OIDC_BUCKET}" --recursive 2>/dev/null
aws s3api delete-bucket --bucket "$OIDC_BUCKET" 2>/dev/null
aws s3 rm "s3://${DATA_BUCKET}" --recursive 2>/dev/null
aws s3api delete-bucket --bucket "$DATA_BUCKET" 2>/dev/null
echo "--- torn down ---"
aws iam get-role --role-name "$ROLE" 2>&1 | grep -q "NoSuchEntity" && echo "role: gone"
aws s3api head-bucket --bucket "$OIDC_BUCKET" 2>&1 | grep -q "404\|Not Found" && echo "oidc bucket: gone"
aws s3api head-bucket --bucket "$DATA_BUCKET" 2>&1 | grep -q "404\|Not Found" && echo "data bucket: gone"
