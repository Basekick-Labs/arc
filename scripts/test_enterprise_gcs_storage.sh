#!/usr/bin/env bash
# The Enterprise chart deploys a shared-storage cluster on Google Cloud Storage
# (#1105): storage.shared.backend=gcs renders the GCS environment on every
# role, mounts an optional service-account key Secret, renders no bundled
# SeaweedFS and no S3 credentials, and refuses the shapes Arc cannot run.
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

fail() { echo "FAIL: $*" >&2; exit 1; }

gcs=(--set storage.shared.backend=gcs --set storage.shared.external=true
     --set storage.shared.bucket=arc-gcs --set storage.shared.prefix=p2)

# Key Secret mounted: GCS env and the mount on writer, reader and compactor.
out=$(render "${gcs[@]}" --set storage.shared.gcs.credentialsSecret=arc-gcs-key \
  --set storage.shared.gcs.endpoint=https://storage.googleapis.com)
[ "$(grep -c 'value: "gcs"' <<<"$out")" -eq 3 ] || fail "ARC_STORAGE_BACKEND=gcs not on all three roles"
[ "$(grep -c 'name: ARC_STORAGE_GCS_CREDENTIALS_FILE' <<<"$out")" -eq 3 ] || fail "credentials file env not on all three roles"
[ "$(grep -c 'mountPath: /etc/arc/gcs' <<<"$out")" -eq 3 ] || fail "key not mounted on all three roles"
[ "$(grep -c 'secretName: "arc-gcs-key"' <<<"$out")" -eq 3 ] || fail "key Secret volume not on all three roles"
grep -q 'name: ARC_STORAGE_GCS_ENDPOINT' <<<"$out" || fail "endpoint not rendered"
grep -q 'ARC_STORAGE_S3_' <<<"$out" && fail "S3 settings rendered for a GCS cluster"
grep -qi 'seaweedfs' <<<"$out" && fail "bundled SeaweedFS rendered for a GCS cluster"

# Workload Identity: a chart-created ServiceAccount carries the annotation,
# and there is no key Secret, mount or credentials file.
out=$(render "${gcs[@]}" --set serviceAccount.create=true \
  --set-string 'serviceAccount.annotations.iam\.gke\.io/gcp-service-account=arc@p.iam.gserviceaccount.com')
grep -q 'kind: ServiceAccount' <<<"$out" || fail "no ServiceAccount rendered for Workload Identity"
grep -q 'iam.gke.io/gcp-service-account: arc@p.iam.gserviceaccount.com' <<<"$out" || fail "Workload Identity annotation not on the ServiceAccount"
grep -q 'ARC_STORAGE_GCS_CREDENTIALS_FILE' <<<"$out" && fail "credentials file rendered without a key Secret"
grep -q '/etc/arc/gcs' <<<"$out" && fail "key mounted without a key Secret"

# A non-default key name is mounted under that name and named in the env.
out=$(render "${gcs[@]}" --set storage.shared.gcs.credentialsSecret=k --set storage.shared.gcs.credentialsKey=sa.json)
[ "$(grep -c 'value: "/etc/arc/gcs/sa.json"' <<<"$out")" -eq 3 ] || fail "custom key name not in the env on all roles"
grep -q 'key: "sa.json"' <<<"$out" || fail "custom key name not projected"

# Refusals.
expect_refusal() {
  local want=$1; shift
  if out=$(render "$@" 2>&1); then fail "rendered, want refusal: $want"; fi
  grep -q "$want" <<<"$out" || fail "refusal did not say \"$want\": $out"
}
expect_refusal "requires storage.shared.external=true" --set storage.shared.backend=gcs --set storage.shared.bucket=b
expect_refusal "apply to backend=s3 only" "${gcs[@]}" --set storage.shared.credentials.accessKey=k
expect_refusal "backend" --set storage.shared.backend=azure
expect_refusal "storage.shared.gcs.endpoint" "${gcs[@]}" --set storage.shared.endpoint=https://s3.example
expect_refusal "applies to storage.mode=shared only" --set storage.mode=local --set storage.shared.gcs.credentialsSecret=k

# The default (s3) is unchanged.
out=$(render)
grep -q 'value: "s3"' <<<"$out" || fail "default backend is no longer s3"
grep -q 'ARC_STORAGE_GCS_' <<<"$out" && fail "GCS env rendered for the default s3 backend"
echo "ok: enterprise chart GCS storage"
