#!/usr/bin/env bash
# The Enterprise chart's shared storage runs Pattern 2 multi-writer by default
# and single-writer when storage.shared.multiWriter=false (#1606).
# Multi-writer sets cluster.shared_storage_mode on every role, which needs the
# shared_storage_multi_writer licence feature at any writer count. Single-writer
# leaves it unset, so one elected primary takes writes and no multi-writer
# licence is needed. An explicit false must win: a `default` in the template
# would turn it back into true, and helm 3 and 4 treat absent and null values
# differently, so every case runs under the CI-pinned helm too.
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

flag_count() { grep -c 'name: ARC_CLUSTER_SHARED_STORAGE_MODE' <<<"$1" || true; }

# Default: multi-writer on every role (writer, reader, compactor).
out=$(render)
[ "$(flag_count "$out")" -eq 3 ] || fail "default: shared_storage_mode not on all three roles"

# Explicit true at one writer: still multi-writer (the value decides, not the count).
out=$(render --set storage.shared.multiWriter=true --set writer.replicas=1)
[ "$(flag_count "$out")" -eq 3 ] || fail "multiWriter=true: shared_storage_mode not on all three roles"

# Explicit false, at three writers and at one: no flag on any role, and
# replication stays off (a shared bucket has nothing to replicate, #1605).
for replicas in 3 1; do
  out=$(render --set storage.shared.multiWriter=false --set writer.replicas="$replicas")
  [ "$(flag_count "$out")" -eq 0 ] || fail "multiWriter=false, $replicas writers: shared_storage_mode rendered"
  grep -A1 'name: ARC_CLUSTER_REPLICATION_ENABLED' <<<"$out" | grep -q 'value: "true"' && fail "multiWriter=false: replication enabled on a shared bucket"
  grep -q 'name: ARC_CLUSTER_FAILOVER_ENABLED' <<<"$out" || fail "multiWriter=false: writer failover not enabled by default"
done

# Local storage never sets the flag, whatever multiWriter says.
out=$(render --set storage.mode=local --set storage.shared.multiWriter=true)
[ "$(flag_count "$out")" -eq 0 ] || fail "local storage: shared_storage_mode rendered"

# The schema accepts only a boolean.
if render --set-string storage.shared.multiWriter=auto >/dev/null 2>&1; then
  fail "schema accepted a non-boolean multiWriter"
fi

echo "OK: shared-storage multi-writer / single-writer rendering"
