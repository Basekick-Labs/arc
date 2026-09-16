#!/usr/bin/env bash
# Every pod in an Arc Enterprise cluster joins Raft, whatever its role.
#
# A node with no advertisable Raft address fails in Coordinator.Start(): the
# transport refuses an address it cannot advertise ("local bind address is not
# advertisable"), and main.go logs "Failed to start cluster coordinator -
# running in standalone mode" and carries on. The pod then looks healthy while
# being invisible to the cluster: not a voter, not in any registry.
#
# That has now happened twice. Readers shipped without a Raft advertise address
# and were fixed; the compactor was never given the same treatment and shipped
# broken until #870. This asserts it for every role so a third one cannot.
#
# Usage: test_cluster_raft_env.sh [chart] [expected roles, comma-separated] [extra helm args...]
set -euo pipefail

chart="${1:-helm/arc-enterprise}"
expected="${2:-writer,reader,compactor}"
shift 2 2>/dev/null || true

manifest=$(mktemp)
trap 'rm -f "$manifest"' EXIT

helm template arc "$chart" \
  --set license.key=not-a-real-key \
  --set cluster.sharedSecret.value=not-a-real-secret \
  --set minio.credentials.rootUser=not-a-real-user \
  --set minio.credentials.rootPassword=not-a-real-password \
  "$@" \
  > "$manifest"

python3 - "$manifest" "$expected" <<'PY'
import re, sys

manifest, expected = sys.argv[1], sys.argv[2]
want_roles = {r for r in expected.split(",") if r}
docs = open(manifest).read().split("\n---\n")

# An env var counts only when a `value:` (or valueFrom:) follows the name. The
# name appearing in a comment must not satisfy the check — the compactor
# template carries a rendered comment about this very variable, which is
# exactly where someone would leave the name behind while deleting the setting.
def env_value(doc, name):
    m = re.search(r"-\s*name:\s*%s\s*\n\s*(value|valueFrom):" % re.escape(name), doc)
    return m is not None

# Take the role from the env entry's value rather than a quoted-literal regex:
# `value: compactor` is valid YAML and valid Kubernetes, and a pattern that
# required the quotes would silently drop the role and check nothing.
def env_literal(doc, name):
    m = re.search(r"-\s*name:\s*%s\s*\n\s*value:\s*(.+)" % re.escape(name), doc)
    if not m:
        return None
    return m.group(1).strip().strip('"').strip("'")

failures = []
seen = {}

for doc in docs:
    if "kind: StatefulSet" not in doc:
        continue
    if not env_value(doc, "ARC_CLUSTER_ENABLED"):
        continue  # not a clustered Arc pod (e.g. the bundled MinIO)
    role = env_literal(doc, "ARC_CLUSTER_ROLE")
    if role is None:
        failures.append("a clustered StatefulSet renders with no ARC_CLUSTER_ROLE, so its role cannot be checked")
        continue
    seen[role] = env_value(doc, "ARC_CLUSTER_RAFT_ADVERTISE_ADDR")

# Assert the expected set rather than reporting whichever roles turned up: a
# role that silently fails to parse must fail the check, not shrink it.
if set(seen) != want_roles:
    failures.append("expected roles %s, rendered %s" % (sorted(want_roles), sorted(seen)))

for role in sorted(r for r, ok in seen.items() if not ok):
    failures.append("role %r has no ARC_CLUSTER_RAFT_ADVERTISE_ADDR: its pod cannot "
                    "join Raft and will silently run standalone" % role)

# The Raft port belongs in each role's headless Service too, so its SRV records
# describe the port the pods actually speak on. Per-pod A records do not depend
# on it, so this is a consistency assertion rather than a connectivity one.
for doc in docs:
    if "kind: Service" not in doc or "clusterIP: None" not in doc:
        continue
    name = re.search(r"\n\s*name:\s*(\S+)", doc)
    if not name or "-headless" not in name.group(1):
        continue
    role = next((r for r in want_roles if "-%s-headless" % r in name.group(1)), None)
    if role is None:
        continue
    if not re.search(r"-\s*name:\s*raft\s*\n\s*port:\s*9200", doc):
        failures.append("headless Service for %s does not publish the Raft port 9200" % role)

if failures:
    for f in failures:
        print(f, file=sys.stderr)
    sys.exit(1)

print("OK: roles %s each advertise a Raft address and publish port 9200" % sorted(seen))
PY
