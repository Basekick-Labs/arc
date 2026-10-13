#!/usr/bin/env bash
set -euo pipefail

chart="${1:-helm/arc}"
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

# The binary ignore must stay root-anchored, and new files in the OSS chart
# and in cmd/arc must remain visible to Git. A binary built inside cmd/arc
# stays ignored.
git check-ignore --no-index -q arc
git check-ignore --no-index -q cmd/arc/arc
for probe in helm/arc/templates/contribution-probe.yaml cmd/arc/contribution-probe.go; do
  if git check-ignore --no-index -q "$probe"; then
    echo "new files like $probe must not be ignored" >&2
    exit 1
  fi
done

cat > "$tmpdir/arc.toml" <<'EOF'
[server]
port = 8000
EOF
cat > "$tmpdir/arc-changed.toml" <<'EOF'
[server]
port = 8001
EOF

helm lint "$chart"
helm template arc-test "$chart" > "$tmpdir/default.yaml"
helm template arc-test "$chart" \
  --set arc.config.enabled=true \
  --set-file arc.config.contents="$tmpdir/arc.toml" \
  --set-string auth.bootstrapToken.value=regression-test-token-32-characters \
  > "$tmpdir/config-and-token.yaml"
helm template arc-test "$chart" \
  --set arc.config.enabled=true \
  --set-file arc.config.contents="$tmpdir/arc-changed.toml" \
  > "$tmpdir/config-changed.yaml"
helm template arc-test "$chart" \
  --set auth.bootstrapToken.existingSecret=precreated-admin-token \
  > "$tmpdir/existing-token.yaml"

# Arc refuses a token under 32 characters and starts with no admin token, so
# the chart must refuse to render one. Exactly 32 is accepted.
if out=$(helm template arc-test "$chart" \
  --set-string auth.bootstrapToken.value=0123456789012345678901234567890 2>&1); then
  echo 'chart rendered a 31-character bootstrap token' >&2
  exit 1
fi
if ! grep -q 'must be at least 32 characters' <<<"$out"; then
  echo "unexpected render error for a short bootstrap token: $out" >&2
  exit 1
fi
helm template arc-test "$chart" \
  --set-string auth.bootstrapToken.value=01234567890123456789012345678901 > /dev/null

python3 - "$tmpdir" <<'PY'
import pathlib
import re
import sys

tmpdir = pathlib.Path(sys.argv[1])

def documents(name):
    return [doc for doc in (tmpdir / name).read_text().split("---") if doc.strip()]

def resource(docs, kind):
    matches = [doc for doc in docs if re.search(r"^kind:\s*" + re.escape(kind) + r"\s*$", doc, re.M)]
    if len(matches) > 1:
        raise AssertionError(f"expected at most one {kind}, got {len(matches)}")
    return matches[0] if matches else ""

def env_secret(deployment):
    match = re.search(
        r"- name: ARC_AUTH_BOOTSTRAP_TOKEN\s+valueFrom:\s+secretKeyRef:\s+name: (\S+)\s+key: bootstrap-token",
        deployment,
    )
    return match.group(1) if match else None

def config_checksum(deployment):
    match = re.search(r"^\s+checksum/config: (\S+)$", deployment, re.M)
    return match.group(1) if match else None

default = documents("default.yaml")
if resource(default, "ConfigMap") or resource(default, "Secret"):
    raise AssertionError("optional resources rendered with default values")
if config_checksum(resource(default, "Deployment")):
    raise AssertionError("config checksum rendered without arc.config")
if env_secret(resource(default, "Deployment")):
    raise AssertionError("bootstrap token env rendered without a configured token")

configured = documents("config-and-token.yaml")
configmap = resource(configured, "ConfigMap")
deployment = resource(configured, "Deployment")
secret = resource(configured, "Secret")
if not configmap or "arc.toml: |" not in configmap or "port = 8000" not in configmap:
    raise AssertionError("enabled arc.config did not render its TOML ConfigMap")
config_name = re.search(r"^  name: (\S+-config)$", configmap, re.M)
mount_name = re.search(r"configMap:\s+name: (\S+)", deployment)
if not config_name or not mount_name or config_name.group(1) != mount_name.group(1):
    raise AssertionError("Deployment does not mount the rendered configuration ConfigMap")
secret_name = re.search(r"^  name: (\S+-bootstrap-token)$", secret, re.M)
if not secret_name or env_secret(deployment) != secret_name.group(1):
    raise AssertionError("Deployment bootstrap token reference does not match its generated Secret")
if not re.search(r'^  bootstrap-token: "?regression-test-token-32-characters"?$', secret, re.M):
    raise AssertionError("generated Secret does not contain the configured token")

# The config is mounted with subPath, which never refreshes in a running pod,
# so a changed config must change the pod template.
checksum = config_checksum(deployment)
changed = config_checksum(resource(documents("config-changed.yaml"), "Deployment"))
if not checksum or not changed or changed == checksum:
    raise AssertionError("pod template does not change when arc.config.contents changes")

existing = documents("existing-token.yaml")
if resource(existing, "Secret"):
    raise AssertionError("chart generated a Secret despite existingSecret")
if env_secret(resource(existing, "Deployment")) != "precreated-admin-token":
    raise AssertionError("Deployment does not use the configured existing Secret")
PY
