#!/usr/bin/env bash
set -euo pipefail

chart="${1:-helm/arc}"
manifest=$(mktemp)
trap 'rm -f "$manifest"' EXIT

check_probe() {
  local threshold="$1"
  helm template arc "$chart" --set "startupProbeFailureThreshold=${threshold}" > "$manifest"
  local probe
  probe=$(sed -n '/startupProbe:/,/livenessProbe:/p' "$manifest")

  [[ "$probe" == *'path: /health'* ]]
  [[ "$probe" == *"failureThreshold: ${threshold}"* ]]
  [[ "$probe" == *'periodSeconds: 10'* ]]
}

check_probe 30
check_probe 45
