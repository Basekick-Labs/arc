#!/usr/bin/env bash
set -euo pipefail

chart="${1:-helm/arc}"
manifest=$(mktemp)
trap 'rm -f "$manifest"' EXIT

check_probe() {
  local threshold="$1"
  shift
  helm template arc "$chart" "$@" > "$manifest"
  local probe
  probe=$(sed -n '/startupProbe:/,/livenessProbe:/p' "$manifest")

  [[ "$probe" == *'path: /health'* ]] || { echo 'startupProbe must target /health' >&2; return 1; }
  [[ "$probe" == *"failureThreshold: ${threshold}"* ]] || { echo "startupProbe must use failureThreshold ${threshold}" >&2; return 1; }
  [[ "$probe" == *'periodSeconds: 10'* ]] || { echo 'startupProbe must use a 10-second period' >&2; return 1; }
}

check_probe 30
check_probe 45 --set startupProbeFailureThreshold=45
