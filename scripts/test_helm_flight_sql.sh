#!/usr/bin/env bash
# Rendered-template checks for the Flight SQL listener in both charts.
# Off by default: no port, no env. On: the env, a named container port and a
# Service port, on the OSS deployment and on the enterprise writers and
# readers only. A port that collides with one of Arc's is refused at render.
set -euo pipefail

fail() { echo "FAIL: $*" >&2; exit 1; }

# render must succeed (a render error would otherwise pass every "nothing
# Flight here" check); refused must fail.
ent=(--set license.key=not-a-real-key --set cluster.sharedSecret.value=not-a-real-secret
  --set seaweedfs.credentials.accessKey=not-a-real-user --set seaweedfs.credentials.secretKey=not-a-real-password)
render() {
  local chart=$1; shift
  local extra=()
  [ "$chart" = helm/arc-enterprise ] && extra=("${ent[@]}")
  helm template t "$chart" "${extra[@]}" "$@" || fail "helm template $chart $* failed"
}
refused() {
  local chart=$1; shift
  local extra=()
  [ "$chart" = helm/arc-enterprise ] && extra=("${ent[@]}")
  helm template t "$chart" "${extra[@]}" "$@" >/dev/null 2>&1 && fail "helm template $chart $* rendered; it must be refused"
  return 0
}

# OSS chart, default: nothing Flight anywhere.
out=$(render helm/arc)
grep -qi 'flight' <<<"$out" && fail "helm/arc renders Flight SQL with flightSql.enabled=false"

# OSS chart, on, at non-default ports.
dep=$(render helm/arc --show-only templates/deployment.yaml --set flightSql.enabled=true --set flightSql.port=50777)
svc=$(render helm/arc --show-only templates/service.yaml --set flightSql.enabled=true --set flightSql.servicePort=443)
grep -A1 'name: ARC_FLIGHT_SQL_ENABLED' <<<"$dep" | grep -q 'value: "true"' || fail "helm/arc: ARC_FLIGHT_SQL_ENABLED missing"
grep -A1 'name: ARC_FLIGHT_SQL_PORT' <<<"$dep" | grep -q 'value: "50777"' || fail "helm/arc: ARC_FLIGHT_SQL_PORT is not the configured port"
grep -A1 'name: flight-sql' <<<"$dep" | grep -q 'containerPort: 50777' || fail "helm/arc: container port flight-sql missing"
grep -B1 -A4 'port: 443' <<<"$svc" | grep -q 'targetPort: flight-sql' || fail "helm/arc: Service port flight-sql missing"
grep -q 'appProtocol: grpc' <<<"$svc" || fail "helm/arc: Service port lacks appProtocol grpc"

# OSS chart, colliding port.
refused helm/arc --set flightSql.enabled=true --set flightSql.port=8000

# Enterprise chart, default: nothing Flight anywhere.
out=$(render helm/arc-enterprise)
grep -qi 'flight' <<<"$out" && fail "helm/arc-enterprise renders Flight SQL with flightSql.enabled=false"

# Enterprise chart, on: writers and readers, not compactors.
on=(--set flightSql.enabled=true --set flightSql.port=50777 --set flightSql.servicePort=443)
for role in writer reader; do
  sts=$(render helm/arc-enterprise --show-only "templates/$role-statefulset.yaml" "${on[@]}")
  svc=$(render helm/arc-enterprise --show-only "templates/$role-service.yaml" "${on[@]}")
  grep -A1 'name: ARC_FLIGHT_SQL_PORT' <<<"$sts" | grep -q 'value: "50777"' || fail "$role: ARC_FLIGHT_SQL_PORT is not the configured port"
  grep -A1 'name: flight-sql' <<<"$sts" | grep -q 'containerPort: 50777' || fail "$role: container port flight-sql missing"
  grep -A3 'name: flight-sql' <<<"$svc" | grep -q 'port: 443' || fail "$role: Service port flight-sql missing"
  grep -q 'appProtocol: grpc' <<<"$svc" || fail "$role: Service port lacks appProtocol grpc"
done
comp=$(render helm/arc-enterprise --show-only templates/compactor-statefulset.yaml "${on[@]}")
grep -qi 'flight' <<<"$comp" && fail "the compactor got the Flight SQL listener"

# Enterprise chart, colliding ports.
for p in 8000 9100 9200; do
  refused helm/arc-enterprise --set flightSql.enabled=true --set flightSql.port=$p
done

echo "Flight SQL chart checks passed"
