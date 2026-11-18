#!/usr/bin/env bash
# Runs one freshness run against a fresh Arc: starts the binary in a temp
# directory with only the settings the run names, runs the harness, stops Arc.
#
#   scripts/freshness/run.sh <run> [extra harness flags]
#
#   r1    defaults (5 s buffer age), 20 rows/s: the age-triggered flush
#   r1s   defaults, 5,000 filler rows per request: the size-triggered flush
#   r2    ingest.max_buffer_age_ms=100
#   r6    one late row (3 h back) into an hour a query just found empty (#1757)
#   r6f   the same with a row stamped 2 h ahead
#   r7    500 measurements, 4 of them probed
#
# Environment:
#   ARC_BIN      Arc binary (default ./arc; build with -tags=duckdb_arrow)
#   PORT         port to listen on (default 18758; must be free)
#   COMPACTION   1 to leave compaction on (default off: its hourly run clears
#                the query caches and would end a late-data trial early)
#   OUT          write the JSON report here
#   KEEP         1 to keep the temp directory (data, arc.log)
#   ARC_*        any other Arc setting, e.g. ARC_INGEST_MAX_BUFFER_AGE_MS=250
set -euo pipefail

run="${1:-}"; shift || true
case "$run" in r1|r1s|r2|r6|r6f|r7) ;; *) sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'; exit 2 ;; esac

repo="$(cd "$(dirname "$0")/../.." && pwd)"
arc_bin="${ARC_BIN:-$repo/arc}"
port="${PORT:-18758}"
[ -x "$arc_bin" ] || { echo "no Arc binary at $arc_bin (set ARC_BIN)" >&2; exit 1; }
arc_bin="$(cd "$(dirname "$arc_bin")" && pwd)/$(basename "$arc_bin")"
if lsof -ti "tcp:$port" >/dev/null 2>&1; then echo "port $port is in use (set PORT)" >&2; exit 1; fi
# Arc also reads /etc/arc/arc.toml; the temp HOME below takes care of ~/.arc.
[ -e /etc/arc/arc.toml ] && echo "warning: /etc/arc/arc.toml exists and Arc will read it" >&2

harness="$(mktemp -d)/freshness"
(cd "$repo" && go build -o "$harness" ./scripts/freshness)

dir="$(mktemp -d)"
token="$(openssl rand -hex 24)"
stop() {
  pids="$(lsof -ti "tcp:$port" 2>/dev/null || true)"
  [ -n "$pids" ] && kill $pids 2>/dev/null || true
  for _ in $(seq 1 50); do lsof -ti "tcp:$port" >/dev/null 2>&1 || break; sleep 0.1; done
  if [ "${KEEP:-0}" = 1 ]; then echo "kept $dir" >&2; else rm -rf "$dir"; fi
  rm -rf "$(dirname "$harness")"
}
trap stop EXIT

# Only what is set here and the ARC_* the caller exported reaches Arc. Never
# start it from the repository root: the arc.toml there sets a 100 ms buffer age.
env_run=()
case "$run" in r2) env_run+=("ARC_INGEST_MAX_BUFFER_AGE_MS=${ARC_INGEST_MAX_BUFFER_AGE_MS:-100}") ;; esac
compaction=false; [ "${COMPACTION:-0}" = 1 ] && compaction=true
passthrough=()
while IFS= read -r kv; do passthrough+=("$kv"); done < <(env | grep '^ARC_' | grep -v -e '^ARC_BIN=' -e '^ARC_TOKEN=' || true)

(cd "$dir" && exec env -i PATH="$PATH" HOME="$dir" \
  ARC_SERVER_PORT="$port" ARC_STORAGE_LOCAL_PATH="$dir/data/arc" ARC_AUTH_DB_PATH="$dir/data/arc.db" \
  ARC_AUTH_BOOTSTRAP_TOKEN="$token" ARC_TELEMETRY_ENABLED=false ARC_LOG_LEVEL=warn \
  ARC_COMPACTION_ENABLED="$compaction" ${passthrough[@]+"${passthrough[@]}"} ${env_run[@]+"${env_run[@]}"} \
  "$arc_bin" >"$dir/arc.log" 2>&1) &

up=0
for _ in $(seq 1 120); do
  if curl -s -o /dev/null "http://127.0.0.1:$port/health"; then up=1; break; fi
  sleep 0.25
done
[ "$up" = 1 ] || { echo "Arc did not start; log:" >&2; tail -20 "$dir/arc.log" >&2; exit 1; }

common=(-http "http://127.0.0.1:$port" -token "$token")
[ -n "${OUT:-}" ] && common+=(-out "$OUT")
case "$run" in
  r1)  "$harness" steady "${common[@]}" -rate 20 -duration 60s "$@" ;;
  r1s) "$harness" steady "${common[@]}" -rate 20 -batch 5000 -duration 30s "$@" ;;
  r2)  "$harness" steady "${common[@]}" -rate 20 -duration 60s "$@" ;;
  r6)  "$harness" late   "${common[@]}" -offset -3h "$@" ;;
  r6f) "$harness" late   "${common[@]}" -offset 2h "$@" ;;
  r7)  "$harness" steady "${common[@]}" -measurements 500 -probe-sample 4 -rate 200 -duration 30s "$@" ;;
esac
