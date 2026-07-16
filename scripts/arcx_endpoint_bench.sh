#!/usr/bin/env bash
# arcx_endpoint_bench.sh — reusable arcx-vs-DuckDB endpoint benchmark + correctness check.
#
# Runs ONE SQL query through Arc's three query endpoints — JSON (/api/v1/query),
# msgpack (/api/v1/query/msgpack), Arrow-IPC (/api/v1/query/arrow) — twice: once with
# ARC_ROUTER=serve (arcx engine serves) and once with ARC_ROUTER=off (DuckDB serves),
# then reports p50 latency + payload size for each, and VERIFIES the two engines agree
# (row_count on JSON). It starts/stops its own Arc server on a non-default port with auth
# + telemetry disabled (ARC_AUTH_ENABLED=false, ARC_TELEMETRY_ENABLED=false), so it never
# touches a running instance and needs no token.
#
# WHY it exists: the differential harness proves CORRECT on synthetic fixtures; this proves
# correct AND fast end-to-end through the real product path (HTTP → router → engine → Arrow
# → wire encode) on REAL data. Reusable across measurements/queries/slices.
#
# Usage:
#   scripts/arcx_endpoint_bench.sh "SELECT host, value FROM production.cpu WHERE host IN ('server001','server005')"
#   scripts/arcx_endpoint_bench.sh                 # runs the default IN example on production/cpu
#   ITERS=30 PORT=8199 scripts/arcx_endpoint_bench.sh "<sql>"
#
# Env overrides:
#   PORT       (default 8199)   — never 8000 (the default prod port)
#   ITERS      (default 15)     — timed iterations per endpoint (best-of, p50 reported)
#   DATA_ROOT  (default ./data) — Arc storage root (must contain arc/<db>/<measurement>/…)
#   ARC_BIN    (default ./arc)  — an arc binary built with -tags=duckdb_arrow,arcx_engine
#   SKIP_DUCKDB=1              — only measure arcx (skip the router=off comparison)
#
# Prereqs: an arc binary with BOTH tags:  make build-arcx   (or)
#   cargo build --release --manifest-path ../arcx/Cargo.toml && \
#   CGO_ENABLED=1 go build -tags=duckdb_arrow,arcx_engine -o arc ./cmd/arc
set -euo pipefail

QUERY="${1:-SELECT host, value FROM production.cpu WHERE host IN ('server001','server005','server009')}"
PORT="${PORT:-8199}"
ITERS="${ITERS:-15}"
DATA_ROOT="${DATA_ROOT:-./data}"
ARC_BIN="${ARC_BIN:-./arc}"
BASE="http://localhost:${PORT}"

[ -x "$ARC_BIN" ] || { echo "ERROR: arc binary not found/executable at $ARC_BIN (build with: make build-arcx)"; exit 1; }
command -v curl >/dev/null || { echo "ERROR: curl required"; exit 1; }
command -v python3 >/dev/null || { echo "ERROR: python3 required"; exit 1; }

BODY=$(python3 -c "import json,sys; print(json.dumps({'sql': sys.argv[1]}))" "$QUERY")
ARC_PID=""

cleanup() { [ -n "$ARC_PID" ] && kill "$ARC_PID" 2>/dev/null || true; }
trap cleanup EXIT

start_arc() { # $1 = router mode (serve|off)
  cleanup; ARC_PID=""
  ARC_SERVER_PORT="$PORT" ARC_ROUTER="$1" \
    ARC_AUTH_ENABLED=false ARC_TELEMETRY_ENABLED=false \
    "$ARC_BIN" >/tmp/arcx_bench_${1}.log 2>&1 &
  ARC_PID=$!
  for _ in $(seq 1 40); do
    curl -fs "${BASE}/health" >/dev/null 2>&1 && return 0
    sleep 0.5
  done
  echo "ERROR: Arc ($1) failed to start on :$PORT — see /tmp/arcx_bench_${1}.log"; exit 1
}

# hit ENDPOINT once, print "time_total size_download http_code"
hit() { # $1 = path
  curl -s -o /dev/null -w "%{time_total} %{size_download} %{http_code}" \
    -H "Content-Type: application/json" \
    -X POST "${BASE}$1" -d "$BODY"
}

# bench ENDPOINT: warm 2, time ITERS, report p50 + size. echoes "p50_s size_bytes rows"
bench() { # $1 = name, $2 = path
  local name="$1" path="$2"
  hit "$path" >/dev/null; hit "$path" >/dev/null   # warm
  local tmp; tmp=$(mktemp)
  for _ in $(seq 1 "$ITERS"); do hit "$path" >> "$tmp"; echo >> "$tmp"; done
  local med size code
  med=$(awk '{print $1}' "$tmp" | sort -n | awk '{a[NR]=$1} END{print a[int(NR/2)+1]}')
  size=$(tail -1 "$tmp" | awk '{print $2}')
  code=$(tail -1 "$tmp" | awk '{print $3}')
  rm -f "$tmp"
  local mb; mb=$(python3 -c "print(f'{$size/1048576:.2f}')")
  printf "  %-9s p50=%6.3fs  payload=%7sMB  http=%s\n" "$name" "$med" "$mb" "$code"
}

# JSON row_count for the correctness check (arcx vs duckdb must agree)
row_count() {
  curl -s -H "Content-Type: application/json" \
    -X POST "${BASE}/api/v1/query" -d "$BODY" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('row_count','?'))" 2>/dev/null || echo "ERR"
}

# did arcx actually serve? (its cost line only appears in serve mode)
# Detect whether arcx served vs fell back to DuckDB. arcx logs a cost line for the
# SCAN path ("arcx scan cost") and the FILTERED footer-agg path ("arcx filtered
# footer-agg: ... served=true"), but the UNFILTERED footer-agg / min-max / count paths
# log nothing, so a "no" here can be a false negative for those shapes — cross-check
# the latency (an arcx footer answer is ~ms; a DuckDB fallback scans rows). A definitive
# marker for every footer shape is a TODO on the engine side (issue: emit a served-shape
# log line uniformly). Until then this covers scan + filtered-agg definitively.
arcx_served() {
  if grep -q "arcx scan cost" /tmp/arcx_bench_serve.log ||
     grep -q "arcx filtered footer-agg:.*served=true" /tmp/arcx_bench_serve.log; then
    echo yes
  else
    echo "no (or an unlogged footer shape — check latency)"
  fi
}

echo "Query: $QUERY"
echo "Port: $PORT   Iters: $ITERS   Data: $DATA_ROOT"
echo

echo "=== arcx (ARC_ROUTER=serve) ==="
start_arc serve
ARCX_ROWS=$(row_count)
echo "  served by arcx: $(arcx_served)   row_count: $ARCX_ROWS"
bench "json"    "/api/v1/query"
bench "msgpack" "/api/v1/query/msgpack"
bench "arrow"   "/api/v1/query/arrow"

if [ "${SKIP_DUCKDB:-0}" != "1" ]; then
  echo
  echo "=== DuckDB (ARC_ROUTER=off) ==="
  start_arc off
  DUCK_ROWS=$(row_count)
  echo "  row_count: $DUCK_ROWS"
  bench "json"    "/api/v1/query"
  bench "msgpack" "/api/v1/query/msgpack"
  bench "arrow"   "/api/v1/query/arrow"

  echo
  if [ "$ARCX_ROWS" = "$DUCK_ROWS" ] && [ "$ARCX_ROWS" != "ERR" ] && [ "$ARCX_ROWS" != "?" ]; then
    echo "✅ CORRECTNESS: arcx and DuckDB agree ($ARCX_ROWS rows)"
  else
    echo "❌ MISMATCH: arcx=$ARCX_ROWS  duckdb=$DUCK_ROWS  — INVESTIGATE"
    exit 2
  fi
  if [ "$(arcx_served)" != "yes" ]; then
    echo "⚠️  NOTE: arcx did NOT serve (declined → the 'arcx' numbers above are DuckDB fallback)."
    echo "    Common cause: the 512 MiB scan byte budget (see arcX #24). Check /tmp/arcx_bench_serve.log."
  fi
fi
