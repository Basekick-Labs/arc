#!/usr/bin/env bash
# arcx_suite_compare.sh — sweep a CORPUS of queries through both engines and report TWO things:
#
#   1. ENGINE time (execution_time_ms) — arcx vs DuckDB. This is the REAL engine comparison:
#      it isolates engine work from the wire. Read from the JSON response body.
#   2. ROUND-TRIP per wire format (json / msgpack / arrow) — the full HTTP time_total. The wire
#      tax is IDENTICAL for both engines (same transcode + socket code), so this characterizes
#      the WIRE, not the engine — useful to see where we are on encoder cost, not to compare
#      arcx vs DuckDB. Shown for arcx-serve (the tax is the same under DuckDB).
#
# Both engines go through the identical HTTP product path (ARC_ROUTER=serve = arcx, off = DuckDB);
# only the engine differs. The server is started ONCE per mode and the corpus looped through it.
#
# Usage:
#   scripts/arcx_suite_compare.sh                 # built-in servable-shape corpus
#   scripts/arcx_suite_compare.sh queries.txt     # your corpus (label<TAB>SQL per line, # = comment)
#   ITERS=5 PORT=8199 DATA_ROOT=./data ARC_BIN=./arc scripts/arcx_suite_compare.sh
#
# Build first:
#   cargo build --release --manifest-path ../arcx/Cargo.toml && \
#   CGO_ENABLED=1 go build -tags=duckdb_arrow,arcx_engine -o arc ./cmd/arc
set -euo pipefail

PORT="${PORT:-8199}"
ITERS="${ITERS:-5}"
DATA_ROOT="${DATA_ROOT:-./data}"
ARC_BIN="${ARC_BIN:-./arc}"
BASE="http://localhost:${PORT}"
CORPUS_FILE="${1:-}"

[ -x "$ARC_BIN" ] || { echo "ERROR: arc binary not found at $ARC_BIN"; exit 1; }
command -v curl >/dev/null || { echo "ERROR: curl required"; exit 1; }
command -v python3 >/dev/null || { echo "ERROR: python3 required"; exit 1; }

# Servable-shape corpus: label<TAB>SIZE<TAB>SQL. SIZE ∈ {small,big}. `small` gets the full
# wire sweep (json/msgpack/arrow round-trip — fast + informative). `big` (multi-hundred-MB /
# multi-GB results) gets ENGINE time ONLY — its round-trip is all wire transfer (tells us
# nothing about the engine, and a full sweep would move tens of GB). Adjust thresholds for
# your dataset. A custom corpus file uses the same 3-column format.
read -r -d '' BUILTIN_CORPUS <<'CORPUS' || true
count(*)	small	SELECT count(*) FROM production.cpu
date_trunc hour,count	small	SELECT date_trunc('hour', time) AS h, count(*) FROM production.cpu GROUP BY 1
min(value)	small	SELECT min(value) FROM production.cpu
max(value)	small	SELECT max(value) FROM production.cpu
scan eq (host)	small	SELECT host, value FROM production.cpu WHERE host = 'server001'
scan int cmp	small	SELECT host, value FROM production.cpu WHERE value > 99.9
scan AND	small	SELECT host, value FROM production.cpu WHERE value > 99.5 AND host = 'server001'
scan OR	small	SELECT host, value FROM production.cpu WHERE host = 'server001' OR host = 'server009'
scan IN	small	SELECT host, value FROM production.cpu WHERE host IN ('server001','server005','server009')
scan BETWEEN	small	SELECT host, value FROM production.cpu WHERE value BETWEEN 99.5 AND 99.9
scan LIKE	small	SELECT host, value FROM production.cpu WHERE host LIKE 'server00%' AND value > 99.9
arith *+-	small	SELECT host, value FROM production.cpu WHERE value * 2.0 > 199.8
division	small	SELECT host, value FROM production.cpu WHERE value / 2.0 > 49.95
multiterm	small	SELECT host, value FROM production.cpu WHERE value * 2 + cpu_idle > 299.0
scan mid (~600k)	small	SELECT host, value FROM production.cpu WHERE value * 2 + cpu_idle > 290.0
CORPUS
# NOTE: large-result shapes (>10M rows / >100MB) are intentionally NOT in the default corpus:
# their HTTP time is transfer-bound (the wire tax, engine-independent), so an HTTP suite tells
# you nothing new about the engine there — and a full sweep moves tens of GB. They're measured
# directly in the lazy-scan / streaming-roundtrip validations (61.8M rows / 1.3GB at ~119MB RSS,
# byte-identical to DuckDB). To include one anyway, add a `big`-tagged line (engine-time only).

ARC_PID=""
cleanup() { [ -n "$ARC_PID" ] && kill "$ARC_PID" 2>/dev/null || true; ARC_PID=""; }
trap cleanup EXIT

start_arc() { # $1 = mode (serve|off)
  cleanup
  ARC_SERVER_PORT="$PORT" ARC_ROUTER="$1" ARC_AUTH_ENABLED=false ARC_TELEMETRY_ENABLED=false \
    DATA_ROOT="$DATA_ROOT" "$ARC_BIN" >/tmp/arcx_suite_${1}.log 2>&1 &
  ARC_PID=$!
  for _ in $(seq 1 60); do curl -fs "${BASE}/health" >/dev/null 2>&1 && return 0; sleep 0.5; done
  echo "ERROR: Arc ($1) failed to start — see /tmp/arcx_suite_${1}.log"; exit 1
}

# ENGINE time: JSON body's execution_time_ms, median of ITERS. Echo "exec_ms row_count served".
engine_json() { # $1 = sql
  local body; body=$(python3 -c "import json,sys;print(json.dumps({'sql':sys.argv[1]}))" "$1")
  : > /tmp/arcx_suite_serve.log 2>/dev/null || true
  curl -s -o /dev/null -m300 -H "Content-Type: application/json" -X POST "${BASE}/api/v1/query" -d "$body" 2>/dev/null || true # warm
  local ms_list="" rc="?"
  for _ in $(seq 1 "$ITERS"); do
    local r; r=$(curl -s -m300 -H "Content-Type: application/json" -X POST "${BASE}/api/v1/query" -d "$body" 2>/dev/null || echo '{}')
    local pair; pair=$(echo "$r" | python3 -c "import json,sys
d=json.load(sys.stdin); print(d.get('execution_time_ms','?'), d.get('row_count','?'))" 2>/dev/null || echo "? ?")
    ms_list+="$(echo "$pair" | awk '{print $1}') "; rc=$(echo "$pair" | awk '{print $2}')
  done
  local med; med=$(echo "$ms_list" | tr ' ' '\n' | grep -v '^$\|?' | sort -n | awk '{a[NR]=$1} END{print (NR?a[int(NR/2)+1]:"?")}')
  local served="?"; grep -q "arcx scan cost\|arcx filtered footer-agg:.*served=true" /tmp/arcx_suite_serve.log 2>/dev/null && served="yes"
  echo "${med:-?} $rc $served"
}

# ROUND-TRIP: median time_total for a wire endpoint. Echo the median seconds.
roundtrip() { # $1 = sql, $2 = endpoint path
  local body; body=$(python3 -c "import json,sys;print(json.dumps({'sql':sys.argv[1]}))" "$1")
  curl -s -o /dev/null -m300 -H "Content-Type: application/json" -X POST "${BASE}$2" -d "$body" 2>/dev/null || true # warm
  local ts=""
  for _ in $(seq 1 "$ITERS"); do
    ts+="$(curl -s -o /dev/null -w '%{time_total}' -m300 -H 'Content-Type: application/json' -X POST "${BASE}$2" -d "$body" 2>/dev/null || echo 999) "
  done
  echo "$ts" | tr ' ' '\n' | grep -v '^$' | sort -n | awk '{a[NR]=$1} END{print a[int(NR/2)+1]}'
}

# Load corpus.
if [ -n "$CORPUS_FILE" ]; then
  CORPUS=$(grep -v '^\s*#' "$CORPUS_FILE" | grep -v '^\s*$'); SRC="$CORPUS_FILE"
else CORPUS="$BUILTIN_CORPUS"; SRC="built-in servable-shape suite"; fi
echo "Corpus: $SRC   Port: $PORT   Iters: $ITERS   Data: $DATA_ROOT"

# ── Pass 1: arcx serve — engine time + wire tax across json/msgpack/arrow ────────────────
echo "→ arc (serve = arcx) …"; start_arc serve
declare -a LABELS SQLS SIZES A_MS A_ROWS A_SERVED WT_JSON WT_MSG WT_ARR
i=0
while IFS=$'\t' read -r label size sql; do
  [ -z "${label:-}" ] && continue
  read -r ms rc served <<<"$(engine_json "$sql")"
  LABELS[$i]="$label"; SQLS[$i]="$sql"; SIZES[$i]="$size"; A_MS[$i]="$ms"; A_ROWS[$i]="$rc"; A_SERVED[$i]="$served"
  if [ "$size" = "small" ]; then
    WT_JSON[$i]="$(roundtrip "$sql" /api/v1/query)"
    WT_MSG[$i]="$(roundtrip "$sql" /api/v1/query/msgpack)"
    WT_ARR[$i]="$(roundtrip "$sql" /api/v1/query/arrow)"
  else
    WT_JSON[$i]="-"; WT_MSG[$i]="-"; WT_ARR[$i]="-"  # big: engine time only, wire sweep skipped
  fi
  i=$((i+1))
done <<<"$CORPUS"
N=$i

# ── Pass 2: DuckDB — engine time only (wire tax is identical, no need to re-measure) ─────
echo "→ arc (off = DuckDB) …"; start_arc off
declare -a D_MS D_ROWS
for ((j=0; j<N; j++)); do read -r ms rc _ <<<"$(engine_json "${SQLS[$j]}")"; D_MS[$j]="$ms"; D_ROWS[$j]="$rc"; done
cleanup

# ── Table 1: ENGINE time (the real arcx-vs-DuckDB comparison) ────────────────────────────
echo
echo "═══ ENGINE (execution_time_ms — wire-independent, the real arcx vs DuckDB) ═══"
printf "%-22s | %-6s | %8s | %8s | %6s | %s\n" "query" "served" "arcx" "duckdb" "x" "rows(match)"
printf -- "-----------------------|--------|----------|----------|--------|------------\n"
match=0; served=0
for ((j=0; j<N; j++)); do
  a="${A_MS[$j]}"; d="${D_MS[$j]}"
  x=$(python3 -c "
a='$a'; d='$d'
try: print(f'{float(d)/float(a):.2f}x' if float(a)>0 else '-')
except: print('-')" 2>/dev/null || echo '-')
  m=$([ "${A_ROWS[$j]}" = "${D_ROWS[$j]}" ] && echo ok || echo MISMATCH); [ "$m" = ok ] && match=$((match+1))
  [ "${A_SERVED[$j]}" = yes ] && served=$((served+1))
  printf "%-22s | %-6s | %6sms | %6sms | %6s | %s(%s)\n" \
    "${LABELS[$j]:0:22}" "${A_SERVED[$j]}" "$a" "$d" "$x" "$m" "${A_ROWS[$j]}"
done
echo "engine served(marker): $served/$N   row-count match: $match/$N   (x>1 = arcx faster)"

# ── Table 2: WIRE TAX (round-trip per format; same for both engines — where we are on encode) ─
echo
echo "═══ WIRE round-trip (arcx-serve; the tax is engine-INDEPENDENT — same under DuckDB) ═══"
printf "%-22s | %9s | %9s | %9s\n" "query" "json" "msgpack" "arrow"
printf -- "-----------------------|-----------|-----------|-----------\n"
fmt_wt() { [ "$1" = "-" ] && printf "%9s" "(skip)" || printf "%8.3fs" "$1"; }
for ((j=0; j<N; j++)); do
  [ "${SIZES[$j]}" = "big" ] && continue  # big rows: no wire sweep (all transfer)
  printf "%-22s | %s | %s | %s\n" "${LABELS[$j]:0:22}" "$(fmt_wt "${WT_JSON[$j]}")" "$(fmt_wt "${WT_MSG[$j]}")" "$(fmt_wt "${WT_ARR[$j]}")"
done
echo
echo "Reading it: ENGINE table = arcx vs DuckDB (isolated). WIRE table = the HTTP tax both pay"
echo "(dominates on large results → engine wins hide there; small/agg results show the engine gap)."
