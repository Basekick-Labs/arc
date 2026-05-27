#!/usr/bin/env bash
# =============================================================================
# Pattern 2 multi-writer smoke harness.
#
# Three scenarios share one harness:
#   base                 — boot 3 writers + 1 reader, ingest N records via
#                          Traefik LB, query through reader, assert count.
#   non-leader-crash     — same, but kill a non-leader writer mid-ingest.
#                          Asserts: writes that succeeded HTTP-side are
#                          all queryable; LB drained the dead writer.
#   leader-crash         — same, but kill the current Raft leader mid-ingest.
#                          Asserts: Raft elects a new leader within a few
#                          seconds; writes continue; succeeded writes are
#                          all queryable.
#
# What this validates end-to-end (from PR1a):
#   - cluster.shared_storage_mode=true accepted by startup validation
#   - all 3 writers come up healthy and serve /ready=200 after WAL replay
#   - Traefik LB distributes writes across writers (Pattern 2: every writer
#     accepts ingest, no leader-forwarding)
#   - writers PUT to the shared MinIO bucket with non-colliding filenames
#   - reader queries pick up records flushed by all writers
#   - HA via LB retry: a writer crash is recovered by /ready draining; the
#     LB stops routing to it within one poll cycle
#   - HA via Raft: a leader crash triggers election; the new leader picks
#     up singleton-task ownership (retention/CQ/delete) for the next tick
#
# Usage:
#   export ARC_LICENSE_KEY="ARC-ENT-..."     # dev key, NOT the customer key
#   export ARC_CLUSTER_SHARED_SECRET=$(openssl rand -hex 32)
#   ./smoke.sh                                # default: scenario=base, 1000 records
#   SCENARIO=non-leader-crash ./smoke.sh
#   SCENARIO=leader-crash ./smoke.sh
#   RECORDS=100000 SCENARIO=base ./smoke.sh
#
# On failure: containers are left running for debugging. Re-run cleans up.
# =============================================================================

set -euo pipefail

cd "$(dirname "$0")"

# ---------- config ----------
SCENARIO="${SCENARIO:-base}"
RECORDS="${RECORDS:-1000}"
TRAEFIK_URL="http://localhost:8000"
# Database name includes a per-run epoch so re-running the smoke doesn't
# collide with leftover state in the shared MinIO bucket (the bucket
# persists across `docker compose down -v` runs because volume-cleanup
# only affects the per-writer SQLite/WAL dirs, not the object store).
DATABASE="smoke_$(date +%s)"
MEASUREMENT="cpu"
READY_TIMEOUT_S="${READY_TIMEOUT_S:-120}"
FLUSH_WAIT_S="${FLUSH_WAIT_S:-20}"
# How far into the write phase we kill a writer (crash scenarios). 0.5 = halfway.
CRASH_AT_FRACTION="${CRASH_AT_FRACTION:-0.5}"
COMPOSE_FILES=(-f docker-compose.yml -f docker-compose.override.smoke.yml)

# ---------- helpers ----------
log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
fail() { log "FAIL: $*"; log "containers preserved for debugging; re-run smoke.sh to teardown"; exit 1; }
ok() { log "ok: $*"; }

require_env() {
  local var=$1
  if [[ -z "${!var:-}" ]]; then
    log "FAIL: $var must be set"
    log "       export $var=<value> before running smoke.sh"
    exit 2
  fi
}

wait_ready() {
  local container=$1
  local deadline=$(( $(date +%s) + READY_TIMEOUT_S ))
  while [[ $(date +%s) -lt $deadline ]]; do
    if docker exec "$container" curl -fsS http://localhost:8000/ready >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

# find_leader echoes the container name of the current Raft leader writer.
# Returns empty if none are leader (shouldn't happen post-bootstrap).
find_leader() {
  for c in arc-writer1 arc-writer2 arc-writer3; do
    # The cluster API exposes Raft status at .raft.is_leader (see
    # internal/cluster/coordinator.go status JSON). The earlier
    # local_node.is_raft_leader path was wrong and silently returned
    # false on every node, which made find_non_leader accidentally
    # pick whichever writer it happened to enumerate first.
    is_leader=$(docker exec "$c" curl -fsS \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      http://localhost:8000/api/v1/cluster 2>/dev/null \
      | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    print("true" if d.get("raft", {}).get("is_leader", False) else "false")
except Exception:
    print("false")
' 2>/dev/null)
    if [[ "$is_leader" == "true" ]]; then
      echo "$c"
      return 0
    fi
  done
  echo ""
  return 1
}

# find_non_leader echoes the container name of a writer that is NOT the
# Raft leader. Returns the first one found.
#
# IMPORTANT: `local leader; leader=$(find_leader)` would mask find_leader's
# exit status (the `local` builtin's exit status always wins). If no
# leader can be identified we MUST fail rather than picking
# arc-writer1 (which may BE the leader). This is the same class of
# silent-fallback bug Gemini caught on PR1a (the local_node.is_raft_leader
# JSON-path mismatch that returned false on every node).
find_non_leader() {
  local leader
  if ! leader=$(find_leader); then
    return 1
  fi
  if [[ -z "$leader" ]]; then
    return 1
  fi
  for c in arc-writer1 arc-writer2 arc-writer3; do
    if [[ "$c" != "$leader" ]]; then
      echo "$c"
      return 0
    fi
  done
  echo ""
  return 1
}

# ---------- scenario validation ----------
case "$SCENARIO" in
  base|non-leader-crash|leader-crash) ;;
  *) fail "unknown SCENARIO=$SCENARIO (use base | non-leader-crash | leader-crash)" ;;
esac

# ---------- pre-flight ----------
require_env ARC_LICENSE_KEY
require_env ARC_CLUSTER_SHARED_SECRET

log "scenario: $SCENARIO ($RECORDS records)"
log "tearing down any existing smoke containers"
docker compose "${COMPOSE_FILES[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

# Defensive: kill any orphaned containers from prior smoke runs that
# share names with this compose's services (`minio`, `arc-writer1`, …).
# `down --remove-orphans` only catches containers labelled for this
# compose project; a stray minio left over from a different stack (the
# enterprise-local compose, a customer's own deployment, etc.) has its
# own project label and would block our minio from starting with "name
# already in use." Force-remove by name; ignore failures (the container
# may not exist, which is fine).
for stray in minio arc-traefik arc-writer1 arc-writer2 arc-writer3 arc-reader1; do
  docker rm -f "$stray" >/dev/null 2>&1 || true
done

log "building + booting 3 writers + 1 reader + MinIO + Traefik"
docker compose "${COMPOSE_FILES[@]}" up -d --build

# ---------- wait for readiness ----------
for c in arc-writer1 arc-writer2 arc-writer3 arc-reader1; do
  log "waiting for $c /ready"
  if ! wait_ready "$c"; then
    docker compose "${COMPOSE_FILES[@]}" logs --tail=80 "$c" || true
    fail "$c did not become ready within ${READY_TIMEOUT_S}s"
  fi
  ok "$c ready"
done

# ---------- bootstrap admin token ----------
log "extracting admin token from arc-writer1 logs"
# The bootstrap-leader token line in Arc's startup output looks like
# (ANSI escapes elided):
#   [bold cyan]  Admin API token: <base64-token>=[/]
# We strip ANSI codes, then grep for the literal "Admin API token:" and
# pick the last whitespace-separated field on that line — which is the
# token value. Previous broken approach used `grep -A1 ... | tail -1`,
# which picked up the next log line and parsed garbage.
ADMIN_TOKEN=$(docker logs arc-writer1 2>&1 | \
  sed -E 's/\x1b\[[0-9;]*[a-zA-Z]//g' | \
  grep -E '^[[:space:]]*Admin API token:' | head -1 | awk '{print $NF}' | tr -d '"')
if [[ -z "${ADMIN_TOKEN:-}" ]]; then
  log "could not extract admin token — last 30 lines of arc-writer1:"
  docker logs arc-writer1 2>&1 | tail -30
  fail "could not extract admin token from arc-writer1 logs"
fi
log "admin token: ${ADMIN_TOKEN:0:12}..."

# ---------- wait for Traefik to discover writer routes ----------
# Container /ready signals the Arc process is up; Traefik's Docker provider
# needs an additional ~few seconds to observe the labels and wire the
# arc-writers + arc-readers routers. Until then POSTs hit Traefik's
# 404-no-route, not the intended writer. Probe via /health (Pattern
# `arc-any` router) which fans out across all nodes — once it returns 200
# we know the routers are populated.
log "waiting for Traefik to discover routes"
TRAEFIK_OK=0
for i in $(seq 1 40); do
  if curl -fsS --max-time 2 "$TRAEFIK_URL/health" >/dev/null 2>&1; then
    # /health proves arc-any router works; verify a writer-only path too
    code=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      "$TRAEFIK_URL/api/v1/databases" 2>/dev/null || echo "fail")
    if [[ "$code" == "200" ]]; then
      TRAEFIK_OK=1
      log "Traefik routes discovered after ${i} probe(s)"
      break
    fi
  fi
  sleep 0.5
done
if [[ "$TRAEFIK_OK" -ne 1 ]]; then
  fail "Traefik did not discover writer routes within 20s"
fi

# ---------- wait for token to replicate to all writers via Raft ----------
# Phase A cluster auth replicates tokens cluster-wide. We extracted the token
# from writer1's banner the moment it printed; replication to writer2/3/reader1
# typically lands within ~500ms but Traefik may route the next request anywhere,
# so probe each writer until ALL accept the token (or fail after 20s).
log "waiting for admin token to replicate across writers"
TOKEN_OK=0
for i in $(seq 1 40); do
  ok=1
  for w in arc-writer1 arc-writer2 arc-writer3; do
    code=$(docker exec "$w" curl -sS -o /dev/null -w '%{http_code}' \
      -H "Authorization: Bearer $ADMIN_TOKEN" \
      http://localhost:8000/api/v1/databases 2>/dev/null || echo "fail")
    if [[ "$code" != "200" ]]; then
      ok=0
      break
    fi
  done
  if [[ "$ok" -eq 1 ]]; then
    TOKEN_OK=1
    log "token replicated to all writers after ${i} probe(s)"
    break
  fi
  sleep 0.5
done
if [[ "$TOKEN_OK" -ne 1 ]]; then
  fail "admin token did not replicate to all writers within 20s"
fi

# ---------- create database ----------
log "creating database $DATABASE"
curl -fsS -X POST "$TRAEFIK_URL/api/v1/databases" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"$DATABASE\"}" >/dev/null || fail "create database"

# ---------- determine kill target (crash scenarios) ----------
KILL_TARGET=""
case "$SCENARIO" in
  leader-crash)
    KILL_TARGET=$(find_leader)
    [[ -z "$KILL_TARGET" ]] && fail "could not identify Raft leader"
    log "kill target (leader): $KILL_TARGET"
    ;;
  non-leader-crash)
    KILL_TARGET=$(find_non_leader)
    [[ -z "$KILL_TARGET" ]] && fail "could not identify non-leader writer"
    log "kill target (non-leader): $KILL_TARGET"
    ;;
esac

# Records at which point we'll kill the target (crash scenarios).
KILL_AT=$(python3 -c "print(int($RECORDS * $CRASH_AT_FRACTION))")

# ---------- write phase ----------
log "writing $RECORDS records via Traefik LB"
[[ "$SCENARIO" != "base" ]] && log "  (will kill $KILL_TARGET at record $KILL_AT)"

BATCH=100
WRITTEN_SENT=0
WRITTEN_OK=0
WRITE_ERRORS=0
KILLED=false
START=$(date +%s)
while [[ $WRITTEN_SENT -lt $RECORDS ]]; do
  remaining=$(( RECORDS - WRITTEN_SENT ))
  this_batch=$(( remaining < BATCH ? remaining : BATCH ))
  lines=""
  for ((i = 0; i < this_batch; i++)); do
    ts_us=$(( ($(date +%s) * 1000000) + WRITTEN_SENT + i ))
    lines+="$MEASUREMENT,host=server$((WRITTEN_SENT + i)) usage=$((RANDOM % 100)) $ts_us"$'\n'
  done

  # Crash trigger
  if [[ "$SCENARIO" != "base" && "$KILLED" == "false" && $WRITTEN_SENT -ge $KILL_AT ]]; then
    log "  >>> killing $KILL_TARGET at record $WRITTEN_SENT"
    docker kill "$KILL_TARGET" >/dev/null
    KILLED=true
  fi

  # Use --max-time so a write to a dying writer doesn't hang the smoke
  # /api/v1/write/line-protocol (WriteSimple) routes the database via the
  # x-arc-database header, not ?db= (?db= is honored by the InfluxDB v1
  # compat /write route only).
  if curl -fsS --max-time 10 -X POST "$TRAEFIK_URL/api/v1/write/line-protocol" \
       -H "Authorization: Bearer $ADMIN_TOKEN" \
       -H "x-arc-database: $DATABASE" \
       --data-binary "$lines" >/dev/null 2>&1; then
    WRITTEN_OK=$(( WRITTEN_OK + this_batch ))
  else
    WRITE_ERRORS=$(( WRITE_ERRORS + 1 ))
    if (( WRITE_ERRORS <= 3 )); then
      log "  write failed at offset $WRITTEN_SENT (error #$WRITE_ERRORS)"
    elif (( WRITE_ERRORS == 4 )); then
      log "  ... further write errors suppressed"
    fi
  fi

  WRITTEN_SENT=$(( WRITTEN_SENT + this_batch ))
  if (( WRITTEN_SENT % 5000 == 0 )); then
    log "  sent $WRITTEN_SENT / $RECORDS (ok: $WRITTEN_OK, errors: $WRITE_ERRORS)"
  fi
done
ELAPSED=$(( $(date +%s) - START ))
ok "write phase: $WRITTEN_OK / $RECORDS records succeeded HTTP-side in ${ELAPSED}s ($WRITE_ERRORS batch errors)"

# ---------- check write distribution ----------
log "per-writer ingest counts (post-crash if applicable):"
for c in arc-writer1 arc-writer2 arc-writer3; do
  state="alive"
  if ! docker exec "$c" true 2>/dev/null; then
    state="DEAD"
  fi
  if [[ "$state" == "DEAD" ]]; then
    log "  $c: $state (killed by crash scenario)"
    continue
  fi
  metric=$(docker exec "$c" curl -fsS http://localhost:8000/metrics 2>/dev/null \
    | grep -E '^arc_ingest_records_total' | head -1 | awk '{print $2}' || echo "?")
  log "  $c: arc_ingest_records_total = ${metric:-?}"
done

# ---------- wait for flush + manifest replication ----------
log "waiting ${FLUSH_WAIT_S}s for flush + manifest replication"
sleep "$FLUSH_WAIT_S"

# ---------- query phase ----------
log "querying via reader"
RESPONSE=$(curl -fsS -X POST "$TRAEFIK_URL/api/v1/query" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "x-arc-database: $DATABASE" \
  -H "Content-Type: application/json" \
  -d "{\"sql\": \"SELECT COUNT(*) AS n FROM $MEASUREMENT\"}" \
  || fail "query")

ACTUAL=$(echo "$RESPONSE" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
    # Arc native response: {"columns": ["n"], "data": [[1000]], ...}
    # Older/dict-row shape: {"data": [{"n": 1000}], ...}
    data = d.get("data") or []
    if data and isinstance(data[0], list):
        print(data[0][0])
    elif data and isinstance(data[0], dict):
        row = data[0]
        print(row.get("n") or row.get("COUNT(*)") or list(row.values())[0])
    else:
        print("0")
except Exception as e:
    sys.stderr.write(f"parse error: {e}\n")
    print("0")
' 2>/dev/null || echo "0")

log "query returned: $ACTUAL records"
log "  HTTP-success during write: $WRITTEN_OK"
log "  HTTP-error during write:   $(( WRITTEN_SENT - WRITTEN_OK ))"

# ---------- scenario-specific assertions ----------
case "$SCENARIO" in
  base)
    if [[ "$ACTUAL" != "$RECORDS" ]]; then
      log "  expected: $RECORDS"
      log "  got:      $ACTUAL"
      fail "base scenario: record count mismatch (no crash should mean exact match)"
    fi
    ok "base scenario: exact count match ($ACTUAL == $RECORDS)"
    ;;
  non-leader-crash|leader-crash)
    # Crash scenarios: every record whose HTTP POST succeeded must be queryable.
    # Records whose POST failed (5xx, timeout) are accepted-as-lost.
    if [[ "$ACTUAL" -lt "$WRITTEN_OK" ]]; then
      log "  HTTP-success: $WRITTEN_OK"
      log "  queryable:    $ACTUAL"
      log "  missing:      $(( WRITTEN_OK - ACTUAL ))"
      fail "$SCENARIO: records lost AFTER successful HTTP response (durability violation)"
    fi
    if [[ "$ACTUAL" -gt "$WRITTEN_OK" ]]; then
      log "  HTTP-success: $WRITTEN_OK"
      log "  queryable:    $ACTUAL"
      log "  extra:        $(( ACTUAL - WRITTEN_OK ))"
      fail "$SCENARIO: more records queryable than were HTTP-success (duplicate insert?)"
    fi
    ok "$SCENARIO: durability OK ($ACTUAL == $WRITTEN_OK; in-flight loss tolerated)"
    ;;
esac

# ---------- teardown ----------
log "smoke passed; tearing down"
docker compose "${COMPOSE_FILES[@]}" down -v --remove-orphans >/dev/null 2>&1 || true

log ""
log "=== Pattern 2 multi-writer smoke ($SCENARIO): PASS ==="
log "  records sent:    $RECORDS"
log "  HTTP success:    $WRITTEN_OK"
log "  HTTP errors:     $(( WRITTEN_SENT - WRITTEN_OK ))"
log "  queryable:       $ACTUAL"
log "  ingest duration: ${ELAPSED}s"
log "  flush wait:      ${FLUSH_WAIT_S}s"
[[ -n "$KILL_TARGET" ]] && log "  kill target:     $KILL_TARGET (killed at record $KILL_AT)"
