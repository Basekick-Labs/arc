#!/usr/bin/env bash
# =============================================================================
# Decimal128 Manual Test Script
#
# Tests the full pipeline: ingest (msgpack row + LP) → flush → query via DuckDB
# Verifies exact decimal precision is preserved through Parquet DECIMAL type.
#
# Prerequisites:
#   pip3 install msgpack
#   make build  (with -tags=duckdb_arrow)
#
# Usage:
#   ./scripts/test-decimal128.sh
# =============================================================================
set -euo pipefail

ARC_PORT="${ARC_PORT:-8099}"
ARC_URL="http://localhost:${ARC_PORT}"
ARC_DIR=$(mktemp -d)
ARC_BIN="./arc"
PASS=0
FAIL=0

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    if [[ -n "${ARC_PID:-}" ]] && kill -0 "$ARC_PID" 2>/dev/null; then
        kill "$ARC_PID" 2>/dev/null || true
        wait "$ARC_PID" 2>/dev/null || true
    fi
    rm -rf "$ARC_DIR"
    echo "Results: ${PASS} passed, ${FAIL} failed"
    if [[ $FAIL -gt 0 ]]; then exit 1; fi
}
trap cleanup EXIT

check() {
    local desc="$1" expected="$2" actual="$3"
    if [[ "$actual" == *"$expected"* ]]; then
        echo "  ✅ $desc"
        PASS=$((PASS + 1))
    else
        echo "  ❌ $desc"
        echo "     expected: $expected"
        echo "     actual:   $actual"
        FAIL=$((FAIL + 1))
    fi
}

# ---- Build ----
echo "=== Building Arc ==="
if [[ ! -f "$ARC_BIN" ]]; then
    go build -tags=duckdb_arrow -o "$ARC_BIN" ./cmd/arc/
fi
echo "Binary: $ARC_BIN"

# ---- Start Arc with decimal config ----
echo ""
echo "=== Starting Arc (port ${ARC_PORT}, data dir ${ARC_DIR}) ==="
ARC_SERVER_PORT="$ARC_PORT" \
ARC_STORAGE_LOCAL_PATH="$ARC_DIR" \
ARC_AUTH_ENABLED=false \
ARC_LOG_LEVEL=warn \
ARC_INGEST_MAX_BUFFER_AGE_MS=500 \
ARC_INGEST_MAX_BUFFER_SIZE=100 \
ARC_INGEST_DECIMAL_COLUMNS="trades:price=18,8;amount=18,8 balances:balance=38,18" \
ARC_INGEST_DEFAULT_DECIMAL_COLUMNS="value=18,6" \
"$ARC_BIN" &
ARC_PID=$!

# Wait for health
for i in $(seq 1 30); do
    if curl -s "${ARC_URL}/health" | grep -q "ok"; then
        echo "Arc is healthy (PID $ARC_PID)"
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "Arc failed to start"
        exit 1
    fi
    sleep 0.2
done

# ---- Test 1: Msgpack row format with float64 values (decimal conversion) ----
echo ""
echo "=== Test 1: Msgpack row format — float64 → Decimal128 ==="
python3 -c "
import msgpack, urllib.request, time

now = int(time.time() * 1_000_000)
for i in range(10):
    payload = {
        'm': 'trades',
        't': now + i,
        'fields': {
            'price': 123.45678901,     # float64 — will be converted to decimal128(18,8)
            'amount': 999.12345678,
            'volume': 42,              # non-decimal column — stays int
        },
        'tags': {
            'symbol': 'BTC-USD',
            'exchange': 'coinbase',
        }
    }
    data = msgpack.packb(payload)
    req = urllib.request.Request(
        '${ARC_URL}/api/v1/write/msgpack',
        data=data,
        headers={
            'Content-Type': 'application/msgpack',
            'x-arc-database': 'testdb',
        }
    )
    resp = urllib.request.urlopen(req)
    assert resp.status == 204, f'Expected 204, got {resp.status}'

print('Sent 10 rows with float64 decimal values')
"
echo "  Ingested via msgpack row format"

# ---- Test 2: Msgpack row format with string values (exact precision) ----
echo ""
echo "=== Test 2: Msgpack row format — string → Decimal128 (exact) ==="
python3 -c "
import msgpack, urllib.request, time

now = int(time.time() * 1_000_000) + 100
for i in range(5):
    payload = {
        'm': 'trades',
        't': now + i,
        'fields': {
            'price': '99999.12345678',     # string — exact decimal128 conversion
            'amount': '0.00000001',         # smallest representable at scale 8
            'volume': 100,
        },
        'tags': {
            'symbol': 'ETH-USD',
            'exchange': 'kraken',
        }
    }
    data = msgpack.packb(payload)
    req = urllib.request.Request(
        '${ARC_URL}/api/v1/write/msgpack',
        data=data,
        headers={
            'Content-Type': 'application/msgpack',
            'x-arc-database': 'testdb',
        }
    )
    resp = urllib.request.urlopen(req)
    assert resp.status == 204, f'Expected 204, got {resp.status}'

print('Sent 5 rows with string decimal values')
"
echo "  Ingested via msgpack row format (string precision)"

# ---- Test 3: Line Protocol with decimal values (separate measurement) ----
echo ""
echo "=== Test 3: Line Protocol — float → Decimal128 ==="
LP_DATA=""
NOW_NS=$(($(date +%s) * 1000000000))
for i in $(seq 1 5); do
    LP_DATA+="balances,account=acc01,currency=USD balance=99999.123456789012345678 ${NOW_NS}
"
    NOW_NS=$((NOW_NS + 1000))
done
curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${ARC_URL}/write?db=testdb" \
    -d "$LP_DATA" | grep -q "204"
echo "  Ingested 5 rows via line protocol (balances measurement)"

# ---- Test 4: Default decimal columns (measurement without explicit config) ----
echo ""
echo "=== Test 4: Default decimal columns ==="
python3 -c "
import msgpack, urllib.request, time

now = int(time.time() * 1_000_000) + 200
for i in range(5):
    payload = {
        'm': 'sensors',
        't': now + i,
        'fields': {
            'value': '3.141593',     # should use default decimal config (18,6)
            'status': 'ok',
        },
        'tags': {'device': 'temp-01'}
    }
    data = msgpack.packb(payload)
    req = urllib.request.Request(
        '${ARC_URL}/api/v1/write/msgpack',
        data=data,
        headers={
            'Content-Type': 'application/msgpack',
            'x-arc-database': 'testdb',
        }
    )
    resp = urllib.request.urlopen(req)
    assert resp.status == 204, f'Expected 204, got {resp.status}'

print('Sent 5 rows to sensors measurement (default decimal config)')
"
echo "  Ingested via msgpack with default decimal columns"

# ---- Wait for flush ----
echo ""
echo "=== Waiting for buffer flush (2s) ==="
sleep 2

# ---- Test 5: Query and verify decimal precision ----
echo ""
echo "=== Test 5: Query — verify decimal precision preserved ==="

# Query trades
RESULT=$(curl -s -X POST "${ARC_URL}/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-arc-database: testdb" \
    -d '{"sql": "SELECT price, amount, typeof(price) as price_type, typeof(amount) as amount_type FROM trades LIMIT 5"}')

echo "  Query result (trades):"
echo "  $RESULT" | python3 -m json.tool 2>/dev/null || echo "  $RESULT"

# Check that DuckDB reports DECIMAL type
check "price column is DECIMAL type" "DECIMAL" "$RESULT"
check "amount column is DECIMAL type" "DECIMAL" "$RESULT"

# ---- Test 6: Verify string-precision values are exact ----
echo ""
echo "=== Test 6: Query — verify exact string-to-decimal values ==="
RESULT2=$(curl -s -X POST "${ARC_URL}/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-arc-database: testdb" \
    -d '{"sql": "SELECT price, amount FROM trades WHERE symbol = '\''ETH-USD'\'' LIMIT 1"}')

echo "  Query result (ETH-USD):"
echo "  $RESULT2" | python3 -m json.tool 2>/dev/null || echo "  $RESULT2"

check "string price preserved" "99999.12345678" "$RESULT2"
# DuckDB may serialize 0.00000001 as "1e-08" — both represent the same value
check "smallest amount preserved" "1e-08" "$RESULT2"

# ---- Test 7: Verify default decimal columns ----
echo ""
echo "=== Test 7: Query — verify default decimal columns (sensors) ==="
RESULT3=$(curl -s -X POST "${ARC_URL}/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-arc-database: testdb" \
    -d '{"sql": "SELECT value, typeof(value) as value_type FROM sensors LIMIT 1"}')

echo "  Query result (sensors):"
echo "  $RESULT3" | python3 -m json.tool 2>/dev/null || echo "  $RESULT3"

check "default decimal column is DECIMAL type" "DECIMAL" "$RESULT3"

# ---- Test 8: Verify non-decimal columns are unaffected ----
echo ""
echo "=== Test 8: Query — non-decimal columns unaffected ==="
RESULT4=$(curl -s -X POST "${ARC_URL}/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-arc-database: testdb" \
    -d '{"sql": "SELECT volume, typeof(volume) as vol_type FROM trades LIMIT 1"}')

echo "  Query result (volume):"
echo "  $RESULT4" | python3 -m json.tool 2>/dev/null || echo "  $RESULT4"

check "volume column is integer type" "BIGINT" "$RESULT4"

# ---- Test 8b: Verify LP decimal (balances measurement) ----
echo ""
echo "=== Test 8b: Query — LP balances decimal ==="
RESULT4B=$(curl -s -X POST "${ARC_URL}/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-arc-database: testdb" \
    -d '{"sql": "SELECT balance, typeof(balance) as bal_type FROM balances LIMIT 1"}')

echo "  Query result (balances):"
echo "  $RESULT4B" | python3 -m json.tool 2>/dev/null || echo "  $RESULT4B"

check "LP balance column is DECIMAL type" "DECIMAL" "$RESULT4B"

# ---- Test 9: Decimal aggregation works ----
echo ""
echo "=== Test 9: Query — decimal aggregation ==="
RESULT5=$(curl -s -X POST "${ARC_URL}/api/v1/query" \
    -H "Content-Type: application/json" \
    -H "x-arc-database: testdb" \
    -d '{"sql": "SELECT symbol, AVG(price) as avg_price, SUM(amount) as total_amount, COUNT(*) as cnt FROM trades GROUP BY symbol ORDER BY symbol"}')

echo "  Aggregation result:"
echo "  $RESULT5" | python3 -m json.tool 2>/dev/null || echo "  $RESULT5"

check "aggregation returns results" "avg_price" "$RESULT5"

echo ""
echo "=== All tests complete ==="
