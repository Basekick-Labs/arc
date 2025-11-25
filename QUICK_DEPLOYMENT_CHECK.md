# Quick Deployment Check - Arc Connection Pool

## ✅ Pre-Deployment Checklist

Before deploying Arc to any environment:

```bash
# 1. Verify correct files exist
[ -f api/duckdb_pool.py ] && echo "✅ Pool file exists" || echo "❌ MISSING"
[ -f api/duckdb_engine.py ] && echo "✅ Engine file exists" || echo "❌ MISSING"

# 2. Verify broken file does NOT exist
[ ! -f api/duckdb_pool_simple.py ] && echo "✅ No broken pool" || echo "❌ REMOVE THIS FILE"

# 3. Check import is correct
grep -q "from api.duckdb_pool import DuckDBConnectionPool" api/duckdb_engine.py && \
  echo "✅ Correct import" || echo "❌ Wrong import"

# 4. Check git commit includes fix
git log --oneline | grep -q "a4ef316" && \
  echo "✅ Has deadlock fix" || echo "⚠️  Missing fix commit"
```

## 🚀 Deploy Command

```bash
# Always rebuild without cache to avoid orphaned files
docker-compose build --no-cache
docker-compose up -d
```

## 🧪 Post-Deployment Test

```bash
# Replace with your URL and token
DOMAIN="arc.ps.customers.basekick.net"
TOKEN="YOUR_TOKEN_HERE"

# Test 1: Simple query
curl -X POST https://$DOMAIN/api/v1/query \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1 as test", "format": "json"}'

# Should return: {"success":true,...}

# Test 2: Concurrent load (10 queries)
for i in {1..10}; do
  curl -s -X POST https://$DOMAIN/api/v1/query \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"sql\": \"SELECT $i as test\", \"format\": \"json\"}" &
done
wait

# All should succeed with {"success":true,...}
```

## 🚨 If Tests Fail

### Error: "No available connections after 5.0s"

Container still has broken pool. Fix immediately:

```bash
CONTAINER_ID=$(docker ps | grep arc | awk '{print $1}')

# Remove broken files
docker exec $CONTAINER_ID rm -f /app/api/duckdb_pool_simple.py
docker exec $CONTAINER_ID rm -rf /app/api/__pycache__

# Copy correct files
docker cp api/duckdb_pool.py $CONTAINER_ID:/app/api/
docker cp api/duckdb_engine.py $CONTAINER_ID:/app/api/

# Restart
docker restart $CONTAINER_ID
```

### Error: "ImportError: cannot import name 'SimpleDuckDBPool'"

Engine file has wrong import:

```bash
# Check what's imported
docker exec $CONTAINER_ID grep "import.*Pool" /app/api/duckdb_engine.py

# Should show: from api.duckdb_pool import DuckDBConnectionPool, QueryPriority
# If not, rebuild container from clean git checkout
```

## 📊 Monitor Pool Health

```bash
# Check logs for pool-related messages
docker logs CONTAINER_ID 2>&1 | grep -i "pool\|connection"

# Look for:
# ✅ "DuckDB connection pool initialized: 5 connections"
# ✅ "Connection pool closed"
# ❌ "Connection pool exhausted"
# ❌ "Connection cleanup failed"
```

## 📋 Key Files Reference

| File | Status | Purpose |
|------|--------|---------|
| `api/duckdb_pool.py` | ✅ Required | Correct connection pool implementation |
| `api/duckdb_engine.py` | ✅ Required | Query engine (must import correct pool) |
| `api/duckdb_pool_simple.py` | ❌ Must NOT exist | Broken pool that causes exhaustion |

## 🔗 Full Documentation

See [`DEPLOYMENT_FIX_SUMMARY.md`](./DEPLOYMENT_FIX_SUMMARY.md) for complete details.
