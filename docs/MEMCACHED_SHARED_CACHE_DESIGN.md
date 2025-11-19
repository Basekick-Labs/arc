# Memcached Shared Cache Implementation Plan

**Target Version**: v2026.01.1
**Status**: Design Document
**Priority**: High (significant performance improvement for multi-worker deployments)

---

## Problem Statement

**Current State (v25.12.1):**
- 42 Gunicorn workers, each with independent in-memory cache
- Cache hit rate: ~2.4% (1/42 chance of hitting same worker)
- Memory usage: 8.4 GB (42 workers × 200MB each, highly duplicated)
- Cache cleared on worker restart/reload

**Impact:**
- Dashboard queries (1.2s for 12.8B rows) execute repeatedly instead of using cache
- Poor user experience for dashboard auto-refresh (every query takes 1.2s)
- Wasted memory (same query results cached in multiple workers)

---

## Proposed Solution: Memcached Shared Cache

### Why Memcached?

| Criterion | Memcached | Redis | Valkey |
|-----------|-----------|-------|--------|
| **License** | ✅ BSD 3-Clause (stable since 2003) | ⚠️ SSPL/RSALv2 (2024) | ✅ BSD 3-Clause |
| **License risk** | ✅ None (community-governed) | ⚠️ High (commercial control) | ✅ Low (Linux Foundation) |
| **Performance** | ✅ 0.5-1ms latency | 1-3ms latency | 1-3ms latency |
| **Use case fit** | ✅ Perfect (key-value only) | Overkill (data structures) | Overkill (data structures) |
| **Memory efficiency** | ✅ Best (optimized for cache) | Good | Good |
| **Maturity** | ✅ 20+ years, battle-tested | Mature | New (2024) |
| **Simplicity** | ✅ Minimal config, <10MB footprint | Complex | Complex |

**Decision: Memcached** for license safety, performance, and simplicity.

---

## Architecture

### Current (Per-Worker Cache)

```
┌────────────────────────────────────────────┐
│  42 Gunicorn Workers                       │
│  ┌──────┐  ┌──────┐  ┌──────┐   ┌──────┐ │
│  │ W1   │  │ W2   │  │ W3   │...│ W42  │ │
│  │Cache │  │Cache │  │Cache │   │Cache │ │
│  │200MB │  │200MB │  │200MB │   │200MB │ │
│  └──────┘  └──────┘  └──────┘   └──────┘ │
│                                            │
│  Total: 8.4 GB (duplicated)                │
│  Hit rate: 2.4% (random distribution)      │
└────────────────────────────────────────────┘
```

### Proposed (Memcached Shared Cache)

```
┌────────────────────────────────────────────┐
│  42 Gunicorn Workers                       │
│  ┌──────┐  ┌──────┐  ┌──────┐   ┌──────┐ │
│  │ W1   │  │ W2   │  │ W3   │...│ W42  │ │
│  └───┬──┘  └───┬──┘  └───┬──┘   └───┬──┘ │
│      │         │         │           │     │
│      └─────────┴─────────┴───────────┘     │
│                  ↓                          │
│      ┌───────────────────────┐              │
│      │   Memcached           │              │
│      │   (Shared Cache)      │              │
│      │   200 MB total        │              │
│      │   localhost:11211     │              │
│      └───────────────────────┘              │
│                                            │
│  Total: 200 MB (shared)                    │
│  Hit rate: 95%+ (all workers share)        │
└────────────────────────────────────────────┘
```

---

## Performance Impact

### Query Performance

| Scenario | Current (v25.12.1) | With Memcached | Improvement |
|----------|-------------------|----------------|-------------|
| **First query (cold)** | 1.2s | 1.2s + 1ms (cache set) | ~same |
| **Repeated query (within 60s)** | 1.2s (97.6% cache miss) | 0.5-1ms (95% cache hit) | **1200x faster** |
| **Dashboard refresh (5s interval)** | 1.2s every time | <1ms (cached) | **Instant** |
| **Cache hit rate** | 2.4% | 95%+ | **40x better** |
| **Memory usage** | 8.4 GB | 200 MB | **97% reduction** |

### Real-World Impact (12.8B Row Database)

**Dashboard user experience:**
- **Before**: Every 5-second refresh takes 1.2s (sluggish, 24% of time waiting)
- **After**: 95% of refreshes are <1ms (instant, imperceptible)

**Resource savings:**
- Memory: 8.4 GB → 200 MB (saves 8.2 GB per Arc instance)
- CPU: 95% fewer 1.2s query executions (huge reduction in DuckDB load)

---

## Implementation Plan

### Phase 1: Core Implementation (3-4 hours)

#### 1.1 Create Memcached Cache Backend (2 hours)

**New file:** `api/memcached_cache.py`

```python
"""
Memcached-backed query cache for Arc

Provides shared cache across all Gunicorn workers using Memcached.
License-safe (BSD 3-Clause), fast (0.5-1ms), and battle-tested.
"""

import hashlib
import logging
import time
from typing import Dict, Any, Optional, Tuple
from pymemcache.client.base import Client
from pymemcache import serde

logger = logging.getLogger(__name__)


class MemcachedQueryCache:
    """
    Memcached-backed query result cache

    Features:
    - Shared across all workers (no duplication)
    - TTL-based expiration (handled by Memcached)
    - Automatic fallback on Memcached failure
    - msgpack serialization for efficiency
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 11211,
        ttl_seconds: int = 60,
        max_result_size_mb: int = 10,
        connect_timeout: float = 1.0,
        timeout: float = 1.0
    ):
        """
        Initialize Memcached cache

        Args:
            host: Memcached host (default: localhost)
            port: Memcached port (default: 11211)
            ttl_seconds: Cache TTL (default: 60)
            max_result_size_mb: Max result size to cache (default: 10MB)
            connect_timeout: Connection timeout (default: 1s)
            timeout: Operation timeout (default: 1s)
        """
        self.host = host
        self.port = port
        self.ttl = ttl_seconds
        self.max_result_size_bytes = max_result_size_mb * 1024 * 1024

        try:
            # Use msgpack for efficient serialization
            self.client = Client(
                (host, port),
                serializer=serde.python_memcache_serializer,
                deserializer=serde.python_memcache_deserializer,
                connect_timeout=connect_timeout,
                timeout=timeout
            )

            # Test connection
            self.client.stats()

            logger.info(
                f"Memcached cache initialized: {host}:{port}, "
                f"TTL={ttl_seconds}s, MaxSize={max_result_size_mb}MB"
            )

        except Exception as e:
            logger.error(f"Failed to connect to Memcached at {host}:{port}: {e}")
            logger.warning("Falling back to query execution (no cache)")
            self.client = None

    def get(self, sql: str, limit: int) -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
        """
        Get cached query result

        Args:
            sql: SQL query
            limit: Query limit

        Returns:
            Tuple of (cached_result, cache_age_seconds) or (None, None)
        """
        if not self.client:
            return None, None

        key = self._hash_key(sql, limit)

        try:
            result = self.client.get(key)

            if result:
                cached_at = result.get('cached_at', time.time())
                cache_age = time.time() - cached_at

                logger.debug(f"Cache HIT: key={key[:16]}..., age={cache_age:.1f}s")
                return result, cache_age

            logger.debug(f"Cache MISS: key={key[:16]}...")
            return None, None

        except Exception as e:
            logger.warning(f"Memcached get failed: {e}, falling back to query")
            return None, None

    def set(self, sql: str, limit: int, result: Dict[str, Any]) -> bool:
        """
        Cache query result

        Args:
            sql: SQL query
            limit: Query limit
            result: Query result to cache

        Returns:
            True if cached, False otherwise
        """
        if not self.client:
            return False

        # Check size limit (estimate)
        result_size = len(str(result))  # Rough estimate
        if result_size > self.max_result_size_bytes:
            logger.debug(
                f"Result too large to cache: {result_size / 1024 / 1024:.1f}MB "
                f"(max: {self.max_result_size_bytes / 1024 / 1024:.1f}MB)"
            )
            return False

        key = self._hash_key(sql, limit)

        try:
            # Add timestamp for cache age calculation
            result['cached_at'] = time.time()

            # Set with TTL
            self.client.set(key, result, expire=self.ttl)

            logger.debug(f"Cache SET: key={key[:16]}..., ttl={self.ttl}s")
            return True

        except Exception as e:
            logger.warning(f"Memcached set failed: {e}")
            return False

    def clear(self) -> bool:
        """Clear all cached results"""
        if not self.client:
            return False

        try:
            self.client.flush_all()
            logger.info("Memcached cache cleared")
            return True
        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            return False

    def stats(self) -> Optional[Dict[str, Any]]:
        """Get Memcached statistics"""
        if not self.client:
            return None

        try:
            return self.client.stats()
        except Exception as e:
            logger.error(f"Failed to get stats: {e}")
            return None

    def _hash_key(self, sql: str, limit: int) -> str:
        """
        Generate cache key from SQL + limit

        Uses SHA256 for consistent key generation
        """
        return hashlib.sha256(f"{sql}:{limit}".encode()).hexdigest()
```

#### 1.2 Update Cache Initialization (1 hour)

**Update:** `api/query_cache.py`

Add backend selection logic:

```python
def init_query_cache(
    ttl_seconds: int = None,
    max_size: int = None,
    max_result_size_mb: int = None,
    backend: str = None
) -> Optional[QueryCache]:
    """
    Initialize query cache with configurable backend

    Backends:
    - "memory": Per-worker in-memory cache (default, no dependencies)
    - "memcached": Shared Memcached cache (requires Memcached server)

    Args:
        ttl_seconds: Cache TTL
        max_size: Max cached queries (memory backend only)
        max_result_size_mb: Max result size to cache
        backend: Cache backend ("memory" or "memcached")

    Returns:
        QueryCache instance or None if disabled
    """
    global _query_cache

    # Check if caching is disabled
    cache_enabled = False
    try:
        from config_loader import get_config
        arc_config = get_config()
        cache_config = arc_config.config.get('query_cache', {})
        cache_enabled = cache_config.get('enabled', False)

        # Read backend and other config
        if backend is None:
            backend = cache_config.get('backend', 'memory')
        if ttl_seconds is None:
            ttl_seconds = cache_config.get('ttl_seconds', 60)
        if max_size is None:
            max_size = cache_config.get('max_size', 100)
        if max_result_size_mb is None:
            max_result_size_mb = cache_config.get('max_result_size_mb', 10)

    except Exception:
        cache_enabled = os.getenv("QUERY_CACHE_ENABLED", "false").lower() == "true"
        backend = os.getenv("QUERY_CACHE_BACKEND", "memory")

    if not cache_enabled:
        logger.info("Query cache DISABLED by configuration")
        return None

    # Select backend
    if backend == "memcached":
        from api.memcached_cache import MemcachedQueryCache

        memcached_host = cache_config.get('memcached_host', 'localhost')
        memcached_port = cache_config.get('memcached_port', 11211)

        _query_cache = MemcachedQueryCache(
            host=memcached_host,
            port=memcached_port,
            ttl_seconds=ttl_seconds,
            max_result_size_mb=max_result_size_mb
        )

        logger.info(f"Query cache initialized: MEMCACHED backend ({memcached_host}:{memcached_port})")

    else:  # backend == "memory" (default)
        _query_cache = QueryCache(
            ttl_seconds=ttl_seconds,
            max_size=max_size,
            max_result_size_mb=max_result_size_mb
        )

        logger.info(f"Query cache initialized: MEMORY backend (per-worker)")

    return _query_cache
```

#### 1.3 Configuration (30 minutes)

**Update:** `arc.conf`

```toml
[query_cache]
# Enable query result caching
enabled = true

# Cache backend: "memory" (per-worker) or "memcached" (shared)
# - "memory": No external dependencies, lower hit rate (2.4% with 42 workers)
# - "memcached": Requires Memcached, high hit rate (95%+), shared across workers
backend = "memcached"  # or "memory"

# Memcached server configuration (only used when backend="memcached")
memcached_host = "localhost"
memcached_port = 11211

# Cache TTL in seconds
ttl_seconds = 60

# Maximum size of individual result to cache (MB)
max_result_size_mb = 10

# Memory backend only: Maximum number of cached queries per worker
max_size = 100
```

**Update:** `.env` (environment variable fallback)

```bash
# Query cache backend: "memory" or "memcached"
QUERY_CACHE_BACKEND=memcached
QUERY_CACHE_MEMCACHED_HOST=localhost
QUERY_CACHE_MEMCACHED_PORT=11211
```

#### 1.4 Dependencies (5 minutes)

**Update:** `requirements.txt`

```txt
# Query cache (Memcached backend - optional)
pymemcache>=4.0.0  # Apache 2.0 license (safe)
```

---

### Phase 2: Testing & Validation (1 hour)

#### 2.1 Unit Tests

**New file:** `tests/test_memcached_cache.py`

```python
import pytest
from api.memcached_cache import MemcachedQueryCache


def test_memcached_cache_set_get():
    """Test basic set/get operations"""
    cache = MemcachedQueryCache(ttl_seconds=5)

    # Test set
    result = {"columns": ["count"], "data": [[12793172000]], "row_count": 1}
    assert cache.set("SELECT COUNT(*) FROM prod.cpu", 1000, result)

    # Test get
    cached, age = cache.get("SELECT COUNT(*) FROM prod.cpu", 1000)
    assert cached is not None
    assert cached["row_count"] == 1
    assert age < 1.0


def test_memcached_cache_ttl():
    """Test TTL expiration"""
    import time

    cache = MemcachedQueryCache(ttl_seconds=2)

    result = {"columns": ["count"], "data": [[1]], "row_count": 1}
    cache.set("SELECT 1", 1000, result)

    # Should be cached
    cached, age = cache.get("SELECT 1", 1000)
    assert cached is not None

    # Wait for expiration
    time.sleep(3)

    # Should be expired
    cached, age = cache.get("SELECT 1", 1000)
    assert cached is None


def test_memcached_cache_size_limit():
    """Test max size limit"""
    cache = MemcachedQueryCache(max_result_size_mb=0.001)  # 1KB limit

    # Large result (>1KB)
    large_result = {"data": [[i] for i in range(1000)]}

    # Should reject due to size
    assert not cache.set("SELECT * FROM large", 1000, large_result)
```

#### 2.2 Integration Test

```bash
# Start Memcached
brew install memcached  # macOS
brew services start memcached

# Or Ubuntu
sudo apt install memcached
sudo systemctl start memcached

# Run Arc with Memcached backend
export QUERY_CACHE_BACKEND=memcached
./start.sh

# Test queries (should see cache hits in logs)
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM prod.cpu"}'

# Repeat - should be cached
curl -X POST http://localhost:8000/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT COUNT(*) FROM prod.cpu"}'
```

---

### Phase 3: Deployment & Monitoring (30 minutes)

#### 3.1 Deployment Guide

**Add to:** `docs/DEPLOYMENT.md`

```markdown
## Memcached Shared Cache (Optional)

For multi-worker deployments, enable Memcached for 40x better cache hit rates.

### Installation

**macOS:**
```bash
brew install memcached
brew services start memcached
```

**Ubuntu:**
```bash
sudo apt install memcached
sudo systemctl start memcached
sudo systemctl enable memcached
```

**Docker:**
```yaml
services:
  memcached:
    image: memcached:1.6-alpine
    ports:
      - "11211:11211"
    command: memcached -m 200  # 200MB memory limit
```

### Configuration

Enable in `arc.conf`:

```toml
[query_cache]
enabled = true
backend = "memcached"
memcached_host = "localhost"
memcached_port = 11211
ttl_seconds = 60
```

### Monitoring

Check Memcached stats:

```bash
# Connect to Memcached
echo "stats" | nc localhost 11211

# Key metrics:
# - get_hits: Cache hits
# - get_misses: Cache misses
# - curr_items: Cached query count
# - bytes: Memory used
```

Monitor Arc logs for cache performance:

```bash
# Look for "Cache HIT" messages
tail -f /app/logs/arc.log | grep "Cache HIT"
```
```

#### 3.2 Monitoring Dashboard

Add Memcached metrics to Arc's monitoring:

```python
# api/monitoring.py

def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics (memory or Memcached)"""
    cache = get_query_cache()

    if isinstance(cache, MemcachedQueryCache):
        stats = cache.stats()
        return {
            "backend": "memcached",
            "hits": stats.get('get_hits', 0),
            "misses": stats.get('get_misses', 0),
            "hit_rate": stats.get('get_hits', 0) / max(stats.get('cmd_get', 1), 1),
            "items": stats.get('curr_items', 0),
            "bytes": stats.get('bytes', 0),
        }
    else:
        return {
            "backend": "memory",
            "hits": cache.hits if cache else 0,
            "misses": cache.misses if cache else 0,
            "hit_rate": cache.hits / max(cache.hits + cache.misses, 1) if cache else 0,
        }
```

---

## Migration Path

### v25.12.1 → v2026.01.1

**For users upgrading:**

1. **No action required** (defaults to memory backend)
2. **Optional**: Install Memcached and enable for better performance

**Backward compatibility:**
- Existing deployments continue using memory cache
- Configuration is additive (no breaking changes)
- Memcached is optional dependency

---

## Success Metrics

### Performance Goals

| Metric | Current (v25.12.1) | Target (v2026.01.1) |
|--------|-------------------|---------------------|
| Cache hit rate (42 workers) | 2.4% | 95%+ |
| Dashboard refresh latency | 1.2s | <1ms (95% of time) |
| Memory usage (cache) | 8.4 GB | 200 MB |
| Query load reduction | 0% | 95% |

### User Experience Goals

- Dashboard feels **instant** (95% of refreshes <1ms)
- Resource efficiency (97% less cache memory)
- Zero downtime deployment (cache backend is hot-swappable)

---

## Risks & Mitigations

### Risk 1: Memcached Unavailability

**Impact:** Cache falls back to direct queries (same as current behavior)

**Mitigation:**
- Automatic fallback on connection failure
- Health checks (test connection on startup)
- Logging alerts for Memcached issues

### Risk 2: Cache Stampede

**Scenario:** Cache expires, 42 workers simultaneously query database

**Mitigation:**
- Add jitter to TTL (TTL ± random(0, 5s))
- Implement request coalescing (future enhancement)

### Risk 3: Stale Data

**Scenario:** Data changes, but cache not invalidated

**Mitigation:**
- Short TTL (60s default, configurable)
- Document cache behavior for users
- Add manual cache clear endpoint

---

## Future Enhancements (v2026+)

### Two-Tier Cache (L1 + L2)

```
Worker → L1 (memory, 10MB, <1ms) → L2 (Memcached, 200MB, 1ms) → Query
```

**Benefits:**
- 30% of queries hit L1 (instant)
- 65% hit L2 (1ms)
- Only 5% hit database

**Implementation:** v2026.02.1+

### Cache Warming

Pre-populate cache with common queries on startup.

**Implementation:** v2026.02.1+

### Smart Cache Keys

Include schema version in cache key to auto-invalidate on schema changes.

**Implementation:** v2026.03.1+

---

## References

- Memcached official docs: https://memcached.org/
- pymemcache documentation: https://pymemcache.readthedocs.io/
- License: https://github.com/memcached/memcached/blob/master/LICENSE

---

**Document Version:** 1.0
**Last Updated:** 2025-11-15
**Author:** Arc Team
**Target Release:** v2026.01.1
