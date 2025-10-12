"""
Query Result Cache for Arc

Provides in-memory caching of query results with TTL expiration.
Significantly speeds up repeated queries and dashboard auto-refresh.

Features:
- TTL-based expiration (configurable, default 60s)
- Size-based eviction (LRU when cache is full)
- Cache hit/miss metrics
- Per-query cache control
- Thread-safe operations
"""

import hashlib
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple
from collections import OrderedDict
import os

logger = logging.getLogger(__name__)


class QueryCache:
    """Thread-safe query result cache with TTL and size limits"""

    def __init__(
        self,
        ttl_seconds: int = None,
        max_size: int = None,
        max_result_size_mb: int = None
    ):
        """
        Initialize query cache

        Args:
            ttl_seconds: Cache TTL in seconds (default: 60, from env QUERY_CACHE_TTL)
            max_size: Maximum number of cached queries (default: 100, from env QUERY_CACHE_MAX_SIZE)
            max_result_size_mb: Max size of individual result to cache in MB (default: 10)
        """
        self.ttl_seconds = ttl_seconds or int(os.getenv("QUERY_CACHE_TTL", "60"))
        self.max_size = max_size or int(os.getenv("QUERY_CACHE_MAX_SIZE", "100"))
        self.max_result_size_mb = max_result_size_mb or int(os.getenv("QUERY_CACHE_MAX_RESULT_MB", "10"))

        # Use OrderedDict for LRU eviction
        self.cache: OrderedDict[str, Tuple[Dict[str, Any], datetime]] = OrderedDict()
        self.lock = threading.RLock()

        # Metrics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expirations = 0

        logger.info(
            f"Query cache initialized: TTL={self.ttl_seconds}s, "
            f"MaxSize={self.max_size}, MaxResultSize={self.max_result_size_mb}MB"
        )

    def _make_key(self, sql: str, limit: int) -> str:
        """
        Create cache key from query SQL and limit

        Args:
            sql: SQL query string
            limit: Result limit

        Returns:
            MD5 hash of query signature
        """
        # Normalize SQL (strip whitespace, lowercase)
        normalized_sql = " ".join(sql.lower().split())
        key_data = f"{normalized_sql}:{limit}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def _estimate_size_mb(self, result: Dict[str, Any]) -> float:
        """
        Estimate result size in MB

        Args:
            result: Query result dict

        Returns:
            Estimated size in MB
        """
        try:
            # Rough estimate: count data elements
            data = result.get("data", [])
            columns = result.get("columns", [])

            if not data:
                return 0.0

            # Estimate: avg 100 bytes per cell
            num_cells = len(data) * len(columns)
            bytes_estimate = num_cells * 100
            mb_estimate = bytes_estimate / (1024 * 1024)

            return mb_estimate

        except Exception as e:
            logger.warning(f"Failed to estimate result size: {e}")
            return 0.0

    def get(self, sql: str, limit: int) -> Tuple[Optional[Dict[str, Any]], Optional[float]]:
        """
        Get cached result if available and not expired

        Args:
            sql: SQL query string
            limit: Result limit

        Returns:
            Tuple of (cached_result, cache_age_seconds) or (None, None) if miss
        """
        key = self._make_key(sql, limit)

        with self.lock:
            if key in self.cache:
                cached_data, cached_at = self.cache[key]
                age_seconds = (datetime.now() - cached_at).total_seconds()

                if age_seconds < self.ttl_seconds:
                    # Cache hit!
                    self.hits += 1

                    # Move to end (LRU)
                    self.cache.move_to_end(key)

                    logger.debug(
                        f"Cache HIT: age={age_seconds:.1f}s, "
                        f"rows={cached_data.get('row_count', 0)}, "
                        f"sql={sql[:50]}"
                    )

                    return cached_data, age_seconds
                else:
                    # Expired - remove it
                    del self.cache[key]
                    self.expirations += 1

                    logger.debug(
                        f"Cache EXPIRED: age={age_seconds:.1f}s, "
                        f"sql={sql[:50]}"
                    )

            # Cache miss
            self.misses += 1
            return None, None

    def set(self, sql: str, limit: int, result: Dict[str, Any]) -> bool:
        """
        Cache query result

        Args:
            sql: SQL query string
            limit: Result limit
            result: Query result dict

        Returns:
            True if cached, False if skipped (too large, etc.)
        """
        # Don't cache failed queries
        if not result.get("success", False):
            return False

        # Check result size
        size_mb = self._estimate_size_mb(result)
        if size_mb > self.max_result_size_mb:
            logger.debug(
                f"Result too large to cache: {size_mb:.1f}MB "
                f"(max {self.max_result_size_mb}MB), sql={sql[:50]}"
            )
            return False

        key = self._make_key(sql, limit)

        with self.lock:
            # Evict oldest if cache is full
            if len(self.cache) >= self.max_size and key not in self.cache:
                # Remove oldest (first item in OrderedDict)
                oldest_key, _ = self.cache.popitem(last=False)
                self.evictions += 1
                logger.debug(f"Cache EVICT: Removed oldest entry (full at {self.max_size})")

            # Store result
            self.cache[key] = (result, datetime.now())

            logger.debug(
                f"Cache SET: rows={result.get('row_count', 0)}, "
                f"size={size_mb:.2f}MB, sql={sql[:50]}"
            )

            return True

    def invalidate(self, pattern: str = None):
        """
        Invalidate cache entries

        Args:
            pattern: SQL pattern to match (None = clear all)
        """
        with self.lock:
            if pattern is None:
                # Clear all
                count = len(self.cache)
                self.cache.clear()
                logger.info(f"Cache cleared: {count} entries removed")
            else:
                # Clear matching pattern (TODO: implement pattern matching)
                pass

    def stats(self) -> Dict[str, Any]:
        """
        Get cache statistics

        Returns:
            Dict with cache metrics
        """
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0.0

            return {
                "enabled": True,
                "ttl_seconds": self.ttl_seconds,
                "max_size": self.max_size,
                "max_result_size_mb": self.max_result_size_mb,
                "current_size": len(self.cache),
                "utilization_percent": round(len(self.cache) / self.max_size * 100, 1),
                "metrics": {
                    "total_requests": total_requests,
                    "hits": self.hits,
                    "misses": self.misses,
                    "hit_rate_percent": round(hit_rate, 1),
                    "evictions": self.evictions,
                    "expirations": self.expirations
                },
                "entries": [
                    {
                        "key_hash": key[:16],
                        "age_seconds": round((datetime.now() - cached_at).total_seconds(), 1),
                        "row_count": data.get("row_count", 0),
                        "columns": len(data.get("columns", [])),
                        "cached_at": cached_at.isoformat()
                    }
                    for key, (data, cached_at) in list(self.cache.items())[:20]  # Show first 20
                ]
            }

    def health_check(self) -> Dict[str, Any]:
        """
        Health check for monitoring

        Returns:
            Dict with health status
        """
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = (self.hits / total_requests * 100) if total_requests > 0 else 0.0

            # Health conditions
            is_healthy = True
            warnings = []

            # Warn if hit rate is low (< 20%)
            if total_requests > 100 and hit_rate < 20:
                warnings.append(f"Low hit rate: {hit_rate:.1f}% (expected >20%)")
                is_healthy = False

            # Warn if cache is underutilized (< 10% with many requests)
            utilization = len(self.cache) / self.max_size * 100
            if total_requests > 1000 and utilization < 10:
                warnings.append(f"Low utilization: {utilization:.1f}% (cache may be too large)")

            # Warn if many evictions (cache too small)
            if self.evictions > self.hits * 0.5:
                warnings.append(f"High eviction rate: {self.evictions} evictions vs {self.hits} hits (cache may be too small)")
                is_healthy = False

            return {
                "healthy": is_healthy,
                "hit_rate_percent": round(hit_rate, 1),
                "current_size": len(self.cache),
                "total_requests": total_requests,
                "warnings": warnings
            }


# Global cache instance (initialized in main.py)
_query_cache: Optional[QueryCache] = None


def get_query_cache() -> Optional[QueryCache]:
    """Get global query cache instance"""
    return _query_cache


def init_query_cache(
    ttl_seconds: int = None,
    max_size: int = None,
    max_result_size_mb: int = None
) -> QueryCache:
    """
    Initialize global query cache

    Args:
        ttl_seconds: Cache TTL
        max_size: Max cached queries
        max_result_size_mb: Max result size to cache

    Returns:
        QueryCache instance
    """
    global _query_cache

    # Check if caching is disabled - check config file first, then env var
    # DEFAULT: Cache is DISABLED unless explicitly enabled
    cache_enabled = False
    try:
        from config_loader import get_config
        arc_config = get_config()
        cache_config = arc_config.config.get('query_cache', {})
        cache_enabled = cache_config.get('enabled', False)

        # Read TTL, max_size, max_result_size_mb from config if not provided
        if ttl_seconds is None:
            ttl_seconds = cache_config.get('ttl_seconds', None)
        if max_size is None:
            max_size = cache_config.get('max_size', None)
        if max_result_size_mb is None:
            max_result_size_mb = cache_config.get('max_result_size_mb', None)
    except Exception:
        # Fallback to environment variable if config not available
        # DEFAULT: Cache is DISABLED unless explicitly enabled
        cache_enabled = os.getenv("QUERY_CACHE_ENABLED", "false").lower() == "true"

    if not cache_enabled:
        logger.info("Query cache DISABLED by configuration")
        return None

    _query_cache = QueryCache(
        ttl_seconds=ttl_seconds,
        max_size=max_size,
        max_result_size_mb=max_result_size_mb
    )

    return _query_cache
