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
        max_result_size_mb: int = None,
        max_total_cache_mb: int = None
    ):
        """
        Initialize query cache

        Args:
            ttl_seconds: Cache TTL in seconds (default: 60, from env QUERY_CACHE_TTL)
            max_size: Maximum number of cached queries (default: 100, from env QUERY_CACHE_MAX_SIZE)
            max_result_size_mb: Max size of individual result to cache in MB (default: 10)
            max_total_cache_mb: Max total cache size in MB (default: 200)
        """
        self.ttl_seconds = ttl_seconds or int(os.getenv("QUERY_CACHE_TTL", "60"))
        self.max_size = max_size or int(os.getenv("QUERY_CACHE_MAX_SIZE", "100"))
        self.max_result_size_mb = max_result_size_mb or int(os.getenv("QUERY_CACHE_MAX_RESULT_MB", "10"))
        self.max_total_cache_mb = max_total_cache_mb or int(os.getenv("QUERY_CACHE_MAX_TOTAL_MB", "200"))

        # Use OrderedDict for LRU eviction
        self.cache: OrderedDict[str, Tuple[Dict[str, Any], datetime]] = OrderedDict()
        self.lock = threading.RLock()

        # Track total cache size
        self.current_cache_size_mb = 0.0

        # Metrics
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.expirations = 0

        # Background cleanup thread
        self._cleanup_running = False
        self._cleanup_thread = None

        logger.info(
            f"Query cache initialized: TTL={self.ttl_seconds}s, "
            f"MaxSize={self.max_size}, MaxResultSize={self.max_result_size_mb}MB, "
            f"MaxTotalCache={self.max_total_cache_mb}MB"
        )

        # Start background cleanup
        self._start_cleanup_thread()

    def _start_cleanup_thread(self):
        """Start background thread to clean expired entries"""
        self._cleanup_running = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_expired,
            daemon=True,
            name="query-cache-cleanup"
        )
        self._cleanup_thread.start()
        logger.debug("Started query cache cleanup thread")

    def _cleanup_expired(self):
        """
        Background task to remove expired cache entries.
        Runs every ttl_seconds/2 to ensure memory is actually released.
        """
        import time
        import gc

        # Run cleanup at half the TTL interval (more aggressive)
        cleanup_interval = max(self.ttl_seconds // 2, 10)

        while self._cleanup_running:
            try:
                time.sleep(cleanup_interval)

                current_time = datetime.now()
                expired_count = 0
                freed_mb = 0.0

                with self.lock:
                    # Find expired entries
                    expired_keys = []
                    for key, (cached_data, cached_at) in list(self.cache.items()):
                        age_seconds = (current_time - cached_at).total_seconds()
                        if age_seconds >= self.ttl_seconds:
                            expired_keys.append(key)
                            # Track size for metrics
                            freed_mb += self._estimate_size_mb(cached_data)

                    # Remove expired entries
                    for key in expired_keys:
                        del self.cache[key]
                        expired_count += 1
                        self.expirations += 1

                    # Update total cache size
                    self.current_cache_size_mb -= freed_mb

                if expired_count > 0:
                    logger.info(
                        f"Cache cleanup: removed {expired_count} expired entries, "
                        f"freed {freed_mb:.1f}MB"
                    )
                    # Force GC to actually release the memory
                    gc.collect()

            except Exception as e:
                logger.error(f"Error in cache cleanup thread: {e}", exc_info=True)

    def stop_cleanup_thread(self):
        """Stop the background cleanup thread gracefully"""
        self._cleanup_running = False
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)
            logger.debug("Stopped cache cleanup thread")

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
        Estimate result size in MB using accurate measurement

        Args:
            result: Query result dict

        Returns:
            Estimated size in MB
        """
        try:
            import sys

            data = result.get("data", [])
            if not data:
                return 0.0

            # Sample first 10 rows for accurate estimate
            sample_size = 0
            sample_count = min(10, len(data))

            for row in data[:sample_count]:
                # Calculate size of each cell in the row
                sample_size += sum(sys.getsizeof(cell) for cell in row)

            # Extrapolate to all rows
            avg_row_size = sample_size / sample_count
            total_bytes = avg_row_size * len(data)

            # Add overhead for columns and metadata
            columns = result.get("columns", [])
            metadata_bytes = sum(sys.getsizeof(col) for col in columns)
            total_bytes += metadata_bytes

            return total_bytes / (1024 * 1024)

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
            # Check if total cache size would exceed limit
            # Evict entries until we have space
            while (self.current_cache_size_mb + size_mb > self.max_total_cache_mb and
                   len(self.cache) > 0 and key not in self.cache):
                # Remove oldest entry
                oldest_key, (oldest_data, _) = self.cache.popitem(last=False)
                oldest_size = self._estimate_size_mb(oldest_data)
                self.current_cache_size_mb -= oldest_size
                self.evictions += 1
                logger.debug(
                    f"Cache EVICT: Total size limit ({self.max_total_cache_mb}MB), "
                    f"freed {oldest_size:.1f}MB"
                )

            # Evict oldest if cache entry count is full
            if len(self.cache) >= self.max_size and key not in self.cache:
                # Remove oldest (first item in OrderedDict)
                oldest_key, (oldest_data, _) = self.cache.popitem(last=False)
                oldest_size = self._estimate_size_mb(oldest_data)
                self.current_cache_size_mb -= oldest_size
                self.evictions += 1
                logger.debug(f"Cache EVICT: Entry count limit ({self.max_size})")

            # If updating existing entry, subtract old size first
            if key in self.cache:
                old_data, _ = self.cache[key]
                old_size = self._estimate_size_mb(old_data)
                self.current_cache_size_mb -= old_size

            # Store result
            self.cache[key] = (result, datetime.now())
            self.current_cache_size_mb += size_mb

            logger.debug(
                f"Cache SET: rows={result.get('row_count', 0)}, "
                f"size={size_mb:.2f}MB, total={self.current_cache_size_mb:.1f}MB, "
                f"sql={sql[:50]}"
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
