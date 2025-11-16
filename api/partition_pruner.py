"""
Partition Pruning for Arc

This module provides file-level partition pruning to skip reading Parquet files
that don't match the query's WHERE clause predicates.

Key optimization: Instead of reading ALL files with /**/*.parquet glob,
we extract time ranges from WHERE clauses and build a targeted file list.

Example:
    Query: SELECT * FROM cpu WHERE time >= '2024-03-15' AND time < '2024-03-16'

    Before: Read all 8,760 hour partitions (1 year)
    After: Read only 24 hour partitions (1 day)

    Result: 365x fewer files, 10-100x faster
"""

import re
import logging
from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Set
from pathlib import Path
import glob as glob_module

logger = logging.getLogger(__name__)

# Try importing statistics filter
try:
    from api.parquet_stats_filter import ParquetStatsFilter
    STATS_FILTER_AVAILABLE = True
except ImportError:
    STATS_FILTER_AVAILABLE = False
    logger.debug("Statistics filtering not available")


class PartitionPruner:
    """
    Prunes partition files based on time predicates in WHERE clauses

    Arc's partition structure: {database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
    Example: default/cpu/2024/03/15/14/cpu_20240315_140000_1000.parquet
    """

    def __init__(self, enable_statistics_filtering: bool = True):
        self.enabled = False  # DISABLED: Still has issues with data gaps - needs more investigation
        self.enable_statistics_filtering = enable_statistics_filtering and STATS_FILTER_AVAILABLE
        self.stats_filter = ParquetStatsFilter() if self.enable_statistics_filtering else None
        self.stats = {
            'queries_optimized': 0,
            'files_pruned': 0,
            'files_scanned': 0
        }

        if enable_statistics_filtering and not STATS_FILTER_AVAILABLE:
            logger.warning("Statistics filtering requested but not available (missing dependencies)")

    def extract_time_range(self, sql: str) -> Optional[Tuple[datetime, datetime]]:
        """
        Extract time range from WHERE clause

        Supports patterns like:
        - time >= '2024-03-15' AND time < '2024-03-16'
        - time BETWEEN '2024-03-15' AND '2024-03-16'
        - time > '2024-03-15 10:00:00'
        - timestamp >= 1710460800 (unix timestamp)

        Args:
            sql: SQL query string

        Returns:
            Tuple of (start_time, end_time) or None if no time filter found
        """
        if 'WHERE' not in sql.upper() and 'where' not in sql:
            return None

        # Extract WHERE clause
        where_match = re.search(r'\bWHERE\b\s+(.+?)(?:\bGROUP BY\b|\bORDER BY\b|\bLIMIT\b|$)',
                                sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return None

        where_clause = where_match.group(1)
        logger.debug(f"Analyzing WHERE clause: {where_clause[:100]}...")

        start_time = None
        end_time = None

        # Pattern 1: time >= 'YYYY-MM-DD' or time > 'YYYY-MM-DD HH:MM:SS'
        start_patterns = [
            r"time\s*>=\s*'([^']+)'",
            r"time\s*>\s*'([^']+)'",
            r"timestamp\s*>=\s*'([^']+)'",
            r"timestamp\s*>\s*'([^']+)'",
        ]

        for pattern in start_patterns:
            match = re.search(pattern, where_clause, re.IGNORECASE)
            if match:
                try:
                    time_str = match.group(1)
                    start_time = self._parse_datetime(time_str)
                    logger.debug(f"Found start time: {start_time}")
                    break
                except Exception as e:
                    logger.debug(f"Failed to parse start time '{time_str}': {e}")

        # Pattern 2: time < 'YYYY-MM-DD' or time <= 'YYYY-MM-DD HH:MM:SS'
        end_patterns = [
            r"time\s*<\s*'([^']+)'",
            r"time\s*<=\s*'([^']+)'",
            r"timestamp\s*<\s*'([^']+)'",
            r"timestamp\s*<=\s*'([^']+)'",
        ]

        for pattern in end_patterns:
            match = re.search(pattern, where_clause, re.IGNORECASE)
            if match:
                try:
                    time_str = match.group(1)
                    end_time = self._parse_datetime(time_str)
                    logger.debug(f"Found end time: {end_time}")
                    break
                except Exception as e:
                    logger.debug(f"Failed to parse end time '{time_str}': {e}")

        # Pattern 3: BETWEEN
        between_match = re.search(
            r"time\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'",
            where_clause,
            re.IGNORECASE
        )
        if between_match:
            try:
                start_time = self._parse_datetime(between_match.group(1))
                end_time = self._parse_datetime(between_match.group(2))
                logger.debug(f"Found BETWEEN range: {start_time} to {end_time}")
            except Exception as e:
                logger.debug(f"Failed to parse BETWEEN clause: {e}")

        if start_time and end_time:
            return (start_time, end_time)
        elif start_time:
            # Only start time - assume query up to "now"
            return (start_time, datetime.utcnow() + timedelta(days=1))
        elif end_time:
            # Only end time - assume from beginning of data
            return (datetime(2020, 1, 1), end_time)

        return None

    def _parse_datetime(self, time_str: str) -> datetime:
        """Parse datetime string in various formats"""
        # Try common formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%Y/%m/%d %H:%M:%S',
            '%Y/%m/%d',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(time_str, fmt)
            except ValueError:
                continue

        # Try ISO format
        try:
            # Handle ISO format with timezone
            if 'T' in time_str:
                # Remove timezone info for simplicity
                time_str = time_str.split('+')[0].split('Z')[0]
                return datetime.fromisoformat(time_str)
        except ValueError:
            pass

        raise ValueError(f"Could not parse datetime: {time_str}")

    def generate_partition_paths(
        self,
        base_path: str,
        database: str,
        measurement: str,
        time_range: Tuple[datetime, datetime]
    ) -> List[str]:
        """
        Generate list of partition paths for the given time range

        Args:
            base_path: Storage base path (e.g., 's3://bucket' or '/data/arc')
            database: Database name
            measurement: Measurement/table name
            time_range: (start_time, end_time) tuple

        Returns:
            List of partition paths to read
        """
        start_time, end_time = time_range

        paths = []
        current = start_time.replace(minute=0, second=0, microsecond=0)

        # Generate hour-by-hour paths
        while current < end_time:
            year = current.strftime('%Y')
            month = current.strftime('%m')
            day = current.strftime('%d')
            hour = current.strftime('%H')

            # Build path for this hour partition
            path = f"{base_path}/{database}/{measurement}/{year}/{month}/{day}/{hour}/*.parquet"
            paths.append(path)

            current += timedelta(hours=1)

        logger.info(
            f"Partition pruning: Generated {len(paths)} hour partitions "
            f"for range {start_time} to {end_time}"
        )

        self.stats['queries_optimized'] += 1

        return paths

    def _filter_existing_paths(self, paths: List[str]) -> List[str]:
        """
        Filter out paths that don't have any matching files

        This is critical for local filesystem because DuckDB read_parquet()
        fails if ANY path in the array doesn't exist, even if other paths do.

        Args:
            paths: List of glob patterns

        Returns:
            List of paths that have at least one matching file
        """
        import glob as glob_module

        existing_paths = []
        for pattern in paths:
            # Check if this glob pattern matches any files
            matches = glob_module.glob(pattern)
            if matches:
                existing_paths.append(pattern)
                logger.debug(f"Path exists: {pattern} ({len(matches)} files)")
            else:
                logger.debug(f"Path skipped (no files): {pattern}")

        logger.info(f"Filtered {len(paths)} paths → {len(existing_paths)} existing paths")
        return existing_paths

    def expand_glob_patterns(self, glob_patterns: List[str]) -> List[str]:
        """
        Expand glob patterns to individual file paths

        Args:
            glob_patterns: List of glob patterns (e.g., ['s3://bucket/db/cpu/2024/03/15/00/*.parquet'])

        Returns:
            List of individual file paths
        """
        all_files = []

        for pattern in glob_patterns:
            # Handle different storage backends
            if pattern.startswith('s3://') or pattern.startswith('gs://'):
                # For remote storage, we'll need to use DuckDB's glob function
                # For now, keep the glob pattern as-is and let DuckDB handle it
                # Statistics filtering will work on a per-partition basis
                logger.debug(f"Remote glob pattern: {pattern}")
                all_files.append(pattern)
            else:
                # Local filesystem - expand glob
                try:
                    expanded = glob_module.glob(pattern, recursive=True)
                    all_files.extend(expanded)
                    logger.debug(f"Expanded {pattern} to {len(expanded)} files")
                except Exception as e:
                    logger.warning(f"Failed to expand glob {pattern}: {e}")
                    all_files.append(pattern)

        return all_files

    def apply_statistics_filtering(
        self,
        partition_paths: List[str],
        time_range: Tuple[datetime, datetime]
    ) -> List[str]:
        """
        Apply statistics-based filtering to partition paths

        Args:
            partition_paths: List of partition paths (may contain globs)
            time_range: (start_time, end_time) tuple

        Returns:
            Filtered list of file paths
        """
        if not self.enable_statistics_filtering or not self.stats_filter:
            logger.debug("Statistics filtering disabled")
            return partition_paths

        # For local files, expand globs and filter
        # For remote (S3/GCS), keep globs (DuckDB will handle enumeration)
        has_remote = any(p.startswith(('s3://', 'gs://')) for p in partition_paths)

        if has_remote:
            # Can't easily enumerate S3/GCS files without credentials here
            # Statistics filtering will be less effective for remote storage
            # TODO: Consider integrating with storage backend for remote file listing
            logger.info("Statistics filtering skipped for remote storage (not yet implemented)")
            return partition_paths

        # Expand local glob patterns to individual files
        all_files = []
        for pattern in partition_paths:
            try:
                expanded = glob_module.glob(pattern, recursive=True)
                logger.info(f"Glob {pattern} expanded to {len(expanded)} files")
                all_files.extend(expanded)
            except Exception as e:
                logger.warning(f"Failed to expand glob {pattern}: {e}")
                # Don't add pattern on error, let it be handled gracefully

        if not all_files:
            logger.warning(f"⚠️ No files found after expanding {len(partition_paths)} globs - returning empty list")
            return []

        logger.info(f"Total files after glob expansion: {len(all_files)}")

        # Apply statistics filtering
        filtered_files = self.stats_filter.filter_files(all_files, time_range)

        if not filtered_files:
            logger.debug("No files match time range after statistics filtering - returning empty list")
            return []

        return filtered_files

    def optimize_table_path(
        self,
        original_path: str,
        sql: str
    ) -> Tuple[str, bool]:
        """
        Optimize a table path based on WHERE clause predicates

        Args:
            original_path: Original glob path (e.g., 's3://bucket/db/cpu/**/*.parquet')
            sql: Full SQL query

        Returns:
            Tuple of (optimized_path_or_list, was_optimized)
        """
        if not self.enabled:
            return original_path, False

        # Extract time range from query
        time_range = self.extract_time_range(sql)
        if not time_range:
            logger.debug("No time range found in query, skipping partition pruning")
            return original_path, False

        # Parse original path to extract components
        # Format: {protocol}://{bucket}/{database}/{measurement}/**/*.parquet
        # or: {base_path}/{database}/{measurement}/**/*.parquet
        # Match paths ending with /{database}/{measurement}/**/*.parquet

        path_match = re.match(
            r'(.+)/([^/]+)/([^/]+)/\*\*/\*\.parquet$',
            original_path
        )

        if not path_match:
            logger.debug(f"Path format not recognized: {original_path}")
            return original_path, False

        base_path = path_match.group(1)
        database = path_match.group(2)
        measurement = path_match.group(3)

        logger.debug(f"Parsed path: base={base_path}, database={database}, measurement={measurement}")

        # Generate optimized partition paths
        partition_paths = self.generate_partition_paths(
            base_path, database, measurement, time_range
        )

        # Filter out non-existent paths for local storage
        # DuckDB read_parquet() fails if ANY path in array doesn't exist
        if base_path.startswith('/') or base_path.startswith('./'):
            # Local filesystem - filter out non-existent paths
            partition_paths = self._filter_existing_paths(partition_paths)

            if len(partition_paths) == 0:
                # No partitions exist for this time range - fall back to original for empty result
                logger.info(
                    f"⚠️ Partition pruning: No data exists for time range "
                    f"{time_range[0]} to {time_range[1]}, using fallback"
                )
                return original_path, False

        # Apply statistics-based filtering if enabled
        if self.enable_statistics_filtering:
            filtered_paths = self.apply_statistics_filtering(partition_paths, time_range)
            if filtered_paths:
                partition_paths = filtered_paths
            elif filtered_paths == []:
                # No files found - fall back to original path for graceful empty result
                logger.info(
                    f"⚠️ Partition pruning: No files found for time range "
                    f"{time_range[0]} to {time_range[1]}, using fallback"
                )
                return original_path, False

        # For DuckDB, we can use a list of paths or a combined glob
        # Using array syntax: ['path1', 'path2', ...]
        if len(partition_paths) == 0:
            # No files found, fall back to original for graceful empty result
            logger.info(f"⚠️ Partition pruning: No partitions found, using fallback")
            return original_path, False
        elif len(partition_paths) == 1:
            optimized = partition_paths[0]
        else:
            # DuckDB supports arrays of paths
            optimized = partition_paths

        files_before = "unknown (glob)"
        files_after = len(partition_paths) if isinstance(partition_paths, list) else 1

        logger.info(
            f"✨ Partition pruning active: "
            f"{files_before} → {files_after} hour partitions"
        )

        return optimized, True

    def get_stats(self) -> dict:
        """Get pruning statistics"""
        return self.stats.copy()

    def reset_stats(self):
        """Reset statistics counters"""
        self.stats = {
            'queries_optimized': 0,
            'files_pruned': 0,
            'files_scanned': 0
        }


# Global instance
_pruner = PartitionPruner()


def optimize_query_path(path: str, sql: str) -> Tuple[str, bool]:
    """
    Convenience function to optimize a query path

    Args:
        path: Original glob path
        sql: SQL query

    Returns:
        Tuple of (optimized_path, was_optimized)
    """
    return _pruner.optimize_table_path(path, sql)


def get_pruner_stats() -> dict:
    """Get global pruner statistics"""
    return _pruner.get_stats()
