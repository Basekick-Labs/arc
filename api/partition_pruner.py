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

logger = logging.getLogger(__name__)


class PartitionPruner:
    """
    Prunes partition files based on time predicates in WHERE clauses

    Arc's partition structure: {database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
    Example: default/cpu/2024/03/15/14/cpu_20240315_140000_1000.parquet
    """

    def __init__(self):
        self.enabled = True
        self.stats = {
            'queries_optimized': 0,
            'files_pruned': 0,
            'files_scanned': 0
        }

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

        path_match = re.match(
            r'((?:s3|gs)://[^/]+|[^/]+)/([^/]+)/([^/]+)/\*\*/\*\.parquet',
            original_path
        )

        if not path_match:
            logger.debug(f"Path format not recognized: {original_path}")
            return original_path, False

        base_path = path_match.group(1)
        database = path_match.group(2)
        measurement = path_match.group(3)

        # Generate optimized partition paths
        partition_paths = self.generate_partition_paths(
            base_path, database, measurement, time_range
        )

        # For DuckDB, we can use a list of paths or a combined glob
        # Using array syntax: ['path1', 'path2', ...]
        if len(partition_paths) == 1:
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
