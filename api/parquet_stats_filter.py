"""
Parquet Statistics-Based File Filtering for Arc

This module provides file-level filtering using Parquet metadata statistics
to skip files that don't match the query's time range predicates.

Key optimization: After partition pruning reduces to specific hours,
statistics filtering further reduces to only files with matching time ranges.

Example:
    Hour partition: 2024/03/15/14/ contains 10 Parquet files
    Query: time >= '2024-03-15 14:30:00' AND time < '2024-03-15 14:45:00'

    Statistics check:
    - file1.parquet: min=14:00, max=14:15 → Skip (max < 14:30)
    - file2.parquet: min=14:15, max=14:30 → Include (overlaps)
    - file3.parquet: min=14:30, max=14:45 → Include (matches)
    - file4.parquet: min=14:45, max=15:00 → Skip (min >= 14:45)

    Result: Read 2 files instead of 10 → 5x faster
"""

import logging
from datetime import datetime
from typing import List, Optional, Tuple, Dict
from pathlib import Path
import re

logger = logging.getLogger(__name__)

# Try importing PyArrow for Parquet metadata reading
try:
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False
    logger.warning("PyArrow not available - statistics-based filtering disabled")


class ParquetStatsFilter:
    """
    Filters Parquet files based on time column statistics

    Reads min/max statistics from Parquet file metadata without loading data.
    Works with all storage backends (local, S3, MinIO, GCS).
    """

    def __init__(self, time_column: str = "time"):
        """
        Initialize statistics filter

        Args:
            time_column: Name of the time column to filter on (default: "time")
        """
        self.time_column = time_column
        self.enabled = PYARROW_AVAILABLE
        self.stats = {
            'files_scanned': 0,
            'files_skipped': 0,
            'files_included': 0,
            'metadata_errors': 0
        }

    def get_file_time_range(self, file_path: str) -> Optional[Tuple[datetime, datetime]]:
        """
        Extract min/max time range from Parquet file metadata

        Args:
            file_path: Path to Parquet file (local or remote)

        Returns:
            Tuple of (min_time, max_time) or None if statistics unavailable
        """
        if not self.enabled:
            return None

        try:
            # Read Parquet metadata (doesn't download full file for remote storage)
            parquet_file = pq.ParquetFile(file_path)
            metadata = parquet_file.metadata

            min_time = None
            max_time = None

            # Scan all row groups to find global min/max
            for i in range(metadata.num_row_groups):
                row_group = metadata.row_group(i)

                # Find the time column
                for j in range(row_group.num_columns):
                    col = row_group.column(j)

                    # Check if this is the time column
                    if col.path_in_schema == self.time_column:
                        stats = col.statistics

                        if stats and stats.has_min_max:
                            # Get min/max values
                            col_min = stats.min
                            col_max = stats.max

                            # Convert to datetime if necessary
                            col_min_dt = self._to_datetime(col_min)
                            col_max_dt = self._to_datetime(col_max)

                            if col_min_dt and col_max_dt:
                                # Update global min/max
                                if min_time is None or col_min_dt < min_time:
                                    min_time = col_min_dt
                                if max_time is None or col_max_dt > max_time:
                                    max_time = col_max_dt

            if min_time and max_time:
                logger.debug(f"File {Path(file_path).name}: time range {min_time} to {max_time}")
                return (min_time, max_time)
            else:
                logger.debug(f"No time statistics found in {Path(file_path).name}")
                return None

        except Exception as e:
            logger.debug(f"Failed to read statistics from {file_path}: {e}")
            self.stats['metadata_errors'] += 1
            return None

    def _to_datetime(self, value) -> Optional[datetime]:
        """
        Convert Parquet statistics value to datetime

        Handles various Parquet timestamp formats:
        - Python datetime objects
        - Pandas Timestamp
        - Unix timestamps (int64 microseconds)
        - ISO strings

        Args:
            value: Statistics value from Parquet metadata

        Returns:
            datetime object or None if conversion fails
        """
        if value is None:
            return None

        # Already a datetime
        if isinstance(value, datetime):
            return value

        # Pandas Timestamp
        try:
            if hasattr(value, 'to_pydatetime'):
                return value.to_pydatetime()
        except:
            pass

        # Unix timestamp (int64 microseconds since epoch)
        if isinstance(value, int):
            try:
                # Parquet INT64 timestamps are typically microseconds
                return datetime.fromtimestamp(value / 1_000_000)
            except:
                pass

        # String representation
        if isinstance(value, (str, bytes)):
            try:
                if isinstance(value, bytes):
                    value = value.decode('utf-8')
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                pass

        logger.debug(f"Could not convert value to datetime: {value} (type: {type(value)})")
        return None

    def filter_files(
        self,
        file_paths: List[str],
        query_time_range: Tuple[datetime, datetime]
    ) -> List[str]:
        """
        Filter files based on time range statistics

        Args:
            file_paths: List of Parquet file paths to filter
            query_time_range: (start_time, end_time) from query WHERE clause

        Returns:
            Filtered list of files that overlap with query time range
        """
        if not self.enabled:
            logger.debug("Statistics filtering disabled (PyArrow not available)")
            return file_paths

        if not file_paths:
            return []

        query_start, query_end = query_time_range
        filtered_files = []

        logger.info(f"Filtering {len(file_paths)} files by statistics for range {query_start} to {query_end}")

        for file_path in file_paths:
            self.stats['files_scanned'] += 1

            # Get file's time range from metadata
            file_time_range = self.get_file_time_range(file_path)

            if file_time_range is None:
                # No statistics available - include file to be safe
                filtered_files.append(file_path)
                self.stats['files_included'] += 1
                logger.debug(f"Including {Path(file_path).name} (no statistics)")
                continue

            file_min, file_max = file_time_range

            # Check for overlap: file overlaps query if:
            # - file_max >= query_start AND file_min <= query_end
            if file_max >= query_start and file_min <= query_end:
                filtered_files.append(file_path)
                self.stats['files_included'] += 1
                logger.debug(f"Including {Path(file_path).name} (overlaps: {file_min} to {file_max})")
            else:
                self.stats['files_skipped'] += 1
                logger.debug(f"Skipping {Path(file_path).name} (no overlap: {file_min} to {file_max})")

        reduction = len(file_paths) - len(filtered_files)
        if reduction > 0:
            logger.info(
                f"✨ Statistics filtering: Skipped {reduction} files "
                f"({len(filtered_files)}/{len(file_paths)} remaining)"
            )
        else:
            logger.debug("Statistics filtering: All files needed for query")

        return filtered_files

    def get_stats(self) -> Dict[str, int]:
        """Get filtering statistics"""
        return self.stats.copy()

    def reset_stats(self):
        """Reset statistics counters"""
        self.stats = {
            'files_scanned': 0,
            'files_skipped': 0,
            'files_included': 0,
            'metadata_errors': 0
        }


# Global instance
_stats_filter = ParquetStatsFilter()


def filter_files_by_statistics(
    file_paths: List[str],
    time_range: Tuple[datetime, datetime]
) -> List[str]:
    """
    Convenience function to filter files by statistics

    Args:
        file_paths: List of Parquet file paths
        time_range: (start_time, end_time) tuple

    Returns:
        Filtered list of files
    """
    return _stats_filter.filter_files(file_paths, time_range)


def get_filter_stats() -> Dict[str, int]:
    """Get global filter statistics"""
    return _stats_filter.get_stats()
