"""
Daily Compaction Tier for Arc Core

Compacts hourly-compacted files into daily files to reduce file count.

Example flow:
- Input: 24 hourly compacted files (cpu/2025/11/05/00/*.parquet, cpu/2025/11/05/01/*.parquet, ...)
- Output: 1 daily compacted file (cpu/2025/11/05/cpu_20251105_daily.parquet)

This is the second tier in Arc's tiered compaction strategy:
- Tier 1: Hourly compaction (every 10 minutes) - merges small files within hour partitions
- Tier 2: Daily compaction (3am daily) - merges hourly files into daily files (THIS MODULE)
- Tier 3: Weekly compaction (future) - merges daily files into weekly files
- Tier 4: Monthly compaction (future) - merges weekly files into monthly files
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any
from pathlib import Path

from storage.compaction_tier import CompactionTier

logger = logging.getLogger(__name__)


class DailyCompaction(CompactionTier):
    """
    Daily compaction tier - compacts hourly files into daily files.

    Runs at 3am daily (configurable) and merges all hourly-compacted files
    from completed days into single daily files.
    """

    def __init__(
        self,
        storage_backend,
        min_age_hours: int = 24,
        min_files: int = 12,
        target_size_mb: int = 2048,
        enabled: bool = True
    ):
        """
        Initialize daily compaction tier

        Args:
            storage_backend: Storage backend instance
            min_age_hours: Don't compact days younger than this (default: 24 hours)
            min_files: Only compact days with at least this many files (default: 12 = half a day)
            target_size_mb: Target size for daily compacted files (default: 2048 MB = 2 GB)
            enabled: Enable daily compaction (default: True)
        """
        super().__init__(
            storage_backend=storage_backend,
            min_age_hours=min_age_hours,
            min_files=min_files,
            target_size_mb=target_size_mb,
            enabled=enabled
        )

        logger.info(
            f"Daily compaction tier initialized: "
            f"min_age_hours={min_age_hours}, "
            f"min_files={min_files}, "
            f"target_size_mb={target_size_mb}, "
            f"enabled={enabled}"
        )

    def get_tier_name(self) -> str:
        """Get tier name"""
        return "daily"

    def get_partition_level(self) -> str:
        """Get partition level (day)"""
        return "day"

    async def find_candidates(self, database: str, measurement: str) -> List[Dict[str, Any]]:
        """
        Find day partitions that are candidates for daily compaction.

        A day is a candidate if:
        1. It's older than min_age_hours (default: 24 hours - completed day only)
        2. It has at least min_files files (default: 12 files - at least half a day)
        3. It contains hourly-compacted files OR many small uncompacted files

        Args:
            database: Database name
            measurement: Measurement name

        Returns:
            List of candidate partitions
        """
        if not self.enabled:
            return []

        candidates = []
        cutoff_time = datetime.utcnow() - timedelta(hours=self.min_age_hours)

        logger.debug(f"Scanning for daily compaction candidates: {database}/{measurement}")

        # Get all day partitions
        day_partitions = await self._list_day_partitions(database, measurement, cutoff_time)

        for partition_info in day_partitions:
            partition_path = partition_info['path']
            files = partition_info['files']
            partition_time = partition_info['time']

            # Check if should compact
            if self.should_compact(files, partition_time):
                candidates.append({
                    'database': database,
                    'measurement': measurement,
                    'partition_path': partition_path,
                    'files': files,
                    'file_count': len(files),
                    'tier': self.get_tier_name()
                })

                logger.info(
                    f"Daily compaction candidate: {database}/{partition_path} "
                    f"({len(files)} files)"
                )

        logger.info(
            f"Found {len(candidates)} daily compaction candidates for {database}/{measurement}"
        )

        return candidates

    def should_compact(self, files: List[str], partition_time: datetime) -> bool:
        """
        Determine if a day partition should be compacted.

        Compaction criteria:
        1. Has at least min_files files (default: 12 - at least half a day of data)
        2. Does NOT already have a daily compacted file
        3. OR has a daily compacted file but many new hourly files have accumulated

        Args:
            files: List of file paths in the partition
            partition_time: Partition timestamp (day)

        Returns:
            True if partition should be compacted
        """
        if len(files) < self.min_files:
            logger.debug(
                f"Skipping daily compaction: only {len(files)} files "
                f"(min {self.min_files} required)"
            )
            return False

        # Separate daily-compacted files from hourly files
        # Note: Use path depth to distinguish, not just filename suffix
        # - Daily files: measurement/year/month/day/file.parquet (5 parts)
        # - Hourly files: measurement/year/month/day/hour/file.parquet (6 parts)
        # This handles transition period where old daily files used '_compacted.parquet'
        daily_compacted_files = []
        hourly_files = []

        for f in files:
            parts = f.split('/')
            if len(parts) == 5:
                # Day-level file = daily compacted file (regardless of suffix)
                daily_compacted_files.append(f)
            else:
                # Hour-level file = hourly file
                hourly_files.append(f)

        # Case 1: No daily compacted file yet, and has enough total files
        if not daily_compacted_files and len(files) >= self.min_files:
            logger.debug(
                f"Daily compaction needed: first time compaction - {len(files)} files"
            )
            return True

        # Case 2: Has daily compacted file, but many new hourly files accumulated
        # Re-compact if we have enough new hourly files (default: 12+)
        if daily_compacted_files and len(hourly_files) >= self.min_files:
            logger.info(
                f"Daily re-compaction needed: "
                f"{len(daily_compacted_files)} daily + {len(hourly_files)} hourly files"
            )
            return True

        return False

    async def _list_day_partitions(
        self,
        database: str,
        measurement: str,
        cutoff_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        List day partitions for a measurement older than cutoff_time.

        Storage path structure: {database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet

        For daily compaction, we group by day: {database}/{measurement}/{year}/{month}/{day}/

        Args:
            database: Database name
            measurement: Measurement name
            cutoff_time: Only return partitions older than this time

        Returns:
            List of day partition info dicts with 'path', 'files', and 'time'
        """
        loop = asyncio.get_event_loop()

        def _list():
            partitions = {}

            try:
                # List all files for this database/measurement
                if hasattr(self.storage_backend, 'base_path'):
                    # Local filesystem
                    base_path = Path(self.storage_backend.base_path)
                    db_meas_path = base_path / database / measurement

                    objects = []
                    if db_meas_path.exists():
                        for file_path in db_meas_path.rglob('*.parquet'):
                            relative = file_path.relative_to(base_path / database)
                            objects.append(str(relative))

                elif hasattr(self.storage_backend, 's3_client'):
                    # S3-based storage
                    bucket = self.storage_backend.bucket
                    prefix = f"{database}/{measurement}/"
                    paginator = self.storage_backend.s3_client.get_paginator('list_objects_v2')

                    objects = []
                    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=100000):
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                key = obj['Key']
                                # Remove database prefix
                                if key.startswith(f"{database}/"):
                                    relative_key = key[len(f"{database}/"):]
                                    objects.append(relative_key)
                else:
                    # Fallback - use storage backend's list_objects
                    prefix = f"{measurement}/"
                    objects = self.storage_backend.list_objects(prefix=prefix, max_keys=100000)

                logger.debug(f"Found {len(objects)} objects for {database}/{measurement}")

                for obj in objects:
                    # Parse path: measurement/year/month/day/hour/file.parquet
                    parts = obj.split('/')
                    if len(parts) >= 5:
                        # Extract time components
                        meas, year, month, day = parts[0], parts[1], parts[2], parts[3]

                        # Validate this is the correct measurement
                        if meas != measurement:
                            continue

                        # Check if partition is old enough (compare at day level)
                        try:
                            partition_time = datetime(int(year), int(month), int(day))

                            if partition_time < cutoff_time:
                                # Build partition path (day level, relative to database)
                                partition_path = f"{measurement}/{year}/{month}/{day}"

                                if partition_path not in partitions:
                                    partitions[partition_path] = {
                                        'files': [],
                                        'time': partition_time
                                    }

                                # Add file path (relative to database)
                                partitions[partition_path]['files'].append(obj)

                        except ValueError:
                            # Invalid date/time in path, skip
                            logger.warning(f"Invalid partition path format: {obj}")
                            continue

                logger.info(
                    f"Found {len(partitions)} day partitions for {database}/{measurement} "
                    f"older than {cutoff_time}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to list day partitions for {database}/{measurement}: {e}",
                    exc_info=True
                )

            # Convert to list of dicts
            return [
                {'path': path, 'files': info['files'], 'time': info['time']}
                for path, info in partitions.items()
            ]

        return await loop.run_in_executor(None, _list)
