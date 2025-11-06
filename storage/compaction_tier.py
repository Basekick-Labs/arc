"""
Base class for tiered compaction in Arc Core

Defines the interface for different compaction tiers (hourly, daily, weekly, monthly).
Each tier is responsible for identifying and compacting files at its granularity level.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class CompactionTier(ABC):
    """
    Abstract base class for compaction tiers.

    Each tier (daily, weekly, monthly) implements this interface to provide:
    - Partition identification (which partitions need compaction)
    - File selection (which files to compact within a partition)
    - Compaction strategy (how to merge files)
    """

    def __init__(
        self,
        storage_backend,
        min_age_hours: int,
        min_files: int,
        target_size_mb: int = 2048,
        enabled: bool = True
    ):
        """
        Initialize compaction tier

        Args:
            storage_backend: Storage backend instance
            min_age_hours: Don't compact partitions younger than this
            min_files: Only compact partitions with at least this many files
            target_size_mb: Target size for compacted files
            enabled: Enable this tier
        """
        self.storage_backend = storage_backend
        self.min_age_hours = min_age_hours
        self.min_files = min_files
        self.target_size_mb = target_size_mb
        self.enabled = enabled

        # Metrics
        self.total_compactions = 0
        self.total_files_compacted = 0
        self.total_bytes_saved = 0

    @abstractmethod
    def get_tier_name(self) -> str:
        """
        Get human-readable tier name (e.g., 'daily', 'weekly', 'monthly')

        Returns:
            Tier name string
        """
        pass

    @abstractmethod
    def get_partition_level(self) -> str:
        """
        Get partition level for this tier (e.g., 'day', 'week', 'month')

        Returns:
            Partition level string
        """
        pass

    @abstractmethod
    async def find_candidates(self, database: str, measurement: str) -> List[Dict[str, Any]]:
        """
        Find partitions that are candidates for compaction at this tier level.

        Args:
            database: Database name
            measurement: Measurement name

        Returns:
            List of candidate partitions with metadata:
            [
                {
                    'database': 'telegraf',
                    'measurement': 'cpu',
                    'partition_path': 'cpu/2025/11/05',  # Example for daily tier
                    'files': ['cpu/2025/11/05/00/file1.parquet', ...],
                    'file_count': 24,
                    'tier': 'daily'
                },
                ...
            ]
        """
        pass

    @abstractmethod
    def should_compact(self, files: List[str], partition_time: datetime) -> bool:
        """
        Determine if a partition should be compacted based on tier-specific criteria.

        Args:
            files: List of file paths in the partition
            partition_time: Partition timestamp

        Returns:
            True if partition should be compacted
        """
        pass

    def get_compacted_filename(self, measurement: str, partition_time: datetime) -> str:
        """
        Generate filename for compacted file.

        Args:
            measurement: Measurement name
            partition_time: Partition timestamp

        Returns:
            Filename (e.g., 'cpu_20251105_daily.parquet')
        """
        timestamp = partition_time.strftime('%Y%m%d')
        return f"{measurement}_{timestamp}_{self.get_tier_name()}.parquet"

    def is_compacted_file(self, filename: str) -> bool:
        """
        Check if file is already a compacted file from this tier.

        Args:
            filename: File name to check

        Returns:
            True if file is compacted at this tier level
        """
        tier_name = self.get_tier_name()
        return f"_{tier_name}.parquet" in filename

    def get_stats(self) -> Dict[str, Any]:
        """
        Get tier statistics.

        Returns:
            Statistics dictionary
        """
        return {
            'tier': self.get_tier_name(),
            'enabled': self.enabled,
            'min_age_hours': self.min_age_hours,
            'min_files': self.min_files,
            'target_size_mb': self.target_size_mb,
            'total_compactions': self.total_compactions,
            'total_files_compacted': self.total_files_compacted,
            'total_bytes_saved': self.total_bytes_saved,
            'total_bytes_saved_mb': self.total_bytes_saved / 1024 / 1024 if self.total_bytes_saved > 0 else 0
        }
