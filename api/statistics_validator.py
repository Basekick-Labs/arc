"""
Parquet Statistics Validator

This module provides startup validation for Parquet file statistics.
It can be integrated into the main application to automatically check
and backfill statistics on startup.

Usage:
    from api.statistics_validator import validate_and_backfill_statistics

    # In main.py startup:
    await validate_and_backfill_statistics(
        base_path="./data",
        auto_backfill=True,  # Set to False to only warn
        max_files_to_check=100  # Limit for quick startup
    )
"""

import logging
import asyncio
from pathlib import Path
from typing import Optional, Tuple
import os

logger = logging.getLogger(__name__)

# Try importing dependencies
try:
    import pyarrow.parquet as pq
    import duckdb
    BACKFILL_AVAILABLE = True
except ImportError:
    BACKFILL_AVAILABLE = False
    logger.warning("PyArrow or DuckDB not available - statistics validation disabled")


async def validate_and_backfill_statistics(
    base_path: str = "./data",
    auto_backfill: bool = False,
    max_files_to_check: int = 100,
    database: Optional[str] = None
) -> dict:
    """
    Validate Parquet files have statistics and optionally backfill

    Args:
        base_path: Data directory path
        auto_backfill: If True, automatically rewrite files missing statistics
        max_files_to_check: Maximum files to check (for quick startup validation)
        database: Optional database filter

    Returns:
        Dict with validation results
    """
    if not BACKFILL_AVAILABLE:
        logger.info("Statistics validation skipped (dependencies not available)")
        return {"skipped": True}

    # Check if validation is enabled via environment variable
    enabled = os.getenv('ARC_VALIDATE_STATISTICS', 'false').lower() == 'true'
    auto_backfill_env = os.getenv('ARC_AUTO_BACKFILL_STATISTICS', 'false').lower() == 'true'

    if auto_backfill_env:
        auto_backfill = True

    if not enabled:
        logger.debug("Statistics validation disabled (set ARC_VALIDATE_STATISTICS=true to enable)")
        return {"disabled": True}

    logger.info("🔍 Validating Parquet file statistics...")

    stats = {
        'files_checked': 0,
        'files_with_stats': 0,
        'files_missing_stats': 0,
        'files_backfilled': 0,
        'files_failed': 0
    }

    try:
        # Build search pattern
        data_path = Path(base_path)
        if database:
            pattern = f"{database}/**/*.parquet"
        else:
            pattern = "**/*.parquet"

        # Sample files for validation (not all files - too slow for startup)
        parquet_files = list(data_path.glob(pattern))

        if not parquet_files:
            logger.info("  No Parquet files found")
            return stats

        # Limit number of files to check for quick startup
        files_to_check = parquet_files[:max_files_to_check]

        logger.info(f"  Checking {len(files_to_check)} files (out of {len(parquet_files)} total)")

        # Check files asynchronously
        loop = asyncio.get_event_loop()

        for file_path in files_to_check:
            stats['files_checked'] += 1

            # Check in executor to avoid blocking
            has_stats = await loop.run_in_executor(
                None,
                _check_file_has_statistics,
                file_path
            )

            if has_stats:
                stats['files_with_stats'] += 1
            else:
                stats['files_missing_stats'] += 1
                logger.warning(f"  ⚠️  Missing statistics: {file_path.relative_to(data_path)}")

                if auto_backfill:
                    # Rewrite file with statistics
                    success = await loop.run_in_executor(
                        None,
                        _rewrite_with_statistics,
                        file_path
                    )

                    if success:
                        stats['files_backfilled'] += 1
                        logger.info(f"  ✅ Backfilled: {file_path.name}")
                    else:
                        stats['files_failed'] += 1
                        logger.error(f"  ❌ Failed to backfill: {file_path.name}")

        # Log summary
        if stats['files_missing_stats'] > 0:
            logger.warning(
                f"⚠️  Found {stats['files_missing_stats']}/{stats['files_checked']} files missing statistics"
            )

            if not auto_backfill:
                logger.info(
                    "💡 Run 'python scripts/backfill_parquet_statistics.py' to fix, "
                    "or set ARC_AUTO_BACKFILL_STATISTICS=true to auto-fix on startup"
                )
            else:
                logger.info(
                    f"✅ Backfilled {stats['files_backfilled']} files "
                    f"({stats['files_failed']} failed)"
                )
        else:
            logger.info("✅ All checked files have statistics")

        return stats

    except Exception as e:
        logger.error(f"Statistics validation failed: {e}", exc_info=True)
        return {"error": str(e)}


def _check_file_has_statistics(file_path: Path) -> bool:
    """Check if a Parquet file has statistics (sync function)"""
    try:
        parquet_file = pq.ParquetFile(file_path)
        metadata = parquet_file.metadata

        # Check if ANY row group has statistics for the 'time' column
        for i in range(metadata.num_row_groups):
            row_group = metadata.row_group(i)

            for j in range(row_group.num_columns):
                col = row_group.column(j)

                if col.path_in_schema == 'time':
                    stats = col.statistics

                    if stats and stats.has_min_max:
                        return True

        return False

    except Exception as e:
        logger.debug(f"Error checking statistics in {file_path}: {e}")
        return False


def _rewrite_with_statistics(file_path: Path) -> bool:
    """Rewrite a Parquet file with statistics (sync function)"""
    try:
        backup_path = file_path.with_suffix('.parquet.backup')
        temp_path = file_path.with_suffix('.parquet.tmp')

        # Read and rewrite using DuckDB
        con = duckdb.connect(':memory:')

        con.execute(f"""
            COPY (
                SELECT * FROM read_parquet('{file_path}')
            ) TO '{temp_path}' (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                COMPRESSION_LEVEL 3,
                ROW_GROUP_SIZE 122880,
                WRITE_STATISTICS true
            )
        """)

        con.close()

        # Move original to backup
        file_path.rename(backup_path)

        # Move temp to original
        temp_path.rename(file_path)

        # Delete backup
        backup_path.unlink()

        return True

    except Exception as e:
        logger.error(f"Failed to rewrite {file_path}: {e}")

        # Cleanup
        if temp_path.exists():
            temp_path.unlink()
        if backup_path.exists():
            backup_path.rename(file_path)

        return False
