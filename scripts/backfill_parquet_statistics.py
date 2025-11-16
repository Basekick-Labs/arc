#!/usr/bin/env python3
"""
Backfill Parquet Statistics to Existing Compacted Files

This script scans existing Parquet files and rewrites any that are missing
statistics metadata. This is necessary for files created before statistics
writing was enabled in the compaction process.

The script:
1. Scans all Parquet files in the data directory
2. Checks if each file has proper statistics metadata
3. Rewrites files missing statistics using DuckDB with WRITE_STATISTICS=true
4. Preserves the original file until the rewrite succeeds

Usage:
    # Dry run (check only, don't modify)
    python scripts/backfill_parquet_statistics.py --dry-run

    # Run backfill for all files
    python scripts/backfill_parquet_statistics.py

    # Run backfill for specific database
    python scripts/backfill_parquet_statistics.py --database prod

    # Run backfill for specific measurement
    python scripts/backfill_parquet_statistics.py --database prod --measurement cpu
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime
import duckdb
import pyarrow.parquet as pq
from typing import Optional, List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ParquetStatisticsBackfill:
    """Backfill statistics to Parquet files"""

    def __init__(self, base_path: str = "./data", dry_run: bool = False):
        self.base_path = Path(base_path)
        self.dry_run = dry_run
        self.stats = {
            'total_files': 0,
            'files_with_stats': 0,
            'files_missing_stats': 0,
            'files_rewritten': 0,
            'files_failed': 0,
            'bytes_processed': 0
        }

    def check_file_has_statistics(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """
        Check if a Parquet file has statistics metadata

        Returns:
            (has_statistics, reason)
        """
        try:
            parquet_file = pq.ParquetFile(file_path)
            metadata = parquet_file.metadata

            # Check if ANY row group has statistics for the 'time' column
            has_time_stats = False

            for i in range(metadata.num_row_groups):
                row_group = metadata.row_group(i)

                for j in range(row_group.num_columns):
                    col = row_group.column(j)

                    if col.path_in_schema == 'time':
                        stats = col.statistics

                        if stats and stats.has_min_max:
                            has_time_stats = True
                            break

                if has_time_stats:
                    break

            if not has_time_stats:
                return False, "Missing 'time' column statistics"

            return True, None

        except Exception as e:
            return False, f"Error reading file: {e}"

    def rewrite_file_with_statistics(self, file_path: Path) -> bool:
        """
        Rewrite a Parquet file with statistics enabled

        Returns:
            True if successful
        """
        try:
            # Create backup path
            backup_path = file_path.with_suffix('.parquet.backup')
            temp_path = file_path.with_suffix('.parquet.tmp')

            # Read and rewrite using DuckDB
            con = duckdb.connect(':memory:')

            # Read existing file
            logger.info(f"  Reading {file_path.name}...")

            # Write with statistics enabled
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

            # Verify the new file has statistics
            has_stats, reason = self.check_file_has_statistics(temp_path)
            if not has_stats:
                logger.error(f"  ❌ Rewritten file still missing statistics: {reason}")
                temp_path.unlink()
                return False

            # Move original to backup
            file_path.rename(backup_path)

            # Move temp to original
            temp_path.rename(file_path)

            # Delete backup
            backup_path.unlink()

            logger.info(f"  ✅ Rewritten with statistics")
            return True

        except Exception as e:
            logger.error(f"  ❌ Failed to rewrite: {e}")

            # Cleanup temp file if it exists
            if temp_path.exists():
                temp_path.unlink()

            # Restore backup if it exists
            if backup_path.exists():
                backup_path.rename(file_path)

            return False

    def scan_and_backfill(
        self,
        database: Optional[str] = None,
        measurement: Optional[str] = None
    ):
        """
        Scan files and backfill statistics where needed

        Args:
            database: Optional database filter
            measurement: Optional measurement filter
        """
        logger.info("="*80)
        logger.info("PARQUET STATISTICS BACKFILL")
        logger.info("="*80)
        logger.info(f"Base path: {self.base_path}")
        logger.info(f"Dry run: {self.dry_run}")
        logger.info(f"Database filter: {database or 'all'}")
        logger.info(f"Measurement filter: {measurement or 'all'}")
        logger.info("")

        # Build search pattern
        if database and measurement:
            pattern = f"{database}/{measurement}/**/*.parquet"
        elif database:
            pattern = f"{database}/**/*.parquet"
        else:
            pattern = "**/*.parquet"

        # Find all Parquet files
        parquet_files = list(self.base_path.glob(pattern))
        logger.info(f"Found {len(parquet_files)} Parquet files")
        logger.info("")

        # Process each file
        for file_path in parquet_files:
            self.stats['total_files'] += 1
            file_size = file_path.stat().st_size
            self.stats['bytes_processed'] += file_size

            # Check if file has statistics
            has_stats, reason = self.check_file_has_statistics(file_path)

            relative_path = file_path.relative_to(self.base_path)

            if has_stats:
                self.stats['files_with_stats'] += 1
                logger.debug(f"✅ {relative_path} - Has statistics")
            else:
                self.stats['files_missing_stats'] += 1
                logger.info(f"⚠️  {relative_path} - Missing statistics ({reason})")

                if not self.dry_run:
                    # Rewrite file with statistics
                    if self.rewrite_file_with_statistics(file_path):
                        self.stats['files_rewritten'] += 1
                    else:
                        self.stats['files_failed'] += 1

        # Print summary
        logger.info("")
        logger.info("="*80)
        logger.info("SUMMARY")
        logger.info("="*80)
        logger.info(f"Total files scanned: {self.stats['total_files']}")
        logger.info(f"Files with statistics: {self.stats['files_with_stats']}")
        logger.info(f"Files missing statistics: {self.stats['files_missing_stats']}")

        if not self.dry_run:
            logger.info(f"Files rewritten: {self.stats['files_rewritten']}")
            logger.info(f"Files failed: {self.stats['files_failed']}")

        logger.info(f"Total data processed: {self.stats['bytes_processed'] / 1024 / 1024:.1f} MB")
        logger.info("")

        if self.dry_run and self.stats['files_missing_stats'] > 0:
            logger.info(f"💡 Run without --dry-run to backfill {self.stats['files_missing_stats']} files")


def main():
    parser = argparse.ArgumentParser(
        description='Backfill Parquet statistics to existing files'
    )
    parser.add_argument(
        '--base-path',
        default='./data',
        help='Base data directory (default: ./data)'
    )
    parser.add_argument(
        '--database',
        help='Filter by database name'
    )
    parser.add_argument(
        '--measurement',
        help='Filter by measurement name'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Check files only, don\'t modify'
    )

    args = parser.parse_args()

    backfill = ParquetStatisticsBackfill(
        base_path=args.base_path,
        dry_run=args.dry_run
    )

    backfill.scan_and_backfill(
        database=args.database,
        measurement=args.measurement
    )


if __name__ == '__main__':
    main()
