#!/usr/bin/env python3
"""
Re-compact existing Parquet files to convert timestamp types to int64.

This script reads existing Parquet files with pa.timestamp('us') columns
and rewrites them with int64 columns for the 'time' field. This provides
3-4x faster query performance for large result sets.

Usage:
    python3 scripts/recompact_timestamps.py --database telegraf --measurement cpu
    python3 scripts/recompact_timestamps.py --database telegraf --all
"""

import argparse
import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path
import logging
from datetime import datetime
import shutil
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def convert_timestamp_to_int64(table: pa.Table) -> pa.Table:
    """
    Convert timestamp columns to int64 (microseconds).

    Args:
        table: Arrow table with potential timestamp columns

    Returns:
        Arrow table with timestamp columns converted to int64
    """
    new_fields = []
    new_arrays = []

    for i, field in enumerate(table.schema):
        array = table.column(i)

        if pa.types.is_timestamp(field.type) and field.name == 'time':
            # Convert timestamp to int64 (microseconds since epoch)
            logger.debug(f"Converting column '{field.name}' from timestamp to int64")

            # Cast timestamp to int64 (microseconds)
            int_array = array.cast(pa.int64())

            new_fields.append(pa.field(field.name, pa.int64()))
            new_arrays.append(int_array)
        else:
            # Keep other columns as-is
            new_fields.append(field)
            new_arrays.append(array)

    # Create new schema and table
    new_schema = pa.schema(new_fields)
    new_table = pa.Table.from_arrays(new_arrays, schema=new_schema)

    return new_table


def recompact_parquet_file(file_path: Path, dry_run: bool = False) -> bool:
    """
    Recompact a single Parquet file, converting timestamps to int64.

    Args:
        file_path: Path to Parquet file
        dry_run: If True, don't actually write changes

    Returns:
        True if file was modified, False if no changes needed
    """
    try:
        # Read existing file
        table = pq.read_table(file_path)

        # Check if 'time' column is timestamp type
        time_field = None
        for field in table.schema:
            if field.name == 'time':
                time_field = field
                break

        if not time_field or not pa.types.is_timestamp(time_field.type):
            logger.debug(f"Skipping {file_path} - 'time' is not timestamp type")
            return False

        logger.info(f"Converting {file_path} ({table.num_rows:,} rows)")

        if dry_run:
            logger.info(f"[DRY RUN] Would convert {file_path}")
            return True

        # Convert timestamps to int64
        new_table = convert_timestamp_to_int64(table)

        # Create backup
        backup_path = file_path.with_suffix('.parquet.backup')
        shutil.copy2(file_path, backup_path)

        try:
            # Write new file with same compression settings
            pq.write_table(
                new_table,
                file_path,
                compression='lz4',  # Use LZ4 compression
                use_dictionary=True,
                write_statistics=True,
                data_page_version='2.0'
            )

            # Verify the new file
            verify_table = pq.read_table(file_path)
            if verify_table.num_rows != table.num_rows:
                raise ValueError(f"Row count mismatch: {verify_table.num_rows} != {table.num_rows}")

            # Remove backup on success
            backup_path.unlink()

            logger.info(f"✅ Converted {file_path}")
            return True

        except Exception as e:
            # Restore backup on failure
            logger.error(f"Failed to write {file_path}, restoring backup: {e}")
            shutil.copy2(backup_path, file_path)
            backup_path.unlink()
            raise

    except Exception as e:
        logger.error(f"Failed to process {file_path}: {e}")
        return False


def recompact_measurement(data_dir: Path, database: str, measurement: str, dry_run: bool = False):
    """
    Recompact all Parquet files for a measurement.

    Args:
        data_dir: Base data directory
        database: Database name
        measurement: Measurement name
        dry_run: If True, don't actually write changes
    """
    measurement_path = data_dir / database / measurement

    if not measurement_path.exists():
        logger.error(f"Measurement path does not exist: {measurement_path}")
        return

    # Find all Parquet files
    parquet_files = list(measurement_path.rglob('*.parquet'))

    if not parquet_files:
        logger.warning(f"No Parquet files found in {measurement_path}")
        return

    logger.info(f"Found {len(parquet_files):,} Parquet files for {database}.{measurement}")

    converted = 0
    skipped = 0
    failed = 0

    for i, file_path in enumerate(parquet_files, 1):
        if i % 100 == 0:
            logger.info(f"Progress: {i}/{len(parquet_files)} files processed")

        try:
            if recompact_parquet_file(file_path, dry_run=dry_run):
                converted += 1
            else:
                skipped += 1
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
            failed += 1

    logger.info(f"\n=== Summary for {database}.{measurement} ===")
    logger.info(f"Total files: {len(parquet_files):,}")
    logger.info(f"Converted: {converted:,}")
    logger.info(f"Skipped: {skipped:,}")
    logger.info(f"Failed: {failed:,}")


def recompact_database(data_dir: Path, database: str, dry_run: bool = False):
    """
    Recompact all measurements in a database.

    Args:
        data_dir: Base data directory
        database: Database name
        dry_run: If True, don't actually write changes
    """
    db_path = data_dir / database

    if not db_path.exists():
        logger.error(f"Database path does not exist: {db_path}")
        return

    # Find all measurements (subdirectories)
    measurements = [d for d in db_path.iterdir() if d.is_dir()]

    if not measurements:
        logger.warning(f"No measurements found in {db_path}")
        return

    logger.info(f"Found {len(measurements)} measurements in database '{database}'")

    for measurement_dir in measurements:
        measurement = measurement_dir.name
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing measurement: {measurement}")
        logger.info(f"{'='*60}")

        recompact_measurement(data_dir, database, measurement, dry_run=dry_run)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Re-compact Parquet files to convert timestamp columns to int64'
    )
    parser.add_argument(
        '--data-dir',
        default='./data/arc',
        help='Base data directory (default: ./data/arc)'
    )
    parser.add_argument(
        '--database',
        required=True,
        help='Database name (e.g., telegraf)'
    )
    parser.add_argument(
        '--measurement',
        help='Measurement name (e.g., cpu). If not specified, processes all measurements.'
    )
    parser.add_argument(
        '--all',
        action='store_true',
        help='Process all measurements in the database'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be done without making changes'
    )

    args = parser.parse_args()

    data_dir = Path(args.data_dir)

    if not data_dir.exists():
        logger.error(f"Data directory does not exist: {data_dir}")
        sys.exit(1)

    if args.dry_run:
        logger.info("DRY RUN MODE - No files will be modified")

    if args.measurement:
        recompact_measurement(data_dir, args.database, args.measurement, dry_run=args.dry_run)
    elif args.all:
        recompact_database(data_dir, args.database, dry_run=args.dry_run)
    else:
        logger.error("Must specify either --measurement or --all")
        sys.exit(1)
