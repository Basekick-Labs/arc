#!/usr/bin/env python3
"""
Manual Compaction Script

Scans the entire Arc dataset and triggers compaction for all eligible partitions.
Useful for:
- One-time cleanup of accumulated uncompacted files
- Testing compaction logic
- Recovery after compaction downtime

Usage:
    python scripts/manual_compaction.py --dry-run    # Preview what would be compacted
    python scripts/manual_compaction.py              # Run actual compaction
    python scripts/manual_compaction.py --database telegraf  # Compact specific database
"""

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

# Add parent directory to path for imports
script_dir = Path(__file__).parent
arc_root = script_dir.parent

# If script is in /app (Docker), arc_root should be /app not /
if str(arc_root) == '/':
    arc_root = Path('/app')

sys.path.insert(0, str(arc_root))

try:
    from storage.compaction import CompactionManager
    from storage.local_backend import LocalBackend
    from api.compaction_lock import CompactionLock
    from config_loader import load_config
except ModuleNotFoundError as e:
    print(f"Error: Cannot import Arc modules. Please run this script from the Arc root directory.")
    print(f"Current directory: {os.getcwd()}")
    print(f"Arc root should be: {arc_root}")
    print(f"\nTry running: cd {arc_root} && python3 scripts/manual_compaction.py ...")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CompactionScanner:
    """Scans storage and triggers compaction for eligible partitions."""

    def __init__(
        self,
        storage_backend,
        config: Dict,
        dry_run: bool = False,
        target_database: str = None
    ):
        self.storage_backend = storage_backend
        self.config = config
        self.dry_run = dry_run
        self.target_database = target_database

        # Stats tracking
        self.total_partitions_scanned = 0
        self.total_eligible_partitions = 0
        self.total_uncompacted_files = 0
        self.total_compacted_files = 0
        self.compaction_jobs_completed = 0
        self.compaction_jobs_failed = 0

    async def scan_and_compact(self) -> Dict:
        """
        Scan all databases/measurements and trigger compaction for eligible partitions.

        Returns summary statistics.
        """
        logger.info("=" * 80)
        logger.info("Starting manual compaction scan")
        logger.info("=" * 80)
        logger.info(f"Dry run: {self.dry_run}")
        logger.info(f"Target database: {self.target_database or 'ALL'}")
        logger.info("")

        # Discover all databases and measurements
        databases_measurements = await self._discover_databases_and_measurements()

        if not databases_measurements:
            logger.warning("No databases or measurements found!")
            return self._get_summary()

        logger.info(f"Found {len(databases_measurements)} database/measurement combinations")
        logger.info("")

        # Scan each measurement for eligible partitions
        for database, measurement in databases_measurements:
            # Skip if targeting specific database
            if self.target_database and database != self.target_database:
                continue

            await self._scan_measurement(database, measurement)

        # Print summary
        self._print_summary()

        return self._get_summary()

    async def _discover_databases_and_measurements(self) -> List[Tuple[str, str]]:
        """Discover all database/measurement combinations in storage."""
        measurements = []

        try:
            if hasattr(self.storage_backend, 'base_path'):
                # Local filesystem
                base_path = Path(self.storage_backend.base_path)

                if not base_path.exists():
                    logger.error(f"Base path does not exist: {base_path}")
                    return []

                # Scan for databases
                for db_dir in base_path.iterdir():
                    if db_dir.is_dir() and not db_dir.name.startswith('.'):
                        database = db_dir.name

                        # Scan for measurements in this database
                        for meas_dir in db_dir.iterdir():
                            if meas_dir.is_dir() and not meas_dir.name.startswith('.'):
                                measurement = meas_dir.name
                                measurements.append((database, measurement))

            logger.info(f"Discovered databases/measurements: {measurements[:5]}..." if len(measurements) > 5 else f"Discovered: {measurements}")

        except Exception as e:
            logger.error(f"Failed to discover databases/measurements: {e}", exc_info=True)

        return measurements

    async def _scan_measurement(self, database: str, measurement: str):
        """Scan a single measurement for eligible partitions."""
        logger.info(f"Scanning {database}.{measurement}")

        try:
            # Find all hour partitions for this measurement
            partitions = await self._find_partitions(database, measurement)

            if not partitions:
                logger.info(f"  No partitions found for {database}.{measurement}")
                return

            logger.info(f"  Found {len(partitions)} partitions")

            # Check each partition for compaction eligibility
            eligible_partitions = []

            for partition_info in partitions:
                partition_path = partition_info['path']
                files = partition_info['files']

                self.total_partitions_scanned += 1

                # Separate compacted and uncompacted files
                compacted_files = [f for f in files if '_compacted.parquet' in f]
                uncompacted_files = [f for f in files if '_compacted.parquet' not in f]

                self.total_uncompacted_files += len(uncompacted_files)
                self.total_compacted_files += len(compacted_files)

                # Check compaction criteria (min_files from config)
                min_files = self.config.get('compaction', {}).get('min_files', 50)

                should_compact = False
                reason = ""

                if not compacted_files and len(files) >= min_files:
                    # First time compaction
                    should_compact = True
                    reason = f"first compaction ({len(files)} files)"
                elif compacted_files and len(uncompacted_files) >= min_files:
                    # Re-compaction
                    should_compact = True
                    reason = f"re-compaction ({len(compacted_files)} compacted + {len(uncompacted_files)} uncompacted)"

                if should_compact:
                    self.total_eligible_partitions += 1
                    eligible_partitions.append({
                        'partition_path': partition_path,
                        'file_count': len(files),
                        'compacted_count': len(compacted_files),
                        'uncompacted_count': len(uncompacted_files),
                        'reason': reason,
                        'files': files  # Add full files list for compaction
                    })
                    logger.info(f"    ✓ {partition_path}: {reason}")

            # Trigger compaction for eligible partitions
            if eligible_partitions:
                if self.dry_run:
                    logger.info(f"  [DRY RUN] Would compact {len(eligible_partitions)} partitions")
                else:
                    await self._compact_partitions(database, measurement, eligible_partitions)
            else:
                logger.info(f"  No eligible partitions for compaction")

        except Exception as e:
            logger.error(f"Failed to scan {database}.{measurement}: {e}", exc_info=True)

        logger.info("")

    async def _find_partitions(self, database: str, measurement: str) -> List[Dict]:
        """Find all hour partitions for a measurement."""
        partitions = {}

        try:
            if hasattr(self.storage_backend, 'base_path'):
                # Local filesystem
                base_path = Path(self.storage_backend.base_path)
                db_meas_path = base_path / database / measurement

                if not db_meas_path.exists():
                    return []

                # Find all parquet files
                for file_path in db_meas_path.rglob('*.parquet'):
                    relative = file_path.relative_to(base_path / database)
                    parts = str(relative).split('/')

                    # Parse path: measurement/year/month/day/hour/file.parquet
                    if len(parts) >= 5:
                        meas, year, month, day, hour = parts[0], parts[1], parts[2], parts[3], parts[4]

                        if meas != measurement:
                            continue

                        # Build partition path
                        partition_path = f"{measurement}/{year}/{month}/{day}/{hour}"

                        if partition_path not in partitions:
                            partitions[partition_path] = []

                        partitions[partition_path].append(str(relative))

        except Exception as e:
            logger.error(f"Failed to find partitions for {database}.{measurement}: {e}", exc_info=True)

        # Convert to list format
        result = []
        for path, files in partitions.items():
            result.append({
                'path': path,
                'files': files
            })

        return result

    async def _compact_partitions(
        self,
        database: str,
        measurement: str,
        eligible_partitions: List[Dict]
    ):
        """Trigger compaction for eligible partitions."""
        logger.info(f"  Compacting {len(eligible_partitions)} partitions...")

        # Create compaction manager
        compaction_config = self.config.get('compaction', {})
        lock_manager = CompactionLock()  # No args needed

        compaction_manager = CompactionManager(
            storage_backend=self.storage_backend,
            lock_manager=lock_manager,
            database=database,
            min_files=compaction_config.get('min_files', 50),
            target_size_mb=compaction_config.get('target_file_size_mb', 512),
            max_concurrent=1  # Run serially for manual script
        )

        # Compact each partition
        for partition_info in eligible_partitions:
            partition_path = partition_info['partition_path']
            files = partition_info.get('files', [])

            try:
                logger.info(f"    Compacting {partition_path}...")

                # Trigger compaction
                result = await compaction_manager.compact_partition(
                    database=database,
                    measurement=measurement,
                    partition_path=partition_path,
                    files=files
                )

                if result:
                    self.compaction_jobs_completed += 1
                    logger.info(f"    ✓ Compacted {partition_path}")
                else:
                    self.compaction_jobs_failed += 1
                    logger.warning(f"    ✗ Failed to compact {partition_path}")

            except Exception as e:
                self.compaction_jobs_failed += 1
                logger.error(f"    ✗ Error compacting {partition_path}: {e}", exc_info=True)

    def _get_summary(self) -> Dict:
        """Get summary statistics."""
        return {
            'total_partitions_scanned': self.total_partitions_scanned,
            'total_eligible_partitions': self.total_eligible_partitions,
            'total_uncompacted_files': self.total_uncompacted_files,
            'total_compacted_files': self.total_compacted_files,
            'compaction_jobs_completed': self.compaction_jobs_completed,
            'compaction_jobs_failed': self.compaction_jobs_failed,
            'dry_run': self.dry_run
        }

    def _print_summary(self):
        """Print summary statistics."""
        logger.info("")
        logger.info("=" * 80)
        logger.info("COMPACTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Partitions scanned:        {self.total_partitions_scanned:,}")
        logger.info(f"Eligible for compaction:   {self.total_eligible_partitions:,}")
        logger.info(f"Uncompacted files found:   {self.total_uncompacted_files:,}")
        logger.info(f"Compacted files found:     {self.total_compacted_files:,}")
        logger.info("")
        if not self.dry_run:
            logger.info(f"Compaction jobs completed: {self.compaction_jobs_completed:,}")
            logger.info(f"Compaction jobs failed:    {self.compaction_jobs_failed:,}")
            logger.info("")
            if self.compaction_jobs_failed > 0:
                logger.warning(f"⚠️  {self.compaction_jobs_failed} compaction jobs failed! Check logs above.")
            else:
                logger.info("✅ All compaction jobs completed successfully!")
        else:
            logger.info("[DRY RUN] No compaction was performed")
        logger.info("=" * 80)


async def main():
    parser = argparse.ArgumentParser(
        description='Manually scan and compact Arc dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Preview what would be compacted (safe, no changes)
  python scripts/manual_compaction.py --dry-run

  # Actually run compaction
  python scripts/manual_compaction.py

  # Compact only telegraf database
  python scripts/manual_compaction.py --database telegraf

  # Compact with custom config file
  python scripts/manual_compaction.py --config /path/to/arc.conf

  # Compact with custom base path
  python scripts/manual_compaction.py --base-path /var/lib/docker/volumes/arc_arc-data/_data/arc
        """
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview what would be compacted without making changes'
    )
    parser.add_argument(
        '--database',
        type=str,
        help='Only compact this database (e.g., telegraf, sitrack)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='arc.conf',
        help='Path to arc.conf configuration file (default: arc.conf)'
    )
    parser.add_argument(
        '--base-path',
        type=str,
        help='Override storage base path (default: from config)'
    )
    parser.add_argument(
        '--min-files',
        type=int,
        help='Override minimum files threshold for compaction (default: from config, usually 50)'
    )

    args = parser.parse_args()

    # Load configuration
    try:
        arc_config = load_config(args.config)
        # Convert ArcConfig to dict for easier access
        config = {
            'storage': {
                'backend': arc_config.get('storage', 'backend', default='local'),
                'base_path': arc_config.get('storage', 'base_path', default='./data/arc')
            },
            'compaction': {
                'min_files': arc_config.get('compaction', 'min_files', default=50),
                'target_file_size_mb': arc_config.get('compaction', 'target_file_size_mb', default=512)
            }
        }
    except Exception as e:
        logger.error(f"Failed to load config from {args.config}: {e}")
        logger.info("Using default configuration...")
        config = {
            'storage': {'backend': 'local', 'base_path': './data/arc'},
            'compaction': {'min_files': 50, 'target_file_size_mb': 512}
        }

    # Override base_path if provided
    if args.base_path:
        config['storage']['base_path'] = args.base_path

    # Override min_files if provided
    if args.min_files:
        config['compaction']['min_files'] = args.min_files
        logger.info(f"Overriding min_files threshold to: {args.min_files}")

    # Initialize storage backend
    base_path = config['storage']['base_path']

    logger.info(f"Using base path: {base_path}")

    if not os.path.exists(base_path):
        logger.error(f"Base path does not exist: {base_path}")
        logger.error("Please check your configuration or provide --base-path")
        sys.exit(1)

    storage_backend = LocalBackend(
        base_path=base_path,
        database='default'  # Will be overridden per-measurement
    )

    # Create scanner and run
    scanner = CompactionScanner(
        storage_backend=storage_backend,
        config=config,
        dry_run=args.dry_run,
        target_database=args.database
    )

    try:
        summary = await scanner.scan_and_compact()

        # Exit with error code if any jobs failed
        if summary['compaction_jobs_failed'] > 0:
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\n\nInterrupted by user. Exiting...")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
