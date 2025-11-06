"""
Compaction Module for Arc Core

Merges small Parquet files into larger files to improve query performance.
Supports tiered compaction strategy:
- Hourly compaction (default): Compacts files within hourly partitions
- Daily compaction (tier 2): Compacts hourly files into daily files
- Weekly compaction (tier 3, future): Compacts daily files into weekly files
- Monthly compaction (tier 4, future): Compacts weekly files into monthly files

Storage Structure:
- Full path: {bucket}/{database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
- Storage backend handles database prefix automatically
- Compaction operates within a single database namespace
- All paths are relative to the database (measurement/year/month/day/hour/)
"""

import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import asyncio

logger = logging.getLogger(__name__)


class CompactionJob:
    """
    Represents a single compaction job for a partition.
    """

    def __init__(
        self,
        measurement: str,
        partition_path: str,
        files: List[str],
        storage_backend,
        database: str = "default",
        target_size_mb: int = 512
    ):
        """
        Initialize compaction job

        Args:
            measurement: Measurement name (e.g., 'cpu')
            partition_path: Partition path (e.g., 'cpu/2025/10/08/14')
            files: List of file keys to compact
            storage_backend: Storage backend instance
            database: Database namespace
            target_size_mb: Target size for compacted file
        """
        self.measurement = measurement
        self.partition_path = partition_path
        self.files = files
        self.storage_backend = storage_backend
        self.database = database
        self.target_size_mb = target_size_mb

        # Job metadata
        self.job_id = f"{partition_path.replace('/', '_')}_{int(datetime.now().timestamp())}"
        self.started_at = None
        self.completed_at = None
        self.status = "pending"  # pending, running, completed, failed
        self.error = None

        # Metrics
        self.files_compacted = 0
        self.bytes_before = 0
        self.bytes_after = 0
        self.duration_seconds = 0

    async def run(self) -> bool:
        """
        Execute compaction job

        Returns:
            True if successful, False otherwise
        """
        self.started_at = datetime.now()
        self.status = "running"

        logger.info(
            f"Starting compaction job {self.job_id}: "
            f"{len(self.files)} files in database '{self.database}', partition '{self.partition_path}'"
        )

        try:
            # Download files to temp directory
            temp_dir = Path(f"./data/compaction/{self.job_id}")
            temp_dir.mkdir(parents=True, exist_ok=True)

            local_files = []
            for file_key in self.files:
                local_path = temp_dir / Path(file_key).name
                await self._download_file(file_key, local_path)
                local_files.append(local_path)
                self.bytes_before += local_path.stat().st_size

            logger.info(
                f"Downloaded {len(local_files)} files "
                f"({self.bytes_before / 1024 / 1024:.1f} MB)"
            )

            # Compact using DuckDB
            compacted_file = await self._compact_files(local_files, temp_dir)
            self.bytes_after = compacted_file.stat().st_size

            logger.info(
                f"Compacted to {compacted_file.name} "
                f"({self.bytes_after / 1024 / 1024:.1f} MB)"
            )

            # Upload compacted file
            compacted_key = f"{self.partition_path}/{compacted_file.name}"
            await self._upload_file(compacted_file, compacted_key)

            # Delete old files from storage
            await self._delete_old_files()

            # Cleanup temp directory
            self._cleanup_temp(temp_dir)

            # Update metrics
            self.files_compacted = len(self.files)
            self.status = "completed"
            self.completed_at = datetime.now()
            self.duration_seconds = (self.completed_at - self.started_at).total_seconds()

            compression_ratio = (
                (1 - self.bytes_after / self.bytes_before) * 100
                if self.bytes_before > 0 else 0
            )

            logger.info(
                f"Compaction job {self.job_id} completed: "
                f"{self.files_compacted} files → 1 file, "
                f"{self.bytes_before / 1024 / 1024:.1f} MB → "
                f"{self.bytes_after / 1024 / 1024:.1f} MB "
                f"({compression_ratio:.1f}% compression), "
                f"duration: {self.duration_seconds:.1f}s"
            )

            return True

        except Exception as e:
            self.status = "failed"
            self.error = str(e)
            self.completed_at = datetime.now()
            logger.error(f"Compaction job {self.job_id} failed: {e}", exc_info=True)
            return False

    async def _download_file(self, file_key: str, local_path: Path):
        """Download file from storage backend"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self.storage_backend.download_file,
            file_key,
            str(local_path)
        )

    async def _compact_files(self, local_files: List[Path], temp_dir: Path) -> Path:
        """
        Compact multiple Parquet files into one using DuckDB.

        Args:
            local_files: List of local Parquet file paths
            temp_dir: Temporary directory for output

        Returns:
            Path to compacted file
        """
        import duckdb

        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = temp_dir / f"{self.measurement}_{timestamp}_compacted.parquet"

        # Use DuckDB to read all files and write compacted file
        loop = asyncio.get_event_loop()

        def _compact():
            con = duckdb.connect(':memory:')

            # Validate each file first and filter out corrupted ones
            valid_files = []
            for file_path in local_files:
                try:
                    # Try to read file metadata to validate it
                    con.execute(f"SELECT COUNT(*) FROM read_parquet('{file_path}')").fetchone()
                    valid_files.append(file_path)
                except Exception as e:
                    logger.error(f"Skipping corrupted file {file_path.name}: {e}")
                    # Move corrupted file to quarantine
                    try:
                        quarantine_dir = temp_dir / "quarantine"
                        quarantine_dir.mkdir(exist_ok=True)
                        corrupted_path = quarantine_dir / file_path.name
                        file_path.rename(corrupted_path)
                        logger.info(f"Moved corrupted file to quarantine: {corrupted_path}")
                    except Exception as move_error:
                        logger.warning(f"Failed to quarantine corrupted file: {move_error}")

            if not valid_files:
                raise ValueError(f"No valid parquet files found in {temp_dir}")

            logger.info(f"Validated {len(valid_files)}/{len(local_files)} files for compaction")

            # Build file list for read_parquet
            # Use explicit list to avoid reading any other .parquet files in temp_dir (like partial output files)
            if len(valid_files) == 1:
                file_pattern = str(valid_files[0])
            else:
                # Use list syntax: [file1, file2, ...] to explicitly specify which files to read
                file_list = '[' + ', '.join([f"'{str(f)}'" for f in valid_files]) + ']'
                file_pattern = file_list

            # Standard compaction query
            # Note: ORDER BY removed due to DuckDB "invalid TType" error with large file counts + union_by_name
            # Data is already mostly time-ordered within hourly partitions
            sql_query = f"SELECT * FROM read_parquet({file_pattern}, union_by_name=true)"

            # Write compacted file with optimized settings
            # Use union_by_name=true to handle schema evolution (missing columns filled with NULL)
            con.execute(f"""
                COPY (
                    {sql_query}
                ) TO '{output_file}' (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD,
                    COMPRESSION_LEVEL 3,
                    ROW_GROUP_SIZE 122880  -- ~120K rows per row group
                )
            """)

            con.close()

        await loop.run_in_executor(None, _compact)

        return output_file

    async def _upload_file(self, local_path: Path, file_key: str):
        """Upload compacted file to storage backend"""
        await self.storage_backend.upload_file(local_path, file_key)
        logger.info(f"Uploaded compacted file: {file_key}")

    async def _delete_old_files(self):
        """Delete old small files from storage backend"""
        loop = asyncio.get_event_loop()

        for file_key in self.files:
            try:
                await loop.run_in_executor(
                    None,
                    self.storage_backend.delete_file,
                    file_key
                )
                logger.debug(f"Deleted old file: {file_key}")
            except Exception as e:
                logger.warning(f"Failed to delete old file {file_key}: {e}")

    def _cleanup_temp(self, temp_dir: Path):
        """Cleanup temporary directory"""
        try:
            import shutil
            shutil.rmtree(temp_dir)
            logger.debug(f"Cleaned up temp directory: {temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp directory {temp_dir}: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get job statistics"""
        return {
            'job_id': self.job_id,
            'database': self.database,
            'measurement': self.measurement,
            'partition_path': self.partition_path,
            'status': self.status,
            'files_compacted': self.files_compacted,
            'bytes_before': self.bytes_before,
            'bytes_after': self.bytes_after,
            'compression_ratio': (
                (1 - self.bytes_after / self.bytes_before)
                if self.bytes_before > 0 else 0
            ),
            'duration_seconds': self.duration_seconds,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error': self.error
        }


class CompactionManager:
    """
    Manages compaction jobs across all measurements.
    Supports both hourly compaction (default) and tiered compaction (daily, weekly, monthly).
    """

    def __init__(
        self,
        storage_backend,
        lock_manager,
        database: str = "default",
        min_age_hours: int = 1,
        min_files: int = 10,
        target_size_mb: int = 512,
        max_concurrent: int = 2,
        tiers: Optional[List] = None
    ):
        """
        Initialize compaction manager

        Args:
            storage_backend: Storage backend instance
            lock_manager: CompactionLock instance
            database: Database namespace (default: "default")
            min_age_hours: Don't compact partitions younger than this
            min_files: Only compact partitions with at least this many files
            target_size_mb: Target size for compacted files
            max_concurrent: Max concurrent compaction jobs
            tiers: List of CompactionTier instances for tiered compaction (optional)
        """
        self.storage_backend = storage_backend
        self.lock_manager = lock_manager
        self.database = database
        self.min_age_hours = min_age_hours
        self.min_files = min_files
        self.target_size_mb = target_size_mb
        self.max_concurrent = max_concurrent

        # Tiered compaction support
        self.tiers = tiers or []
        if self.tiers:
            enabled_tiers = [tier.get_tier_name() for tier in self.tiers if tier.enabled]
            logger.info(
                f"Compaction manager initialized with {len(enabled_tiers)} tier(s): "
                f"{', '.join(enabled_tiers)}"
            )
        else:
            logger.info("Compaction manager initialized (hourly compaction only)")

        # Active jobs
        self.active_jobs: Dict[str, CompactionJob] = {}
        self.job_history: List[Dict[str, Any]] = []

        # Metrics
        self.total_jobs_completed = 0
        self.total_jobs_failed = 0
        self.total_files_compacted = 0
        self.total_bytes_saved = 0

        # Lock for storage backend database switching
        # CRITICAL: Multiple concurrent jobs share the same storage_backend instance.
        # The storage backend's `database` property is modified per-job to support
        # multi-database compaction. This lock serializes jobs to prevent race conditions
        # where Job A's file operations might use Job B's database value.
        #
        # NOTE: This effectively limits compaction to 1 job at a time, regardless of
        # max_concurrent setting. This is a necessary trade-off until storage backends
        # support database-as-parameter instead of mutable state.
        self._storage_lock = asyncio.Lock()

    async def find_candidates(self) -> List[Dict[str, Any]]:
        """
        Find partitions that are candidates for compaction across ALL databases.
        Includes both hourly compaction and tiered compaction candidates.

        Returns:
            List of candidate partitions with metadata
        """
        candidates = []
        cutoff_time = datetime.utcnow() - timedelta(hours=self.min_age_hours)

        logger.info("Scanning for compaction candidates...")

        # List all measurements across all databases
        measurements = await self._list_measurements()

        for database, measurement in measurements:
            # 1. Hourly compaction candidates (default behavior)
            hourly_candidates = await self._find_hourly_candidates(
                database, measurement, cutoff_time
            )
            candidates.extend(hourly_candidates)

            # 2. Tiered compaction candidates (daily, weekly, monthly)
            for tier in self.tiers:
                if tier.enabled:
                    tier_candidates = await tier.find_candidates(database, measurement)
                    candidates.extend(tier_candidates)

        logger.info(f"Found {len(candidates)} compaction candidates")
        return candidates

    async def _find_hourly_candidates(
        self,
        database: str,
        measurement: str,
        cutoff_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Find hourly partition candidates for compaction.

        Args:
            database: Database name
            measurement: Measurement name
            cutoff_time: Only return partitions older than this time

        Returns:
            List of hourly candidates
        """
        candidates = []

        # List all hour partitions for this measurement
        partitions = await self._list_partitions(database, measurement, cutoff_time)

        for partition_info in partitions:
            partition_path = partition_info['path']
            files = partition_info['files']

            # Separate compacted and uncompacted files
            compacted_files = [f for f in files if '_compacted.parquet' in f]
            uncompacted_files = [f for f in files if '_compacted.parquet' not in f]

            # Check if meets criteria for compaction
            # Case 1: No compacted files yet, and has enough total files
            # Case 2: Has compacted files, but also has many new uncompacted files
            should_compact = False
            files_to_compact = []

            if not compacted_files and len(files) >= self.min_files:
                # First time compaction - compact all files
                should_compact = True
                files_to_compact = files
                logger.debug(
                    f"{database}/{partition_path}: First compaction - {len(files)} files"
                )
            elif compacted_files and len(uncompacted_files) >= self.min_files:
                # Re-compaction - has compacted files + many new uncompacted files
                # Compact ALL files (compacted + uncompacted) into new larger compacted file
                should_compact = True
                files_to_compact = files
                logger.info(
                    f"{database}/{partition_path}: Re-compaction - "
                    f"{len(compacted_files)} compacted + {len(uncompacted_files)} uncompacted files"
                )

            if should_compact:
                candidates.append({
                    'database': database,
                    'measurement': measurement,
                    'partition_path': partition_path,
                    'file_count': len(files_to_compact),
                    'files': files_to_compact,
                    'tier': 'hourly'
                })

                logger.info(
                    f"Candidate: {database}/{partition_path} ({len(files_to_compact)} files)"
                )

        return candidates

    async def compact_partition(
        self,
        database: str,
        measurement: str,
        partition_path: str,
        files: List[str]
    ) -> bool:
        """
        Compact a single partition

        Args:
            database: Database name
            measurement: Measurement name
            partition_path: Partition path (relative to database)
            files: List of files to compact (relative to database)

        Returns:
            True if successful
        """
        # Lock key includes database to avoid conflicts
        lock_key = f"{database}/{partition_path}"

        # Try to acquire lock
        if not self.lock_manager.acquire_lock(lock_key):
            logger.info(f"Partition {lock_key} already locked, skipping")
            return False

        try:
            # CRITICAL FIX: Do NOT modify shared storage_backend.database property!
            # Multiple concurrent jobs would race and corrupt each other's database context.
            # Instead, we temporarily set the database ONLY for this specific job instance.

            # Save original database value
            original_database = getattr(self.storage_backend, 'database', None)

            # Acquire lock to safely modify database property for the ENTIRE job duration
            async with self._storage_lock:
                # Set database for this operation
                if hasattr(self.storage_backend, 'database'):
                    self.storage_backend.database = database

                # Create and run job
                # IMPORTANT: job.run() must complete INSIDE the lock to prevent race conditions
                job = CompactionJob(
                    measurement=measurement,
                    partition_path=partition_path,
                    files=files,
                    storage_backend=self.storage_backend,
                    database=database,
                    target_size_mb=self.target_size_mb
                )

                self.active_jobs[job.job_id] = job

                # Run job with database property locked
                success = await job.run()

                # Restore original database before releasing lock
                if hasattr(self.storage_backend, 'database'):
                    self.storage_backend.database = original_database

            # Update metrics (outside lock)
            if success:
                self.total_jobs_completed += 1
                self.total_files_compacted += job.files_compacted
                self.total_bytes_saved += (job.bytes_before - job.bytes_after)
            else:
                self.total_jobs_failed += 1

            # Move to history
            self.job_history.append(job.get_stats())
            del self.active_jobs[job.job_id]

            return success

        finally:
            # Always release lock
            self.lock_manager.release_lock(lock_key)

    async def run_compaction_cycle(self):
        """
        Run one compaction cycle - find and compact eligible partitions across ALL databases
        """
        logger.info("Starting compaction cycle")

        # Find candidates
        candidates = await self.find_candidates()

        if not candidates:
            logger.info("No compaction candidates found")
            return

        # Compact candidates (respecting max_concurrent limit)
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def _compact_with_limit(candidate):
            async with semaphore:
                return await self.compact_partition(
                    candidate['database'],
                    candidate['measurement'],
                    candidate['partition_path'],
                    candidate['files']
                )

        tasks = [_compact_with_limit(c) for c in candidates]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        successes = 0
        failures = 0

        # Log exceptions properly
        for i, result in enumerate(results):
            if result is True:
                successes += 1
            elif isinstance(result, Exception):
                failures += 1
                candidate = candidates[i]
                logger.error(
                    f"Compaction failed for {candidate['database']}/{candidate['partition_path']}: {result}",
                    exc_info=result
                )
            else:
                failures += 1

        logger.info(
            f"Compaction cycle complete: "
            f"{successes} successful, {failures} failed"
        )

    async def _list_measurements(self) -> List[str]:
        """
        List all measurements across ALL databases in storage.

        Returns:
            List of tuples (database, measurement)
        """
        loop = asyncio.get_event_loop()

        def _list():
            measurements = []

            try:
                # Check if storage backend is local or S3
                if hasattr(self.storage_backend, 'base_path'):
                    # Local filesystem - scan all database directories
                    from pathlib import Path
                    base_path = Path(self.storage_backend.base_path)

                    if base_path.exists():
                        for db_dir in base_path.iterdir():
                            if db_dir.is_dir() and not db_dir.name.startswith('.'):
                                database = db_dir.name
                                # Scan for measurements in this database
                                for meas_dir in db_dir.iterdir():
                                    if meas_dir.is_dir() and not meas_dir.name.startswith('.'):
                                        measurement = meas_dir.name
                                        measurements.append((database, measurement))

                    logger.info(f"Found {len(measurements)} measurements across all databases: {measurements}")

                elif hasattr(self.storage_backend, 's3_client'):
                    # S3-based storage - scan all databases in bucket root
                    bucket = self.storage_backend.bucket
                    paginator = self.storage_backend.s3_client.get_paginator('list_objects_v2')

                    # List all objects in bucket (no prefix)
                    for page in paginator.paginate(Bucket=bucket, Prefix='', MaxKeys=10000):
                        if 'Contents' in page:
                            for obj in page['Contents']:
                                key = obj['Key']
                                parts = key.split('/')
                                # Path structure: database/measurement/year/month/day/hour/file.parquet
                                if len(parts) >= 6:
                                    database = parts[0]
                                    measurement = parts[1]

                                    # Skip numeric directories (year partitions)
                                    if not measurement.isdigit():
                                        db_meas = (database, measurement)
                                        if db_meas not in measurements:
                                            measurements.append(db_meas)

                    logger.info(f"Found {len(measurements)} measurements across all databases: {measurements}")

                else:
                    # Fallback to original behavior for current database only
                    objects = self.storage_backend.list_objects(prefix='', max_keys=10000)
                    for obj in objects:
                        parts = obj.split('/')
                        if len(parts) >= 5:
                            measurement = parts[0]
                            if (self.database, measurement) not in measurements:
                                measurements.append((self.database, measurement))

                    logger.info(f"Found {len(measurements)} measurements in database '{self.database}': {measurements}")

            except Exception as e:
                logger.error(f"Failed to list measurements: {e}")

            return measurements

        return await loop.run_in_executor(None, _list)

    async def _list_partitions(
        self,
        database: str,
        measurement: str,
        cutoff_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        List hour partitions for a measurement older than cutoff_time.

        Storage path structure: {database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet

        Args:
            database: Database name (e.g., 'systems')
            measurement: Measurement name (e.g., 'cpu')
            cutoff_time: Only return partitions older than this time

        Returns:
            List of partition info dicts with 'path' and 'files'
        """
        loop = asyncio.get_event_loop()

        def _list():
            partitions = {}

            try:
                # List all files for this database/measurement
                # Check if storage backend is local or S3
                if hasattr(self.storage_backend, 'base_path'):
                    # Local filesystem
                    from pathlib import Path
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
                    # Fallback - use storage backend's list_objects (assumes current database)
                    prefix = f"{measurement}/"
                    objects = self.storage_backend.list_objects(prefix=prefix, max_keys=100000)

                logger.debug(f"Found {len(objects)} objects for {database}/{measurement}")

                for obj in objects:
                    # Parse path: measurement/year/month/day/hour/file.parquet
                    parts = obj.split('/')
                    if len(parts) >= 5:
                        # Extract time components
                        meas, year, month, day, hour = parts[0], parts[1], parts[2], parts[3], parts[4]

                        # Validate this is the correct measurement
                        if meas != measurement:
                            continue

                        # Check if partition is old enough
                        try:
                            partition_time = datetime(
                                int(year), int(month), int(day), int(hour)
                            )

                            if partition_time < cutoff_time:
                                # Build partition path (relative to database)
                                partition_path = f"{measurement}/{year}/{month}/{day}/{hour}"

                                if partition_path not in partitions:
                                    partitions[partition_path] = []

                                # Add file path (relative to database, storage backend handles prefix)
                                partitions[partition_path].append(obj)

                        except ValueError:
                            # Invalid date/time in path, skip
                            logger.warning(f"Invalid partition path format: {obj}")
                            continue

                logger.info(
                    f"Found {len(partitions)} partitions for {database}/{measurement} "
                    f"older than {cutoff_time}"
                )

            except Exception as e:
                logger.error(
                    f"Failed to list partitions for {database}/{measurement}: {e}"
                )

            # Convert to list of dicts
            return [
                {'path': path, 'files': files}
                for path, files in partitions.items()
            ]

        return await loop.run_in_executor(None, _list)

    def get_stats(self) -> Dict[str, Any]:
        """Get compaction statistics including tier stats"""
        stats = {
            'database': self.database,
            'total_jobs_completed': self.total_jobs_completed,
            'total_jobs_failed': self.total_jobs_failed,
            'total_files_compacted': self.total_files_compacted,
            'total_bytes_saved': self.total_bytes_saved,
            'total_bytes_saved_mb': self.total_bytes_saved / 1024 / 1024,
            'active_jobs': len(self.active_jobs),
            'recent_jobs': self.job_history[-10:]  # Last 10 jobs
        }

        # Add tier statistics if tiers are enabled
        if self.tiers:
            stats['tiers'] = [tier.get_stats() for tier in self.tiers]

        return stats
