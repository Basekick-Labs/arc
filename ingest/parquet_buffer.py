"""
Parquet Buffer Writer

Buffers incoming metrics in memory and periodically flushes to Parquet files.
Designed for high-throughput write workloads (Telegraf, OpenTelemetry, etc.)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from pathlib import Path
import polars as pl
from collections import defaultdict
import tempfile
import os

logger = logging.getLogger(__name__)


class ParquetBuffer:
    """
    Thread-safe buffer for batching writes to Parquet files

    Features:
    - Automatic flushing based on size or time
    - Per-measurement buffering
    - Atomic file writes (write to temp, then move)
    - Storage backend abstraction (S3/MinIO/GCS)
    """

    def __init__(
        self,
        storage_backend,
        max_buffer_size: int = 10000,
        max_buffer_age_seconds: int = 60,
        compression: str = 'snappy'
    ):
        """
        Initialize buffer

        Args:
            storage_backend: Storage backend instance (S3Backend, MinIOBackend, etc.)
            max_buffer_size: Max records per measurement before flush
            max_buffer_age_seconds: Max seconds before flush
            compression: Parquet compression (snappy, gzip, zstd)
        """
        self.storage_backend = storage_backend
        self.max_buffer_size = max_buffer_size
        self.max_buffer_age_seconds = max_buffer_age_seconds
        self.compression = compression

        # Per-measurement buffers
        self.buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.buffer_start_times: Dict[str, datetime] = {}

        # Metrics
        self.total_records_buffered = 0
        self.total_records_written = 0
        self.total_flushes = 0
        self.total_errors = 0

        # Background task
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()

    async def start(self):
        """Start background flush task"""
        if self._running:
            return

        self._running = True
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info(f"ParquetBuffer started (max_size={self.max_buffer_size}, max_age={self.max_buffer_age_seconds}s)")

    async def stop(self):
        """Stop background task and flush remaining data"""
        if not self._running:
            return

        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Flush all remaining buffers
        await self.flush_all()
        logger.info(f"ParquetBuffer stopped. Total records written: {self.total_records_written}")

    async def write(self, records: List[Dict[str, Any]]):
        """
        Add records to buffer

        Args:
            records: List of flat dictionaries with keys: time, measurement, tag_*, field_*
        """
        if not records:
            return

        records_to_flush = []

        async with self._lock:
            # Group records by (database, measurement) tuple to prevent data mixing
            # CRITICAL FIX: Concurrent writes to different databases for the same measurement
            # were being mixed in a single buffer, causing data loss
            by_buffer_key = defaultdict(list)
            for record in records:
                measurement = record.get('measurement', 'unknown')
                database = record.get('_database', self.storage_backend.database)
                buffer_key = (database, measurement)
                by_buffer_key[buffer_key].append(record)

            # Add to buffers
            for buffer_key, buffer_records in by_buffer_key.items():
                database, measurement = buffer_key

                if buffer_key not in self.buffer_start_times:
                    self.buffer_start_times[buffer_key] = datetime.now(timezone.utc)

                self.buffers[buffer_key].extend(buffer_records)
                self.total_records_buffered += len(buffer_records)

                # Check if buffer should be flushed
                if len(self.buffers[buffer_key]) >= self.max_buffer_size:
                    logger.debug(f"Buffer for '{database}/{measurement}' reached size limit, flushing")
                    # Extract records to flush while holding lock
                    buffer_data = self.buffers[buffer_key]
                    self.buffers[buffer_key] = []
                    del self.buffer_start_times[buffer_key]
                    records_to_flush.append((buffer_key, buffer_data))

        # Flush outside the lock to avoid blocking other writers
        for buffer_key, buffer_records in records_to_flush:
            await self._flush_records(buffer_key, buffer_records)

    async def _periodic_flush(self):
        """Background task that periodically flushes old buffers"""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds

                records_to_flush = []

                async with self._lock:
                    now = datetime.now(timezone.utc)
                    buffer_keys_to_flush = []

                    for buffer_key, start_time in self.buffer_start_times.items():
                        age_seconds = (now - start_time).total_seconds()
                        if age_seconds >= self.max_buffer_age_seconds:
                            buffer_keys_to_flush.append(buffer_key)

                    for buffer_key in buffer_keys_to_flush:
                        database, measurement = buffer_key
                        logger.debug(f"Buffer for '{database}/{measurement}' reached age limit, flushing")
                        if self.buffers[buffer_key]:
                            buffer_records = self.buffers[buffer_key]
                            self.buffers[buffer_key] = []
                            del self.buffer_start_times[buffer_key]
                            records_to_flush.append((buffer_key, buffer_records))

                # Flush outside the lock
                for buffer_key, buffer_records in records_to_flush:
                    await self._flush_records(buffer_key, buffer_records)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")
                await asyncio.sleep(5)

    async def _flush_records(self, buffer_key: tuple, records: List[Dict[str, Any]]):
        """
        Flush records to Parquet without holding lock

        Args:
            buffer_key: Tuple of (database, measurement)
            records: List of records to flush

        This method is called with records already extracted from the buffer
        """
        database, measurement = buffer_key

        if not records:
            return

        try:
            # Database already provided via buffer_key tuple
            # Remove _database from all records before writing to parquet
            database_override = database
            records = [{k: v for k, v in record.items() if k != '_database'} for record in records]

            # OPTIMIZATION: Convert to columnar format before DataFrame
            # This is more efficient than passing list[dict] to Polars
            columns = defaultdict(list)
            for record in records:
                for key, value in record.items():
                    columns[key].append(value)

            # Convert to DataFrame from columnar format (more efficient)
            df = pl.DataFrame(columns)

            # Ensure time column is datetime
            if 'time' in df.columns:
                df = df.with_columns(pl.col('time').cast(pl.Datetime))

            # Sort by time
            df = df.sort('time')

            # Generate filename with timestamp range
            min_time = df['time'].min()
            max_time = df['time'].max()

            timestamp_str = min_time.strftime('%Y%m%d_%H%M%S')
            filename = f"{measurement}_{timestamp_str}_{len(records)}.parquet"

            # Partition path by date and hour (PHASE 2: Hour-level partitioning)
            # Storage backend will add database prefix
            date_partition = min_time.strftime('%Y/%m/%d/%H')
            remote_path = f"{measurement}/{date_partition}/{filename}"

            # Write to temporary file
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.parquet', delete=False) as tmp_file:
                tmp_path = tmp_file.name

            try:
                # Write Parquet file with statistics enabled (PHASE 1 OPTIMIZATION)
                df.write_parquet(
                    tmp_path,
                    compression=self.compression,
                    statistics=True,  # Enable min/max stats for query pruning
                    use_pyarrow=True  # Better Parquet writer
                )

                # Upload to storage (with optional database override)
                await self.storage_backend.upload_file(tmp_path, remote_path, database_override=database_override)

                self.total_records_written += len(records)
                self.total_flushes += 1

                logger.info(f"Flushed {len(records)} records for '{measurement}' to {remote_path}")

            finally:
                # Clean up temp file
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)

        except Exception as e:
            logger.error(f"Failed to flush measurement '{measurement}': {e}")
            self.total_errors += 1

            # Re-add records to buffer on error (acquire lock to do so safely)
            async with self._lock:
                # Strict limit check - calculate available space BEFORE adding
                buffer_records = self.buffers[buffer_key]
                max_capacity = self.max_buffer_size * 2
                available_space = max_capacity - len(buffer_records)

                if available_space >= len(records):
                    # Enough space to re-add all records
                    buffer_records.extend(records)
                    # Reset start time for this buffer
                    if buffer_key not in self.buffer_start_times:
                        self.buffer_start_times[buffer_key] = datetime.now(timezone.utc)
                    logger.warning(
                        f"Re-added {len(records)} records to buffer after flush error "
                        f"(buffer at {len(buffer_records)}/{max_capacity})"
                    )
                elif available_space > 0:
                    # Partial space available - add what we can, drop the rest
                    buffer_records.extend(records[:available_space])
                    dropped = len(records) - available_space
                    logger.error(
                        f"Dropped {dropped} of {len(records)} records due to buffer overflow "
                        f"(buffer at capacity: {len(buffer_records)}/{max_capacity})"
                    )
                    logger.error(f"This may indicate persistent storage issues. Check MinIO/S3 connectivity.")
                else:
                    # No space available - drop all records
                    logger.error(
                        f"Dropping {len(records)} records - buffer for '{database}/{measurement}' already at capacity "
                        f"({len(buffer_records)}/{max_capacity} records)"
                    )
                    logger.error(f"This may indicate persistent storage issues. Check MinIO/S3 connectivity.")

    async def _flush_measurement(self, measurement: str):
        """
        Flush buffer for a specific measurement across all databases

        This method should be called while holding self._lock
        """
        # Find all buffer_keys that match this measurement
        buffer_keys_to_flush = [
            buffer_key for buffer_key in self.buffers.keys()
            if buffer_key[1] == measurement  # buffer_key is (database, measurement)
        ]

        if not buffer_keys_to_flush:
            return

        # Collect records to flush
        records_to_flush = []
        for buffer_key in buffer_keys_to_flush:
            if self.buffers[buffer_key]:
                records = self.buffers[buffer_key]
                self.buffers[buffer_key] = []
                del self.buffer_start_times[buffer_key]
                records_to_flush.append((buffer_key, records))

        # Release lock before actual flush
        for buffer_key, records in records_to_flush:
            await self._flush_records(buffer_key, records)

    async def flush_all(self):
        """Flush all (database, measurement) buffers"""
        records_to_flush = []

        async with self._lock:
            buffer_keys = list(self.buffers.keys())
            for buffer_key in buffer_keys:
                if self.buffers[buffer_key]:
                    buffer_records = self.buffers[buffer_key]
                    self.buffers[buffer_key] = []
                    if buffer_key in self.buffer_start_times:
                        del self.buffer_start_times[buffer_key]
                    records_to_flush.append((buffer_key, buffer_records))

        # Flush outside the lock
        for buffer_key, buffer_records in records_to_flush:
            await self._flush_records(buffer_key, buffer_records)

    async def flush_measurement_sync(self, measurement: str):
        """Explicitly flush a specific measurement (with lock)"""
        async with self._lock:
            await self._flush_measurement(measurement)

    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        return {
            'total_records_buffered': self.total_records_buffered,
            'total_records_written': self.total_records_written,
            'total_flushes': self.total_flushes,
            'total_errors': self.total_errors,
            'current_buffer_sizes': {
                measurement: len(records)
                for measurement, records in self.buffers.items()
            },
            'oldest_buffer_age_seconds': self._get_oldest_buffer_age()
        }

    def _get_oldest_buffer_age(self) -> Optional[float]:
        """Get age of oldest buffer in seconds"""
        if not self.buffer_start_times:
            return None

        now = datetime.now(timezone.utc)
        oldest = min(self.buffer_start_times.values())
        return (now - oldest).total_seconds()


class InMemoryBuffer:
    """
    Simple in-memory buffer for testing/development

    Does not write to disk, just keeps records in memory
    """

    def __init__(self, max_buffer_size: int = 10000):
        self.max_buffer_size = max_buffer_size
        self.records: List[Dict[str, Any]] = []
        self._lock = asyncio.Lock()

    async def write(self, records: List[Dict[str, Any]]):
        """Add records to buffer"""
        async with self._lock:
            self.records.extend(records)

            # Trim if over limit
            if len(self.records) > self.max_buffer_size:
                excess = len(self.records) - self.max_buffer_size
                self.records = self.records[excess:]
                logger.warning(f"Buffer overflow, dropped {excess} oldest records")

    async def read_all(self) -> List[Dict[str, Any]]:
        """Read all buffered records"""
        async with self._lock:
            return list(self.records)

    async def clear(self):
        """Clear buffer"""
        async with self._lock:
            count = len(self.records)
            self.records = []
            logger.info(f"Cleared {count} records from buffer")

    def get_stats(self) -> Dict[str, Any]:
        """Get buffer statistics"""
        return {
            'current_size': len(self.records),
            'max_size': self.max_buffer_size
        }
