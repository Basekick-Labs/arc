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
            # Group records by measurement
            by_measurement = defaultdict(list)
            for record in records:
                measurement = record.get('measurement', 'unknown')
                by_measurement[measurement].append(record)

            # Add to buffers
            for measurement, measurement_records in by_measurement.items():
                if measurement not in self.buffer_start_times:
                    self.buffer_start_times[measurement] = datetime.now(timezone.utc)

                self.buffers[measurement].extend(measurement_records)
                self.total_records_buffered += len(measurement_records)

                # Check if buffer should be flushed
                if len(self.buffers[measurement]) >= self.max_buffer_size:
                    logger.debug(f"Buffer for '{measurement}' reached size limit, flushing")
                    # Extract records to flush while holding lock
                    buffer_records = self.buffers[measurement]
                    self.buffers[measurement] = []
                    del self.buffer_start_times[measurement]
                    records_to_flush.append((measurement, buffer_records))

        # Flush outside the lock to avoid blocking other writers
        for measurement, buffer_records in records_to_flush:
            await self._flush_records(measurement, buffer_records)

    async def _periodic_flush(self):
        """Background task that periodically flushes old buffers"""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds

                records_to_flush = []

                async with self._lock:
                    now = datetime.now(timezone.utc)
                    measurements_to_flush = []

                    for measurement, start_time in self.buffer_start_times.items():
                        age_seconds = (now - start_time).total_seconds()
                        if age_seconds >= self.max_buffer_age_seconds:
                            measurements_to_flush.append(measurement)

                    for measurement in measurements_to_flush:
                        logger.debug(f"Buffer for '{measurement}' reached age limit, flushing")
                        if self.buffers[measurement]:
                            buffer_records = self.buffers[measurement]
                            self.buffers[measurement] = []
                            del self.buffer_start_times[measurement]
                            records_to_flush.append((measurement, buffer_records))

                # Flush outside the lock
                for measurement, buffer_records in records_to_flush:
                    await self._flush_records(measurement, buffer_records)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")
                await asyncio.sleep(5)

    async def _flush_records(self, measurement: str, records: List[Dict[str, Any]]):
        """
        Flush records to Parquet without holding lock

        This method is called with records already extracted from the buffer
        """
        if not records:
            return

        try:
            # Check if records have a database override
            database_override = None
            if records and '_database' in records[0]:
                database_override = records[0]['_database']
                # Remove _database from all records before writing to parquet
                records = [{k: v for k, v in record.items() if k != '_database'} for record in records]

            # Convert to DataFrame
            df = pl.DataFrame(records)

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
                # Only re-add if buffer isn't already too large (prevent infinite growth)
                if len(self.buffers[measurement]) < self.max_buffer_size * 2:
                    self.buffers[measurement].extend(records)
                    # Reset start time for this measurement
                    if measurement not in self.buffer_start_times:
                        self.buffer_start_times[measurement] = datetime.now(timezone.utc)
                    logger.warning(f"Re-added {len(records)} records to buffer after flush error")
                else:
                    logger.error(f"Dropping {len(records)} records - buffer for '{measurement}' already at capacity ({len(self.buffers[measurement])} records)")
                    logger.error(f"This may indicate persistent storage issues. Check MinIO/S3 connectivity.")

    async def _flush_measurement(self, measurement: str):
        """
        Flush buffer for a specific measurement

        This method should be called while holding self._lock
        """
        if measurement not in self.buffers or not self.buffers[measurement]:
            return

        records = self.buffers[measurement]
        self.buffers[measurement] = []
        del self.buffer_start_times[measurement]

        # Release lock before actual flush
        await self._flush_records(measurement, records)

    async def flush_all(self):
        """Flush all measurement buffers"""
        records_to_flush = []

        async with self._lock:
            measurements = list(self.buffers.keys())
            for measurement in measurements:
                if self.buffers[measurement]:
                    buffer_records = self.buffers[measurement]
                    self.buffers[measurement] = []
                    if measurement in self.buffer_start_times:
                        del self.buffer_start_times[measurement]
                    records_to_flush.append((measurement, buffer_records))

        # Flush outside the lock
        for measurement, buffer_records in records_to_flush:
            await self._flush_records(measurement, buffer_records)

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
